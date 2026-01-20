package runner

import (
	"context"
	"database/sql"
	"hash/crc32"
	"math"
	"strconv"
	"strings"

	"shiro/internal/db"
)

// originSampleLimit bounds sample rows embedded in reports.
const originSampleLimit = 10

func drainRows(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
	}
	return rows.Err()
}

func signatureFromRows(rows *sql.Rows, roundScale int) (db.Signature, error) {
	cols, err := rows.Columns()
	if err != nil {
		return db.Signature{}, err
	}
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	sig := db.Signature{}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return db.Signature{}, err
		}
		sig.Count++
		var b strings.Builder
		first := true
		for _, v := range values {
			if !first {
				b.WriteByte('#')
			}
			first = false
			if v == nil {
				b.WriteString("NULL")
			} else {
				b.WriteString(normalizeSignatureValue(v, roundScale))
			}
		}
		sig.Checksum ^= int64(crc32.ChecksumIEEE([]byte(b.String())))
	}
	if err := rows.Err(); err != nil {
		return db.Signature{}, err
	}
	return sig, nil
}

func signatureAndSampleFromRows(rows *sql.Rows, limit int, roundScale int) (db.Signature, []string, [][]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return db.Signature{}, nil, nil, err
	}
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	sig := db.Signature{}
	samples := make([][]string, 0, limit)
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return db.Signature{}, nil, nil, err
		}
		sig.Count++
		var b strings.Builder
		first := true
		for _, v := range values {
			if !first {
				b.WriteByte('#')
			}
			first = false
			if v == nil {
				b.WriteString("NULL")
			} else {
				b.WriteString(normalizeSignatureValue(v, roundScale))
			}
		}
		sig.Checksum ^= int64(crc32.ChecksumIEEE([]byte(b.String())))
		if len(samples) < limit {
			row := make([]string, len(values))
			for i, v := range values {
				if v == nil {
					row[i] = "NULL"
				} else {
					row[i] = normalizeSignatureValue(v, roundScale)
				}
			}
			samples = append(samples, row)
		}
	}
	if err := rows.Err(); err != nil {
		return db.Signature{}, nil, nil, err
	}
	return sig, cols, samples, nil
}

func normalizeSignatureValue(raw []byte, roundScale int) string {
	if raw == nil {
		return "NULL"
	}
	text := string(raw)
	if roundScale <= 0 {
		return text
	}
	if !looksNumeric(text) {
		return text
	}
	val, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return text
	}
	scale := math.Pow10(roundScale)
	val = math.Round(val*scale) / scale
	return strconv.FormatFloat(val, 'f', roundScale, 64)
}

func looksNumeric(s string) bool {
	if s == "" {
		return false
	}
	hasDigit := false
	for i, r := range s {
		if r >= '0' && r <= '9' {
			hasDigit = true
			continue
		}
		switch r {
		case '+', '-', '.', 'e', 'E':
			if (r == 'e' || r == 'E') && !hasDigit {
				return false
			}
			if (r == '+' || r == '-') && i > 0 && s[i-1] != 'e' && s[i-1] != 'E' {
				return false
			}
		default:
			return false
		}
	}
	last := s[len(s)-1]
	if last == 'e' || last == 'E' || last == '+' || last == '-' {
		return false
	}
	return hasDigit
}

func (r *Runner) signatureForSQL(ctx context.Context, sqlText string) (db.Signature, error) {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	rows, err := r.exec.QueryContext(qctx, sqlText)
	if err != nil {
		return db.Signature{}, err
	}
	defer rows.Close()
	return signatureFromRows(rows, r.signatureRoundScale())
}

func (r *Runner) signatureForSQLOnConn(ctx context.Context, conn *sql.Conn, sqlText string, roundScale int) (db.Signature, error) {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	rows, err := conn.QueryContext(qctx, sqlText)
	if err != nil {
		return db.Signature{}, err
	}
	defer rows.Close()
	return signatureFromRows(rows, roundScale)
}

func (r *Runner) signatureRoundScale() int {
	if r.cfg.Signature.RoundScale < 0 {
		return 0
	}
	return r.cfg.Signature.RoundScale
}

func (r *Runner) planCacheRoundScale() int {
	if r.cfg.Signature.PlanCacheRoundScale < 0 {
		return r.signatureRoundScale()
	}
	return r.cfg.Signature.PlanCacheRoundScale
}
