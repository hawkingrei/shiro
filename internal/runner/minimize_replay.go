package runner

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"shiro/internal/db"
	"shiro/internal/oracle"
	"shiro/internal/util"
	"shiro/internal/validator"
)

const replayTraceSQLMax = 400

const (
	replayForeignKeyChecksOffSQL = "SET SESSION FOREIGN_KEY_CHECKS=0"
	replayForeignKeyChecksOnSQL  = "SET SESSION FOREIGN_KEY_CHECKS=1"
)

type replayTrace struct {
	lastOp  string
	lastSQL string
	lastErr error
}

func (t *replayTrace) record(op, sqlText string) {
	if t == nil {
		return
	}
	t.lastOp = op
	t.lastSQL = abbrevSQL(sqlText, replayTraceSQLMax)
	t.lastErr = nil
}

func (t *replayTrace) recordErr(err error) {
	if t == nil || err == nil {
		return
	}
	t.lastErr = err
}

func abbrevSQL(sqlText string, maxLen int) string {
	trimmed := strings.TrimSpace(sqlText)
	if maxLen <= 0 || len(trimmed) <= maxLen {
		return trimmed
	}
	return trimmed[:maxLen] + "..."
}

func closeReplayConn(conn *sql.Conn, trace *replayTrace) {
	if conn == nil {
		return
	}
	if err := conn.Close(); err != nil {
		if trace != nil && (trace.lastSQL != "" || trace.lastErr != nil) {
			util.Warnf("close minimize conn: %v last_op=%s last_sql=%s last_err=%v", err, trace.lastOp, trace.lastSQL, trace.lastErr)
			return
		}
		util.Warnf("close minimize conn: %v", err)
	}
}

func wrapReplayInsertsWithForeignKeyChecks(inserts []string) []string {
	normalized := make([]string, 0, len(inserts))
	for _, stmt := range inserts {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		normalized = append(normalized, trimmed)
	}
	out := make([]string, 0, len(normalized)+2)
	out = append(out, replayForeignKeyChecksOffSQL)
	out = append(out, normalized...)
	out = append(out, replayForeignKeyChecksOnSQL)
	return out
}

func (r *Runner) execReplayInserts(ctx context.Context, conn *sql.Conn, inserts []string, trace *replayTrace) error {
	if len(inserts) == 0 {
		return nil
	}
	return execStatements(ctx, conn, wrapReplayInsertsWithForeignKeyChecks(inserts), r.validator, trace)
}

func (r *Runner) replayCase(ctx context.Context, schemaSQL, inserts, caseSQL []string, result oracle.Result, spec replaySpec) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	conn, err := r.exec.Conn(ctx)
	if err != nil {
		return false
	}
	trace := &replayTrace{}
	defer closeReplayConn(conn, trace)
	minDB := r.baseDB + "_min"
	if err := r.resetDatabaseOnConn(ctx, conn, minDB, trace); err != nil {
		return false
	}
	if err := r.prepareConnWithTrace(ctx, conn, minDB, trace); err != nil {
		return false
	}
	if err := execStatements(ctx, conn, schemaSQL, r.validator, trace); err != nil {
		return false
	}
	if err := r.execReplayInserts(ctx, conn, inserts, trace); err != nil {
		return false
	}

	switch spec.kind {
	case "signature":
		base, err := querySignatureConn(ctx, conn, spec.expectedSQL, r.validator, trace)
		if err != nil {
			return false
		}
		if spec.setVar != "" {
			if err := r.execOnConnWithTrace(ctx, conn, "SET SESSION "+spec.setVar, trace); err != nil {
				return false
			}
		}
		other, err := querySignatureConn(ctx, conn, spec.actualSQL, r.validator, trace)
		if spec.setVar != "" {
			resetVarOnConn(ctx, conn, spec.setVar, trace)
		}
		if err != nil {
			return false
		}
		return base != other
	case "count":
		base, err := queryCountConn(ctx, conn, spec.expectedSQL, r.validator, trace)
		if err != nil {
			return false
		}
		other, err := queryCountConn(ctx, conn, spec.actualSQL, r.validator, trace)
		if err != nil {
			return false
		}
		return base != other
	case "plan_rows":
		base, err := queryPlanRowsConn(ctx, conn, spec.expectedSQL, r.validator, trace)
		if err != nil {
			return false
		}
		other, err := queryPlanRowsConn(ctx, conn, spec.actualSQL, r.validator, trace)
		if err != nil {
			return false
		}
		return other > base*(1.0+spec.tolerance)
	case "rows_affected":
		base, err := queryCountConn(ctx, conn, spec.expectedSQL, r.validator, trace)
		if err != nil {
			return false
		}
		affected, err := execRowsAffected(ctx, conn, spec.actualSQL, r.validator, trace)
		if err != nil {
			return false
		}
		return affected != base
	case "impo_contains":
		baseRows, baseTrunc, err := queryRowSetConn(ctx, conn, spec.expectedSQL, r.validator, spec.maxRows, trace)
		if err != nil || baseTrunc {
			return false
		}
		mutRows, mutTrunc, err := queryRowSetConn(ctx, conn, spec.actualSQL, r.validator, spec.maxRows, trace)
		if err != nil || mutTrunc {
			return false
		}
		cmp := compareRowSets(baseRows, mutRows)
		return !implicationOK(spec.impoIsUpper, cmp)
	case "case_error":
		err := execStatements(ctx, conn, caseSQL, r.validator, trace)
		return errorMatches(err, result.Err)
	default:
		err := execStatements(ctx, conn, caseSQL, r.validator, trace)
		return errorMatches(err, result.Err)
	}
}

func (r *Runner) execOnConnWithTrace(ctx context.Context, conn *sql.Conn, sqlText string, trace *replayTrace) error {
	if trace != nil {
		trace.record("exec", sqlText)
	}
	err := r.execOnConn(ctx, conn, sqlText)
	if trace != nil {
		trace.recordErr(err)
	}
	return err
}

func (r *Runner) prepareConnWithTrace(ctx context.Context, conn *sql.Conn, dbName string, trace *replayTrace) error {
	if dbName == "" {
		return nil
	}
	return r.execOnConnWithTrace(ctx, conn, fmt.Sprintf("USE %s", dbName), trace)
}

func (r *Runner) resetDatabaseOnConn(ctx context.Context, conn *sql.Conn, name string, trace *replayTrace) error {
	if err := r.execOnConnWithTrace(ctx, conn, fmt.Sprintf("DROP DATABASE IF EXISTS %s", name), trace); err != nil {
		return err
	}
	if err := r.execOnConnWithTrace(ctx, conn, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", name), trace); err != nil {
		return err
	}
	return nil
}

func buildReplaySpec(result oracle.Result) replaySpec {
	if result.Err != nil {
		return replaySpec{kind: "case_error"}
	}
	if result.Details == nil {
		return replaySpec{}
	}
	kind, _ := result.Details["replay_kind"].(string)
	expected, _ := result.Details["replay_expected_sql"].(string)
	actual, _ := result.Details["replay_actual_sql"].(string)
	setVar, _ := result.Details["replay_set_var"].(string)
	tol, _ := result.Details["replay_tolerance"].(float64)
	maxRows, _ := result.Details["replay_max_rows"].(int)
	impoIsUpper, _ := result.Details["replay_impo_is_upper"].(bool)
	if tol == 0 {
		tol = 0.1
	}
	return replaySpec{
		kind:        kind,
		expectedSQL: expected,
		actualSQL:   actual,
		setVar:      setVar,
		tolerance:   tol,
		maxRows:     maxRows,
		impoIsUpper: impoIsUpper,
	}
}

func dedupeStatements(stmts []string) []string {
	seen := make(map[string]struct{}, len(stmts))
	out := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func shrinkStatements(stmts []string, maxRounds int, test func([]string) bool) []string {
	if len(stmts) < 2 {
		return stmts
	}
	if maxRounds <= 0 {
		maxRounds = minimizeDefaultRounds
	}
	n := 2
	rounds := 0
	for len(stmts) >= 2 && rounds < maxRounds {
		rounds++
		chunk := len(stmts) / n
		if chunk == 0 {
			break
		}
		removed := false
		for i := 0; i < n; i++ {
			start := i * chunk
			end := start + chunk
			if i == n-1 {
				end = len(stmts)
			}
			candidate := append([]string{}, stmts[:start]...)
			candidate = append(candidate, stmts[end:]...)
			if test(candidate) {
				stmts = candidate
				n = max(n-1, 2)
				removed = true
				break
			}
		}
		if !removed {
			if n >= len(stmts) {
				break
			}
			n = min(n*2, len(stmts))
		}
	}
	return stmts
}

func execStatements(ctx context.Context, conn *sql.Conn, stmts []string, v *validator.Validator, trace *replayTrace) error {
	for _, stmt := range stmts {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if trace != nil {
			trace.record("exec", stmt)
		}
		if v != nil {
			if err := v.Validate(stmt); err != nil {
				if trace != nil {
					trace.recordErr(err)
				}
				return err
			}
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			if trace != nil {
				trace.recordErr(err)
			}
			return err
		}
	}
	return nil
}

type impoRowSet struct {
	columns int
	rows    []string
}

func queryRowSetConn(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator, maxRows int, trace *replayTrace) (impoRowSet, bool, error) {
	if trace != nil {
		trace.record("query", query)
	}
	if v != nil {
		if err := v.Validate(query); err != nil {
			if trace != nil {
				trace.recordErr(err)
			}
			return impoRowSet{}, false, err
		}
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		if trace != nil {
			trace.recordErr(err)
		}
		return impoRowSet{}, false, err
	}
	defer util.CloseWithErr(rows, "impo replay rows")

	cols, err := rows.Columns()
	if err != nil {
		if trace != nil {
			trace.recordErr(err)
		}
		return impoRowSet{}, false, err
	}
	if maxRows <= 0 {
		maxRows = 50
	}
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	out := impoRowSet{columns: len(cols), rows: make([]string, 0)}
	truncated := false
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			if trace != nil {
				trace.recordErr(err)
			}
			return impoRowSet{}, false, err
		}
		if len(out.rows) < maxRows {
			parts := make([]string, 0, len(values))
			for _, v := range values {
				if v == nil {
					parts = append(parts, "NULL")
					continue
				}
				parts = append(parts, string(v))
			}
			out.rows = append(out.rows, strings.Join(parts, "\x1f"))
		} else {
			truncated = true
		}
	}
	if err := rows.Err(); err != nil {
		if trace != nil {
			trace.recordErr(err)
		}
		return impoRowSet{}, false, err
	}
	return out, truncated, nil
}

func compareRowSets(base impoRowSet, other impoRowSet) int {
	empty1 := base.columns == 0
	empty2 := other.columns == 0
	if empty1 || empty2 {
		switch {
		case empty1 && empty2:
			return 0
		case empty1:
			return -1
		default:
			return 1
		}
	}
	if base.columns != other.columns {
		return 2
	}

	mp := make(map[string]int, len(other.rows))
	for _, row := range other.rows {
		mp[row]++
	}
	allInOther := true
	for _, row := range base.rows {
		if num, ok := mp[row]; ok {
			if num <= 1 {
				delete(mp, row)
			} else {
				mp[row] = num - 1
			}
		} else {
			allInOther = false
		}
	}

	if allInOther {
		if len(mp) == 0 {
			return 0
		}
		return -1
	}
	if len(mp) == 0 {
		return 1
	}
	return 2
}

func implicationOK(isUpper bool, cmp int) bool {
	if cmp == 0 {
		return true
	}
	if isUpper {
		return cmp == -1
	}
	return cmp == 1
}

func querySignatureConn(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator, trace *replayTrace) (db.Signature, error) {
	if trace != nil {
		trace.record("query", query)
	}
	if v != nil {
		if err := v.Validate(query); err != nil {
			if trace != nil {
				trace.recordErr(err)
			}
			return db.Signature{}, err
		}
	}
	row := conn.QueryRowContext(ctx, query)
	var sig db.Signature
	if err := row.Scan(&sig.Count, &sig.Checksum); err != nil {
		if trace != nil {
			trace.recordErr(err)
		}
		return db.Signature{}, err
	}
	return sig, nil
}

func queryCountConn(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator, trace *replayTrace) (int64, error) {
	if trace != nil {
		trace.record("query", query)
	}
	if v != nil {
		if err := v.Validate(query); err != nil {
			if trace != nil {
				trace.recordErr(err)
			}
			return 0, err
		}
	}
	row := conn.QueryRowContext(ctx, query)
	var count int64
	if err := row.Scan(&count); err != nil {
		if trace != nil {
			trace.recordErr(err)
		}
		return 0, err
	}
	return count, nil
}

func execRowsAffected(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator, trace *replayTrace) (int64, error) {
	if trace != nil {
		trace.record("exec", query)
	}
	if v != nil {
		if err := v.Validate(query); err != nil {
			if trace != nil {
				trace.recordErr(err)
			}
			return 0, err
		}
	}
	res, err := conn.ExecContext(ctx, query)
	if err != nil {
		if trace != nil {
			trace.recordErr(err)
		}
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return affected, nil
}

func queryPlanRowsConn(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator, trace *replayTrace) (float64, error) {
	if trace != nil {
		trace.record("query", query)
	}
	if v != nil {
		if err := v.Validate(query); err != nil {
			if trace != nil {
				trace.recordErr(err)
			}
			return 0, err
		}
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		if trace != nil {
			trace.recordErr(err)
		}
		return 0, err
	}
	defer util.CloseWithErr(rows, "plan rows")

	cols, err := rows.Columns()
	if err != nil {
		if trace != nil {
			trace.recordErr(err)
		}
		return 0, err
	}
	if len(cols) == 0 {
		err := fmt.Errorf("no columns in explain result")
		if trace != nil {
			trace.recordErr(err)
		}
		return 0, err
	}

	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			if trace != nil {
				trace.recordErr(err)
			}
			return 0, err
		}
		for i, name := range cols {
			if name == "estRows" || name == "rows" || name == "est_rows" {
				if len(values[i]) == 0 {
					continue
				}
				var v float64
				if _, err := fmt.Sscanf(string(values[i]), "%f", &v); err == nil {
					return v, nil
				}
			}
		}
	}
	err = fmt.Errorf("no estRows field")
	if trace != nil {
		trace.recordErr(err)
	}
	return 0, err
}

func resetVarOnConn(ctx context.Context, conn *sql.Conn, assignment string, trace *replayTrace) {
	name := strings.SplitN(assignment, "=", 2)[0]
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	sqlText := "SET SESSION " + name + "=DEFAULT"
	if trace != nil {
		trace.record("exec", sqlText)
	}
	_, err := conn.ExecContext(ctx, sqlText)
	if trace != nil {
		trace.recordErr(err)
	}
}

func errorMatches(err error, expected error) bool {
	if expected == nil {
		return err == nil
	}
	if err == nil {
		return false
	}
	if isPanicError(expected) {
		return isPanicError(err)
	}
	exp := strings.ToLower(expected.Error())
	got := strings.ToLower(err.Error())
	return strings.Contains(got, exp) || strings.Contains(exp, got)
}
