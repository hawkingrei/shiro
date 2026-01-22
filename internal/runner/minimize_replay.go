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

func (r *Runner) replayCase(ctx context.Context, schemaSQL, inserts, caseSQL []string, result oracle.Result, spec replaySpec) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	conn, err := r.exec.Conn(ctx)
	if err != nil {
		return false
	}
	defer util.CloseWithErr(conn, "minimize conn")
	minDB := r.baseDB + "_min"
	if err := r.resetDatabaseOnConn(ctx, conn, minDB); err != nil {
		return false
	}
	if err := r.prepareConn(ctx, conn, minDB); err != nil {
		return false
	}
	if err := execStatements(ctx, conn, schemaSQL, r.validator); err != nil {
		return false
	}
	if err := execStatements(ctx, conn, inserts, r.validator); err != nil {
		return false
	}

	switch spec.kind {
	case "signature":
		base, err := querySignatureConn(ctx, conn, spec.expectedSQL, r.validator)
		if err != nil {
			return false
		}
		if spec.setVar != "" {
			if err := r.execOnConn(ctx, conn, "SET SESSION "+spec.setVar); err != nil {
				return false
			}
		}
		other, err := querySignatureConn(ctx, conn, spec.actualSQL, r.validator)
		if spec.setVar != "" {
			resetVarOnConn(conn, ctx, spec.setVar)
		}
		if err != nil {
			return false
		}
		return base != other
	case "count":
		base, err := queryCountConn(ctx, conn, spec.expectedSQL, r.validator)
		if err != nil {
			return false
		}
		other, err := queryCountConn(ctx, conn, spec.actualSQL, r.validator)
		if err != nil {
			return false
		}
		return base != other
	case "plan_rows":
		base, err := queryPlanRowsConn(ctx, conn, spec.expectedSQL, r.validator)
		if err != nil {
			return false
		}
		other, err := queryPlanRowsConn(ctx, conn, spec.actualSQL, r.validator)
		if err != nil {
			return false
		}
		return other > base*(1.0+spec.tolerance)
	case "rows_affected":
		base, err := queryCountConn(ctx, conn, spec.expectedSQL, r.validator)
		if err != nil {
			return false
		}
		affected, err := execRowsAffected(ctx, conn, spec.actualSQL, r.validator)
		if err != nil {
			return false
		}
		return affected != base
	case "case_error":
		err := execStatements(ctx, conn, caseSQL, r.validator)
		return errorMatches(err, result.Err)
	default:
		err := execStatements(ctx, conn, caseSQL, r.validator)
		return errorMatches(err, result.Err)
	}
}

func (r *Runner) resetDatabaseOnConn(ctx context.Context, conn *sql.Conn, name string) error {
	if err := r.execOnConn(ctx, conn, fmt.Sprintf("DROP DATABASE IF EXISTS %s", name)); err != nil {
		return err
	}
	if err := r.execOnConn(ctx, conn, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", name)); err != nil {
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
	if tol == 0 {
		tol = 0.1
	}
	return replaySpec{
		kind:        kind,
		expectedSQL: expected,
		actualSQL:   actual,
		setVar:      setVar,
		tolerance:   tol,
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
		maxRounds = 8
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

func execStatements(ctx context.Context, conn *sql.Conn, stmts []string, v *validator.Validator) error {
	for _, stmt := range stmts {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if v != nil {
			if err := v.Validate(stmt); err != nil {
				return err
			}
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func querySignatureConn(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator) (db.Signature, error) {
	if v != nil {
		if err := v.Validate(query); err != nil {
			return db.Signature{}, err
		}
	}
	row := conn.QueryRowContext(ctx, query)
	var sig db.Signature
	if err := row.Scan(&sig.Count, &sig.Checksum); err != nil {
		return db.Signature{}, err
	}
	return sig, nil
}

func queryCountConn(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator) (int64, error) {
	if v != nil {
		if err := v.Validate(query); err != nil {
			return 0, err
		}
	}
	row := conn.QueryRowContext(ctx, query)
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func execRowsAffected(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator) (int64, error) {
	if v != nil {
		if err := v.Validate(query); err != nil {
			return 0, err
		}
	}
	res, err := conn.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return affected, nil
}

func queryPlanRowsConn(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator) (float64, error) {
	if v != nil {
		if err := v.Validate(query); err != nil {
			return 0, err
		}
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer util.CloseWithErr(rows, "plan rows")

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	if len(cols) == 0 {
		return 0, fmt.Errorf("no columns in explain result")
	}

	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
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
	return 0, fmt.Errorf("no estRows field")
}

func resetVarOnConn(conn *sql.Conn, ctx context.Context, assignment string) {
	name := strings.SplitN(assignment, "=", 2)[0]
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	_, _ = conn.ExecContext(ctx, "SET SESSION "+name+"=DEFAULT")
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
