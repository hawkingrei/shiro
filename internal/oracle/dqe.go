package oracle

import (
	"context"
	"fmt"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

// DQE implements the DML query equivalence oracle.
//
// It checks UPDATE/DELETE semantics by comparing:
// - The expected number of affected rows computed via a COUNT query, and
// - The actual rows affected by the DML statement.
// A mismatch indicates a potential execution correctness issue.
type DQE struct{}

// Name returns the oracle identifier.
func (o DQE) Name() string { return "DQE" }

// Run randomly chooses UPDATE or DELETE, then compares affected rows
// against a predicate-derived count.
//
// Example:
//
//	Update: UPDATE t SET a = a + 1 WHERE b > 5
//	Check:  SELECT COUNT(*) FROM t WHERE b > 5 AND NOT (a <=> a + 1)
//
// If rows affected != count, execution semantics are wrong.
func (o DQE) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	if !state.HasBaseTables() {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "dqe:no_base_tables"}}
	}
	baseTables := state.BaseTables()
	tbl := baseTables[gen.Rand.Intn(len(baseTables))]
	choice := gen.Rand.Intn(2)

	if choice == 0 {
		updateSQL, predicate, setExpr, colRef := pickDQEUpdate(gen, tbl)
		if updateSQL == "" || predicate == nil || setExpr == nil || colRef.Table == "" {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "dqe:update_guard"}}
		}
		if !predicate.Deterministic() {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "dqe:predicate_guard"}}
		}
		colSQL := fmt.Sprintf("%s.%s", colRef.Table, colRef.Name)
		setExprSQL := buildExpr(setExpr)
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s AND NOT (%s <=> %s)", tbl.Name, buildExpr(predicate), colSQL, setExprSQL)
		count, err := exec.QueryCount(ctx, countSQL)
		if err != nil {
			return Result{OK: true, Oracle: o.Name(), SQL: []string{countSQL}, Err: err}
		}
		res, err := exec.ExecContext(ctx, updateSQL)
		if err != nil {
			return Result{OK: true, Oracle: o.Name(), SQL: []string{updateSQL}, Err: err}
		}
		affected, _ := res.RowsAffected()
		if affected != count {
			expectedExplain, expectedExplainErr := explainSQL(ctx, exec, countSQL)
			actualExplain, actualExplainErr := explainSQL(ctx, exec, updateSQL)
			return Result{
				OK:       false,
				Oracle:   o.Name(),
				SQL:      []string{updateSQL},
				Expected: fmt.Sprintf("rows affected=%d", count),
				Actual:   fmt.Sprintf("rows affected=%d", affected),
				Details: map[string]any{
					"replay_kind":          "rows_affected",
					"replay_expected_sql":  countSQL,
					"replay_actual_sql":    updateSQL,
					"expected_explain":     expectedExplain,
					"actual_explain":       actualExplain,
					"expected_explain_err": errString(expectedExplainErr),
					"actual_explain_err":   errString(actualExplainErr),
				},
			}
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{updateSQL, countSQL}}
	}

	deleteSQL, predicate := pickDQEDelete(gen, tbl)
	if deleteSQL == "" || predicate == nil {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "dqe:delete_guard"}}
	}
	if !predicate.Deterministic() {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "dqe:predicate_guard"}}
	}
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tbl.Name, buildExpr(predicate))
	count, err := exec.QueryCount(ctx, countSQL)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{countSQL}, Err: err}
	}
	res, err := exec.ExecContext(ctx, deleteSQL)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{deleteSQL}, Err: err}
	}
	affected, _ := res.RowsAffected()
	if affected != count {
		expectedExplain, expectedExplainErr := explainSQL(ctx, exec, countSQL)
		actualExplain, actualExplainErr := explainSQL(ctx, exec, deleteSQL)
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{deleteSQL},
			Expected: fmt.Sprintf("rows affected=%d", count),
			Actual:   fmt.Sprintf("rows affected=%d", affected),
			Details: map[string]any{
				"replay_kind":          "rows_affected",
				"replay_expected_sql":  countSQL,
				"replay_actual_sql":    deleteSQL,
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{deleteSQL, countSQL}}
}

func pickDQEUpdate(gen *generator.Generator, tbl schema.Table) (sql string, predicate generator.Expr, setExpr generator.Expr, colRef generator.ColumnRef) {
	const maxTries = 5
	var firstSQL string
	var firstPred generator.Expr
	var firstSet generator.Expr
	var firstRef generator.ColumnRef
	for i := 0; i < maxTries; i++ {
		sql, predicate, setExpr, colRef = gen.UpdateSQL(tbl)
		if i == 0 {
			firstSQL, firstPred, firstSet, firstRef = sql, predicate, setExpr, colRef
		}
		if predicate == nil {
			continue
		}
		hasExists, hasNotExists := generator.ExprHasExistsSubquery(predicate)
		if hasExists || hasNotExists {
			return sql, predicate, setExpr, colRef
		}
	}
	return firstSQL, firstPred, firstSet, firstRef
}

func pickDQEDelete(gen *generator.Generator, tbl schema.Table) (sql string, predicate generator.Expr) {
	const maxTries = 5
	var firstSQL string
	var firstPred generator.Expr
	for i := 0; i < maxTries; i++ {
		sql, predicate = gen.DeleteSQL(tbl)
		if i == 0 {
			firstSQL, firstPred = sql, predicate
		}
		if predicate == nil {
			continue
		}
		hasExists, hasNotExists := generator.ExprHasExistsSubquery(predicate)
		if hasExists || hasNotExists {
			return sql, predicate
		}
	}
	return firstSQL, firstPred
}
