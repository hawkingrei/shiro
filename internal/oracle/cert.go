package oracle

import (
	"context"
	"fmt"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

// CERT implements cardinality estimation restriction testing.
//
// It checks whether adding a restrictive predicate decreases (or at least does not
// drastically increase) the estimated row count in EXPLAIN. A large increase after
// adding a filter is suspicious and may indicate optimizer cardinality bugs.
type CERT struct {
	Tolerance   float64
	MinBaseRows float64
}

// Name returns the oracle identifier.
func (o CERT) Name() string { return "CERT" }

// Run compares EXPLAIN estRows for a base query and a restricted query.
// If restricted estRows exceeds base estRows by the configured tolerance,
// the case is flagged.
//
// Example:
//
//	Base:       EXPLAIN SELECT * FROM t WHERE a > 10
//	Restricted: EXPLAIN SELECT * FROM t WHERE a > 10 AND b = 5
//
// If restricted estRows is much larger, cardinality estimation is suspicious.
func (o CERT) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	if o.Tolerance == 0 {
		o.Tolerance = 0.1
	}
	query := gen.GenerateSelectQuery()
	if query == nil || query.Where == nil {
		return Result{OK: true, Oracle: o.Name()}
	}

	baseExplain := "EXPLAIN " + query.SQLString()
	baseRows, err := exec.QueryPlanRows(ctx, baseExplain)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{baseExplain}, Err: err}
	}

	restricted := query.Clone()
	tables := tablesForQuery(query, state)
	if len(tables) == 0 {
		tables = state.Tables
	}
	restrictPred := gen.GeneratePredicate(tables, 1, false, 0)
	if !isSimplePredicate(restrictPred) {
		return Result{OK: true, Oracle: o.Name()}
	}
	restricted.Where = generator.BinaryExpr{Left: query.Where, Op: "AND", Right: restrictPred}
	if o.MinBaseRows > 0 && baseRows < o.MinBaseRows {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{query.SQLString(), restricted.SQLString()}}
	}
	restrictedExplain := "EXPLAIN " + restricted.SQLString()
	restrictedRows, err := exec.QueryPlanRows(ctx, restrictedExplain)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{restrictedExplain}, Err: err}
	}

	if restrictedRows > baseRows*(1.0+o.Tolerance) {
		expectedExplain, expectedExplainErr := explainSQL(ctx, exec, query.SQLString())
		actualExplain, actualExplainErr := explainSQL(ctx, exec, restricted.SQLString())
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{query.SQLString(), restricted.SQLString()},
			Expected: fmt.Sprintf("restricted estRows <= %.2f", baseRows),
			Actual:   fmt.Sprintf("restricted estRows %.2f", restrictedRows),
			Details: map[string]any{
				"base_est_rows":        baseRows,
				"restricted_est_rows":  restrictedRows,
				"replay_kind":          "plan_rows",
				"replay_expected_sql":  query.SQLString(),
				"replay_actual_sql":    restricted.SQLString(),
				"replay_tolerance":     o.Tolerance,
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{query.SQLString(), restricted.SQLString()}}
}
