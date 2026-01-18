package oracle

import (
	"context"
	"fmt"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

type CERT struct {
	Tolerance float64
}

func (o CERT) Name() string { return "CERT" }

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
	restricted.Where = generator.BinaryExpr{Left: query.Where, Op: "AND", Right: gen.GeneratePredicate(tables, 1, false, 0)}
	restrictedExplain := "EXPLAIN " + restricted.SQLString()
	restrictedRows, err := exec.QueryPlanRows(ctx, restrictedExplain)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{restrictedExplain}, Err: err}
	}

	if restrictedRows > baseRows*(1.0+o.Tolerance) {
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{query.SQLString(), restricted.SQLString()},
			Expected: fmt.Sprintf("restricted estRows <= %.2f", baseRows),
			Actual:   fmt.Sprintf("restricted estRows %.2f", restrictedRows),
			Details: map[string]any{
				"base_est_rows":       baseRows,
				"restricted_est_rows": restrictedRows,
				"replay_kind":         "plan_rows",
				"replay_expected_sql": baseExplain,
				"replay_actual_sql":   restrictedExplain,
				"replay_tolerance":    o.Tolerance,
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{query.SQLString(), restricted.SQLString()}}
}
