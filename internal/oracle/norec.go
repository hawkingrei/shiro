package oracle

import (
	"context"
	"fmt"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

type NoREC struct{}

func (o NoREC) Name() string { return "NoREC" }

func (o NoREC) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	query := gen.GenerateSelectQuery()
	if query == nil || query.Where == nil {
		return Result{OK: true, Oracle: o.Name()}
	}
	if query.Distinct || len(query.GroupBy) > 0 || query.Having != nil || query.Limit != nil || queryHasAggregate(query) || queryHasSubquery(query) {
		return Result{OK: true, Oracle: o.Name()}
	}
	optimized := query.SQLString()
	optimizedCount := fmt.Sprintf("SELECT COUNT(*) FROM (%s) q", optimized)

	unoptimized := buildWith(query) + buildNoRECQuery(query)
	unoptimizedCount := buildWith(query) + fmt.Sprintf("SELECT IFNULL(SUM(CASE WHEN %s THEN 1 ELSE 0 END),0) FROM %s", buildExpr(query.Where), buildFrom(query))

	optCount, err := exec.QueryCount(ctx, optimizedCount)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{optimizedCount, unoptimizedCount}, Err: err}
	}
	unoptCount, err := exec.QueryCount(ctx, unoptimizedCount)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{optimizedCount, unoptimizedCount}, Err: err}
	}
	if optCount != unoptCount {
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{optimized, unoptimized},
			Expected: fmt.Sprintf("optimized count=%d", optCount),
			Actual:   fmt.Sprintf("unoptimized count=%d", unoptCount),
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{optimized, unoptimized}}
}

func buildNoRECQuery(query *generator.SelectQuery) string {
	return fmt.Sprintf("SELECT (CASE WHEN %s THEN 1 ELSE 0 END) AS b FROM %s", buildExpr(query.Where), buildFrom(query))
}
