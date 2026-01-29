package oracle

import (
	"context"
	"fmt"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

// NoREC implements the NoREC oracle.
//
// It checks query correctness by comparing:
// 1) The COUNT(*) of the original query (optimized execution), and
// 2) The SUM of predicate evaluations over the same FROM clause (unoptimized form).
//
// If the counts differ, the optimizer likely changed the semantics.
type NoREC struct{}

// Name returns the oracle identifier.
func (o NoREC) Name() string { return "NoREC" }

// Run generates a simple SELECT with a WHERE predicate and compares the two counts.
// It skips complex queries (aggregates, GROUP BY, DISTINCT, HAVING, subqueries),
// because NoREC assumes a flat SELECT with a single predicate. LIMIT is allowed
// only when paired with ORDER BY and is applied to both forms (top-N).
//
// Example:
//
//	Q:  SELECT * FROM t WHERE a > 10
//	NoREC: SELECT IFNULL(SUM(CASE WHEN a > 10 THEN 1 ELSE 0 END),0) FROM t
//
// If the counts differ, the optimizer likely changed semantics.
func (o NoREC) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, _ *schema.State) Result {
	builder := generator.NewSelectQueryBuilder(gen).
		RequireWhere().
		PredicateMode(generator.PredicateModeSimple).
		RequireDeterministic()
	query, reason, attempts := builder.BuildWithReason()
	if query == nil || query.Where == nil {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": builderSkipReason("norec", reason), "builder_reason": reason, "builder_attempts": attempts}}
	}
	if shouldSkipNoREC(query) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "norec:guardrail"}}
	}
	optimized := query.SQLString()
	optimizedCount := fmt.Sprintf("SELECT COUNT(*) FROM (%s) q", optimized)

	unoptimized := buildWith(query) + buildNoRECQuery(query)
	orderLimit := buildOrderLimit(query)
	unoptimizedCount := buildWith(query) + fmt.Sprintf(
		"SELECT IFNULL(SUM(b),0) FROM (SELECT CASE WHEN %s THEN 1 ELSE 0 END AS b FROM %s%s) q",
		buildExpr(query.Where),
		buildFrom(query),
		orderLimit,
	)

	optCount, err := exec.QueryCount(ctx, optimizedCount)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{optimizedCount, unoptimizedCount}, Err: err}
	}
	unoptCount, err := exec.QueryCount(ctx, unoptimizedCount)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{optimizedCount, unoptimizedCount}, Err: err}
	}
	if optCount != unoptCount {
		unoptimizedExplain, _ := explainSQL(ctx, exec, unoptimizedCount)
		optimizedExplain, _ := explainSQL(ctx, exec, optimizedCount)
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{optimized, unoptimized},
			Expected: fmt.Sprintf("optimized count=%d", optCount),
			Actual:   fmt.Sprintf("unoptimized count=%d", unoptCount),
			Details: map[string]any{
				"replay_kind":           "count",
				"replay_expected_sql":   optimizedCount,
				"replay_actual_sql":     unoptimizedCount,
				"norec_optimized_sql":   optimizedCount,
				"norec_unoptimized_sql": unoptimizedCount,
				"norec_predicate":       buildExpr(query.Where),
				"unoptimized_explain":   unoptimizedExplain,
				"optimized_explain":     optimizedExplain,
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{optimized, unoptimized}}
}

func buildNoRECQuery(query *generator.SelectQuery) string {
	return fmt.Sprintf("SELECT (CASE WHEN %s THEN 1 ELSE 0 END) AS b FROM %s%s", buildExpr(query.Where), buildFrom(query), buildOrderLimit(query))
}

func shouldSkipNoREC(query *generator.SelectQuery) bool {
	if query == nil {
		return true
	}
	// NoREC requires a flat predicate. The following constructs break equivalence:
	// - DISTINCT: COUNT(*) counts distinct rows, SUM(CASE...) counts base rows.
	//   Example:
	//     Optimized:   SELECT COUNT(*) FROM (SELECT DISTINCT a FROM t WHERE a > 0) q;
	//     Unoptimized: SELECT IFNULL(SUM(CASE WHEN a > 0 THEN 1 ELSE 0 END),0) FROM t;
	// - GROUP BY / HAVING: optimized counts groups, unoptimized counts rows.
	//   Example:
	//     Optimized:   SELECT COUNT(*) FROM (SELECT a, COUNT(*) FROM t WHERE a > 0 GROUP BY a) q;
	//     Unoptimized: SELECT IFNULL(SUM(CASE WHEN a > 0 THEN 1 ELSE 0 END),0) FROM t;
	// - HAVING example:
	//     Optimized:   SELECT COUNT(*) FROM (SELECT a FROM t GROUP BY a HAVING SUM(b) > 0) q;
	//     Unoptimized: SELECT IFNULL(SUM(CASE WHEN /* predicate */ THEN 1 ELSE 0 END),0) FROM t;
	// - LIMIT without ORDER BY: non-deterministic top-N selection.
	//   Example:
	//     Optimized:   SELECT COUNT(*) FROM (SELECT * FROM t WHERE a > 0 LIMIT 5) q;
	//     Unoptimized: SELECT IFNULL(SUM(CASE WHEN a > 0 THEN 1 ELSE 0 END),0) FROM t;
	// - Subquery in WHERE: predicate is no longer flat.
	if query.Distinct || len(query.GroupBy) > 0 || query.Having != nil {
		return true
	}
	if query.Limit != nil && len(query.OrderBy) == 0 {
		return true
	}
	if queryHasAggregate(query) || hasSubqueryInPredicate(query) {
		return true
	}
	return false
}

func hasSubqueryInPredicate(query *generator.SelectQuery) bool {
	if query == nil || query.Where == nil {
		return false
	}
	return exprHasSubquery(query.Where)
}

func buildOrderLimit(query *generator.SelectQuery) string {
	if query == nil {
		return ""
	}
	var out string
	if len(query.OrderBy) > 0 {
		parts := make([]string, 0, len(query.OrderBy))
		for _, ob := range query.OrderBy {
			if ob.Expr == nil {
				continue
			}
			item := buildExpr(ob.Expr)
			if ob.Desc {
				item += " DESC"
			}
			parts = append(parts, item)
		}
		if len(parts) > 0 {
			out += " ORDER BY " + strings.Join(parts, ", ")
		}
	}
	if query.Limit != nil {
		out += fmt.Sprintf(" LIMIT %d", *query.Limit)
	}
	return out
}
