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

const noRECBuildMaxTries = 10

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
	spec := QuerySpec{
		Oracle:   "norec",
		Profile:  ProfileByName("NoREC"),
		MaxTries: noRECBuildMaxTries,
		Constraints: generator.SelectQueryConstraints{
			RequireWhere:         true,
			RequireDeterministic: true,
			QueryGuardReason:     noRECQueryGuardReason,
		},
		SkipReasonOverrides: map[string]string{
			"constraint:aggregate":              "norec:guardrail",
			"constraint:distinct":               "norec:guardrail",
			"constraint:group_by":               "norec:guardrail",
			"constraint:having":                 "norec:guardrail",
			"constraint:set_ops":                "norec:guardrail",
			"constraint:limit_without_order_by": "norec:guardrail",
			"constraint:predicate_subquery":     "norec:guardrail",
		},
	}
	query, details := buildQueryWithSpec(gen, spec)
	if query == nil || query.Where == nil {
		return Result{OK: true, Oracle: o.Name(), Details: details}
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
		reason, code := sqlErrorReason("norec", err)
		details := map[string]any{"error_reason": reason}
		if code != 0 {
			details["error_code"] = int(code)
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{optimizedCount, unoptimizedCount}, Err: err, Details: details}
	}
	unoptCount, err := exec.QueryCount(ctx, unoptimizedCount)
	if err != nil {
		reason, code := sqlErrorReason("norec", err)
		details := map[string]any{"error_reason": reason}
		if code != 0 {
			details["error_code"] = int(code)
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{optimizedCount, unoptimizedCount}, Err: err, Details: details}
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

func noRECQueryGuardReason(query *generator.SelectQuery) (bool, string) {
	if query == nil {
		return false, "constraint:query_guard"
	}
	// NoREC requires a flat predicate in a single SELECT block.
	// The following constructs break equivalence:
	// - Set operations: unoptimized rewrite is defined on one SELECT block only.
	// - LIMIT without ORDER BY: non-deterministic top-N selection.
	//   Example:
	//     Optimized:   SELECT COUNT(*) FROM (SELECT * FROM t WHERE a > 0 LIMIT 5) q;
	//     Unoptimized: SELECT IFNULL(SUM(CASE WHEN a > 0 THEN 1 ELSE 0 END),0) FROM t;
	// - Subquery in WHERE: predicate is no longer flat.
	if len(query.SetOps) > 0 {
		return false, "constraint:set_ops"
	}
	if query.Limit != nil && len(query.OrderBy) == 0 {
		return false, "constraint:limit_without_order_by"
	}
	if hasSubqueryInPredicate(query) {
		return false, "constraint:predicate_subquery"
	}
	return true, ""
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
