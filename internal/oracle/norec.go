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
// It skips complex queries (aggregates, GROUP BY, DISTINCT, HAVING, LIMIT, subqueries),
// because NoREC assumes a flat SELECT with a single predicate.
//
// Example:
//
//	Q:  SELECT * FROM t WHERE a > 10
//	NoREC: SELECT IFNULL(SUM(CASE WHEN a > 10 THEN 1 ELSE 0 END),0) FROM t
//
// If the counts differ, the optimizer likely changed semantics.
func (o NoREC) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	query := gen.GenerateSelectQuery()
	if query == nil || query.Where == nil {
		return Result{OK: true, Oracle: o.Name()}
	}
	if shouldSkipNoREC(query) {
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
	return fmt.Sprintf("SELECT (CASE WHEN %s THEN 1 ELSE 0 END) AS b FROM %s", buildExpr(query.Where), buildFrom(query))
}

func shouldSkipNoREC(query *generator.SelectQuery) bool {
	if query == nil {
		return true
	}
	if len(query.With) > 0 {
		return true
	}
	if query.Distinct || len(query.GroupBy) > 0 || query.Having != nil || query.Limit != nil {
		return true
	}
	if queryHasAggregate(query) || queryHasSubquery(query) {
		return true
	}
	return false
}

func explainSQL(ctx context.Context, exec *db.DB, query string) (string, error) {
	rows, err := exec.QueryContext(ctx, "EXPLAIN "+query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	values := make([][]byte, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var b strings.Builder
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return "", err
		}
		for i, v := range values {
			if i > 0 {
				b.WriteByte('\t')
			}
			if v == nil {
				b.WriteString("NULL")
			} else {
				b.Write(v)
			}
		}
		b.WriteByte('\n')
	}
	return b.String(), nil
}
