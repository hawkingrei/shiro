package oracle

import (
	"strings"
	"testing"

	"shiro/internal/generator"
)

func TestBuildWithRecursive(t *testing.T) {
	cteQuery := &generator.SelectQuery{
		Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  generator.FromClause{BaseTable: "t0"},
	}
	query := &generator.SelectQuery{
		WithRecursive: true,
		With:          []generator.CTE{{Name: "c", Query: cteQuery}},
	}
	withSQL := buildWith(query)
	if !strings.HasPrefix(withSQL, "WITH RECURSIVE c AS (SELECT 1 AS c0 FROM t0)") {
		t.Fatalf("unexpected WITH RECURSIVE SQL: %s", withSQL)
	}
}

func TestBuildFromNaturalJoin(t *testing.T) {
	query := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:    generator.JoinLeft,
					Natural: true,
					Table:   "t1",
					Using:   []string{"id"},
				},
			},
		},
	}
	fromSQL := buildFrom(query)
	if !strings.Contains(fromSQL, "NATURAL LEFT JOIN t1") {
		t.Fatalf("unexpected FROM SQL for NATURAL JOIN: %s", fromSQL)
	}
	if strings.Contains(fromSQL, " USING (") || strings.Contains(fromSQL, " ON ") {
		t.Fatalf("NATURAL JOIN must not render USING/ON, got %s", fromSQL)
	}
}

func TestQueryHelperPreferAnalysisFastPath(t *testing.T) {
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  generator.FromClause{BaseTable: "t0"},
		Analysis: &generator.QueryAnalysis{
			HasAggregate: true,
			HasSubquery:  true,
			Deterministic: false,
		},
	}
	if !queryHasAggregate(query) {
		t.Fatalf("expected queryHasAggregate to use analysis fast-path")
	}
	if !queryHasSubquery(query) {
		t.Fatalf("expected queryHasSubquery to use analysis fast-path")
	}
	if queryDeterministic(query) {
		t.Fatalf("expected queryDeterministic to use analysis fast-path")
	}
}

