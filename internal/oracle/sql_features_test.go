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

func TestBuildFromKeepsBaseAndJoinAlias(t *testing.T) {
	query := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			BaseAlias: "b",
			Joins: []generator.Join{
				{
					Type:       generator.JoinInner,
					Table:      "t1",
					TableAlias: "j",
					On: generator.BinaryExpr{
						Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "b", Name: "id"}},
						Op:    "=",
						Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "j", Name: "id"}},
					},
				},
			},
		},
	}
	fromSQL := buildFrom(query)
	if !strings.Contains(fromSQL, "t0 AS b") {
		t.Fatalf("expected base alias in FROM SQL, got %s", fromSQL)
	}
	if !strings.Contains(fromSQL, "JOIN t1 AS j") {
		t.Fatalf("expected join alias in FROM SQL, got %s", fromSQL)
	}
}

func TestQueryHelperPreferAnalysisFastPath(t *testing.T) {
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  generator.FromClause{BaseTable: "t0"},
		Analysis: &generator.QueryAnalysis{
			HasAggregate:  true,
			HasSubquery:   true,
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
