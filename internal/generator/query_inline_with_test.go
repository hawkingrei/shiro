package generator

import "testing"

func TestBuildDisallowInlineWithInCTEBody(t *testing.T) {
	nested := withWrappedQuery("t1")
	query := &SelectQuery{
		With: []CTE{
			{Name: "c0", Query: nested},
		},
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  FromClause{BaseTable: "t0"},
	}
	assertBuildPanics(t, query)
}

func TestBuildDisallowInlineWithInSetOperationOperand(t *testing.T) {
	query := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  FromClause{BaseTable: "t0"},
		SetOps: []SetOperation{
			{Type: SetOperationUnion, Query: withWrappedQuery("t1")},
		},
	}
	assertBuildPanics(t, query)
}

func TestBuildDisallowInlineWithInDerivedTable(t *testing.T) {
	query := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From: FromClause{
			BaseTable: "d0",
			BaseAlias: "d0",
			BaseQuery: withWrappedQuery("t1"),
		},
	}
	assertBuildPanics(t, query)
}

func withWrappedQuery(table string) *SelectQuery {
	inner := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "x"}},
		From:  FromClause{BaseTable: table},
	}
	outer := &SelectQuery{
		With:  []CTE{{Name: "w0", Query: inner}},
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  FromClause{BaseTable: table},
	}
	return outer
}

func assertBuildPanics(t *testing.T, query *SelectQuery) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for nested WITH in inline context")
		}
	}()
	var b SQLBuilder
	query.Build(&b)
}
