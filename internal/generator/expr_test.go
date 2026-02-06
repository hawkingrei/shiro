package generator

import "testing"

func TestColumnExprBuildUnqualified(t *testing.T) {
	expr := ColumnExpr{Ref: ColumnRef{Name: "c0"}}
	builder := SQLBuilder{}
	expr.Build(&builder)
	if got := builder.String(); got != "c0" {
		t.Fatalf("expected unqualified column, got %q", got)
	}
}

func TestCompareSubqueryExprBuild(t *testing.T) {
	sub := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  FromClause{BaseTable: "t0"},
	}
	expr := CompareSubqueryExpr{
		Left:       ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c0"}},
		Op:         ">=",
		Quantifier: "some",
		Query:      sub,
	}
	var b SQLBuilder
	expr.Build(&b)
	if got := b.String(); got != "(t1.c0 >= SOME (SELECT 1 AS c0 FROM t0))" {
		t.Fatalf("unexpected SQL: %s", got)
	}
}
