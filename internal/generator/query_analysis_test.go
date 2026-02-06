package generator

import "testing"

func TestAnalyzeQuery(t *testing.T) {
	limit := 5
	subquery := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "a"}},
		From:  FromClause{BaseTable: "t2"},
	}
	query := &SelectQuery{
		With:     []CTE{{Name: "cte1", Query: subquery}},
		Distinct: true,
		Items: []SelectItem{
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
			{Expr: WindowExpr{Name: "SUM", Args: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t", Name: "a"}}}}, Alias: "w"},
			{Expr: SubqueryExpr{Query: subquery}, Alias: "sq"},
		},
		From:    FromClause{BaseTable: "t", Joins: []Join{{Type: JoinInner, Table: "t2"}}},
		Where:   InExpr{Left: ColumnExpr{Ref: ColumnRef{Table: "t", Name: "a"}}, List: []Expr{LiteralExpr{Value: 1}}},
		GroupBy: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t", Name: "b"}}},
		Having:  BinaryExpr{Left: ColumnExpr{Ref: ColumnRef{Table: "t", Name: "b"}}, Op: ">", Right: LiteralExpr{Value: 0}},
		OrderBy: []OrderBy{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t", Name: "b"}}}},
		Limit:   &limit,
	}
	analysis := AnalyzeQuery(query)
	if !analysis.Deterministic {
		t.Fatalf("expected deterministic query")
	}
	if !analysis.HasAggregate {
		t.Fatalf("expected aggregate to be detected")
	}
	if !analysis.HasWindow {
		t.Fatalf("expected window to be detected")
	}
	if !analysis.HasSubquery {
		t.Fatalf("expected subquery to be detected")
	}
	if !analysis.HasLimit || !analysis.HasOrderBy || !analysis.HasGroupBy || !analysis.HasHaving || !analysis.HasDistinct || !analysis.HasCTE {
		t.Fatalf("expected limit/order-by/group-by/having/distinct/cte flags to be set")
	}
	if analysis.JoinCount != 1 {
		t.Fatalf("expected join count 1, got %d", analysis.JoinCount)
	}
	if analysis.JoinTypeSeq != "JOIN" {
		t.Fatalf("expected join type seq JOIN, got %s", analysis.JoinTypeSeq)
	}
	if analysis.JoinGraphSig != "t->JOIN:t2" {
		t.Fatalf("expected join graph sig t->JOIN:t2, got %s", analysis.JoinGraphSig)
	}
}
