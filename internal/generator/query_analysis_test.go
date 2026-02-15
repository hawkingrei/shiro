package generator

import (
	"strings"
	"testing"

	"shiro/internal/schema"

	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

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
	if analysis.HasSetOps {
		t.Fatalf("did not expect set-op flag")
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

func TestAnalyzeQueryWithDerivedAndSetOpQuantifiedSubquery(t *testing.T) {
	derived := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
		},
		From: FromClause{BaseTable: "t0"},
	}
	quantifiedSubquery := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
		},
		From: FromClause{BaseTable: "t1"},
	}
	setRHS := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
		},
		From: FromClause{BaseTable: "t2"},
	}
	query := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "d0", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
		},
		From: FromClause{
			BaseTable: "t0",
			BaseQuery: derived,
			BaseAlias: "d0",
		},
		Where: CompareSubqueryExpr{
			Left:       ColumnExpr{Ref: ColumnRef{Table: "d0", Name: "c0", Type: schema.TypeInt}},
			Op:         ">=",
			Quantifier: "ANY",
			Query:      quantifiedSubquery,
		},
		SetOps: []SetOperation{
			{
				Type:  SetOperationUnion,
				All:   true,
				Query: setRHS,
			},
		},
	}

	features := AnalyzeQueryFeatures(query)
	if !features.HasSubquery {
		t.Fatalf("expected subquery feature for derived/quantified query")
	}
	if !features.HasSetOperations {
		t.Fatalf("expected set-operation feature")
	}
	if !features.HasDerivedTables {
		t.Fatalf("expected derived-table feature")
	}
	if !features.HasQuantifiedSubqueries {
		t.Fatalf("expected quantified-subquery feature")
	}
	if features.HasAggregate {
		t.Fatalf("unexpected aggregate feature")
	}
	analysis := AnalyzeQuery(query)
	if !analysis.HasSubquery {
		t.Fatalf("expected analysis to mark subquery")
	}
	if !analysis.HasSetOps {
		t.Fatalf("expected analysis to mark set operations")
	}
	if analysis.HasOrderBy || analysis.HasLimit || analysis.HasGroupBy || analysis.HasHaving {
		t.Fatalf("unexpected order/limit/group/having flags")
	}
}

func TestAnalyzeQueryFeaturesNestedSetOpAndDerivedInSubquery(t *testing.T) {
	nested := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "d1", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
		},
		From: FromClause{
			BaseQuery: &SelectQuery{
				Items: []SelectItem{
					{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
				},
				From: FromClause{BaseTable: "t1"},
			},
			BaseAlias: "d1",
		},
		SetOps: []SetOperation{
			{
				Type: SetOperationUnion,
				Query: &SelectQuery{
					Items: []SelectItem{
						{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
					},
					From: FromClause{BaseTable: "t2"},
				},
			},
		},
	}

	query := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  FromClause{BaseTable: "t0"},
		Where: ExistsExpr{Query: nested},
	}

	features := AnalyzeQueryFeatures(query)
	if !features.HasSubquery {
		t.Fatalf("expected subquery feature")
	}
	if !features.HasSetOperations {
		t.Fatalf("expected nested set-operation feature")
	}
	if !features.HasDerivedTables {
		t.Fatalf("expected nested derived-table feature")
	}
}

func TestSelectQuerySQLWithDerivedAndSetOpQuantifiedSubqueryParses(t *testing.T) {
	derived := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
		},
		From: FromClause{BaseTable: "t0"},
	}
	quantifiedSubquery := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
		},
		From: FromClause{BaseTable: "t1"},
	}
	setRHS := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
		},
		From: FromClause{BaseTable: "t2"},
	}
	query := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "d0", Name: "c0", Type: schema.TypeInt}}, Alias: "c0"},
		},
		From: FromClause{
			BaseTable: "t0",
			BaseQuery: derived,
			BaseAlias: "d0",
		},
		Where: CompareSubqueryExpr{
			Left:       ColumnExpr{Ref: ColumnRef{Table: "d0", Name: "c0", Type: schema.TypeInt}},
			Op:         ">=",
			Quantifier: "ANY",
			Query:      quantifiedSubquery,
		},
		SetOps: []SetOperation{
			{
				Type:  SetOperationIntersect,
				Query: setRHS,
			},
		},
	}

	sql := query.SQLString()
	if !strings.Contains(sql, "ANY") || !strings.Contains(sql, "INTERSECT") {
		t.Fatalf("expected quantified/set-op SQL, got %s", sql)
	}
	p := parser.New()
	if _, _, err := p.Parse(sql, "", ""); err != nil {
		t.Fatalf("parse failed: %v\nsql=%s", err, sql)
	}
}
