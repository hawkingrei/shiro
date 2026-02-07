package generator

import "testing"

type nonDetExpr struct{}

func (nonDetExpr) Build(b *SQLBuilder) {
	b.Write("RAND()")
}

func (nonDetExpr) Columns() []ColumnRef { return nil }

func (nonDetExpr) Deterministic() bool { return false }

func TestQueryDeterministicRecursesIntoNestedQueries(t *testing.T) {
	baseQuery := &SelectQuery{
		Items: []SelectItem{{Expr: nonDetExpr{}, Alias: "c0"}},
		From:  FromClause{BaseTable: "t0"},
	}
	query := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From: FromClause{
			BaseTable: "t0",
			BaseQuery: baseQuery,
			BaseAlias: "d0",
		},
	}
	if QueryDeterministic(query) {
		t.Fatalf("expected nondeterministic query when base derived query is nondeterministic")
	}
}

func TestQueryDeterministicRecursesIntoSetOpsAndJoinDerived(t *testing.T) {
	setRHS := &SelectQuery{
		Items: []SelectItem{{Expr: nonDetExpr{}, Alias: "c0"}},
		From:  FromClause{BaseTable: "t1"},
	}
	joinDerived := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  FromClause{BaseTable: "t2"},
		Where: nonDetExpr{},
	}
	query := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:       JoinInner,
					Table:      "t2",
					TableAlias: "d1",
					TableQuery: joinDerived,
				},
			},
		},
		SetOps: []SetOperation{
			{
				Type:  SetOperationUnion,
				Query: setRHS,
			},
		},
	}
	if QueryDeterministic(query) {
		t.Fatalf("expected nondeterministic query when set-op/join-derived contains nondeterministic expression")
	}
}

func TestQueryDeterministicRecursesIntoWindowDefs(t *testing.T) {
	query := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  FromClause{BaseTable: "t0"},
		WindowDefs: []WindowDef{
			{
				Name:        "w0",
				PartitionBy: []Expr{nonDetExpr{}},
			},
		},
	}
	if QueryDeterministic(query) {
		t.Fatalf("expected nondeterministic query when window definition contains nondeterministic expression")
	}
}
