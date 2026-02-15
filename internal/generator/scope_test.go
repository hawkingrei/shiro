package generator

import (
	"testing"

	"shiro/internal/schema"
)

func TestValidateQueryScopeJoinOnUsesFutureTable(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
		}},
	}

	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k0", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}},
					},
				},
				{
					Type:  JoinInner,
					Table: "t2",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k0", Type: schema.TypeInt}},
					},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}}},
		},
	}

	if gen.validateQueryScope(query) {
		t.Fatalf("expected join scope validation to fail when ON uses future table")
	}
}

func TestValidateQueryScopeJoinOnUsesVisibleTables(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
		}},
	}

	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}},
					},
				},
				{
					Type:  JoinInner,
					Table: "t2",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k0", Type: schema.TypeInt}},
					},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}}},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected join scope validation to pass for visible tables")
	}
}

func TestValidateQueryScopeSetOpTableNotVisibleToMainQuery(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "c0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "c0", Type: schema.TypeInt},
				},
			},
		}},
	}

	query := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}}},
		},
		From: FromClause{BaseTable: "t0"},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c0", Type: schema.TypeInt}},
			Op:    "=",
			Right: LiteralExpr{Value: 1},
		},
		SetOps: []SetOperation{
			{
				Type: SetOperationUnion,
				Query: &SelectQuery{
					Items: []SelectItem{
						{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c0", Type: schema.TypeInt}}},
					},
					From: FromClause{BaseTable: "t1"},
				},
			},
		},
	}

	if gen.validateQueryScope(query) {
		t.Fatalf("expected set-op operand tables to be invisible to main query scope")
	}
}

func TestValidateQueryScopeUsingHidesQualifiedJoinedColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
		}},
	}

	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"k0"},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}}},
		},
	}

	if gen.validateQueryScope(query) {
		t.Fatalf("expected USING column to be hidden for qualified references")
	}
}

func TestValidateQueryScopeUsingKeepsNonUsingQualifiedColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
		}},
	}

	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"k0"},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c1", Type: schema.TypeInt}}},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected non-USING qualified columns to remain visible")
	}
}

func TestValidateQueryScopeNestedDerivedUsingHidesQualifiedColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
		}},
	}

	derived := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"k0"},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}}, Alias: "k0"},
		},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseAlias: "d0",
			BaseQuery: derived,
		},
		Items: []SelectItem{
			{Expr: LiteralExpr{Value: 1}, Alias: "c0"},
		},
	}

	if gen.validateQueryScope(query) {
		t.Fatalf("expected nested derived query scope validation to hide USING-qualified column")
	}
}

func TestValidateQueryScopeNaturalJoinHidesQualifiedColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
		}},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:    JoinInner,
					Table:   "t1",
					Natural: true,
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}}},
		},
	}
	if gen.validateQueryScope(query) {
		t.Fatalf("expected NATURAL join common column to be hidden for qualified references")
	}
}

func TestValidateQueryScopeNaturalJoinKeepsNonCommonColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
		}},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:    JoinInner,
					Table:   "t1",
					Natural: true,
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}}},
		},
	}
	if !gen.validateQueryScope(query) {
		t.Fatalf("expected NATURAL join non-common columns to remain visible")
	}
}

func TestValidateQueryScopeNaturalRightJoinHidesQualifiedCommonColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k0", Type: schema.TypeVarchar},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k0", Type: schema.TypeVarchar},
				},
			},
			{
				Name: "v0",
				Columns: []schema.Column{
					{Name: "g0", Type: schema.TypeInt},
				},
			},
		}},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t1",
			Joins: []Join{
				{
					Type:    JoinRight,
					Table:   "t2",
					Natural: true,
				},
				{
					Type:  JoinInner,
					Table: "v0",
					On:    BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 0}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "v0", Name: "g0", Type: schema.TypeInt}}},
		},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
			Op:    ">",
			Right: LiteralExpr{Value: 0},
		},
	}
	if gen.validateQueryScope(query) {
		t.Fatalf("expected NATURAL RIGHT JOIN common column to be hidden for qualified references")
	}
}
