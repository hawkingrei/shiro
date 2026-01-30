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
