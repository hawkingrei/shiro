package oracle

import (
	"testing"

	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestQueryColumnsValidChecksSetOperationOperands(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}, Alias: "c0"},
		},
		From: generator.FromClause{BaseTable: "t0"},
		SetOps: []generator.SetOperation{
			{
				Type: generator.SetOperationIntersect,
				Query: &generator.SelectQuery{
					Items: []generator.SelectItem{
						{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "missing"}}, Alias: "c0"},
					},
					From: generator.FromClause{BaseTable: "t0"},
				},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); ok || reason != "unknown_column" {
		t.Fatalf("queryColumnsValid() = (%v, %q), want false/unknown_column", ok, reason)
	}
}

func TestSanitizeQueryColumnsRepairsSetOperationOperands(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}, Alias: "c0"},
		},
		From: generator.FromClause{BaseTable: "t0"},
		SetOps: []generator.SetOperation{
			{
				Type: generator.SetOperationUnion,
				Query: &generator.SelectQuery{
					Items: []generator.SelectItem{
						{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "missing"}}, Alias: "c0"},
					},
					From: generator.FromClause{BaseTable: "t0"},
				},
			},
		},
	}

	if !sanitizeQueryColumns(query, state) {
		t.Fatalf("expected sanitizeQueryColumns to change set-operation operand")
	}
	if ok, reason := queryColumnsValid(query, state, nil); !ok {
		t.Fatalf("queryColumnsValid() after sanitize = (%v, %q), want true", ok, reason)
	}

	got, ok := query.SetOps[0].Query.Items[0].Expr.(generator.ColumnExpr)
	if !ok {
		t.Fatalf("expected sanitized operand expression to remain a column, got %T", query.SetOps[0].Query.Items[0].Expr)
	}
	if got.Ref.Table != "t0" || got.Ref.Name != "id" {
		t.Fatalf("unexpected sanitized operand column: %#v", got.Ref)
	}
}

func TestQueryColumnsValidAllowsUnqualifiedNaturalJoinColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
					{Name: "c1", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Natural: true},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); !ok {
		t.Fatalf("queryColumnsValid() = (%v, %q), want true", ok, reason)
	}
}

func TestSanitizeQueryColumnsKeepsUnqualifiedNaturalJoinColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
					{Name: "c1", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Natural: true},
			},
		},
	}

	if sanitizeQueryColumns(query, state) {
		t.Fatalf("expected sanitizeQueryColumns to keep valid unqualified NATURAL JOIN column")
	}

	col, ok := query.Items[0].Expr.(generator.ColumnExpr)
	if !ok {
		t.Fatalf("expected column expression, got %T", query.Items[0].Expr)
	}
	if col.Ref.Table != "" || col.Ref.Name != "k0" {
		t.Fatalf("unexpected sanitized column ref: %#v", col.Ref)
	}
}
