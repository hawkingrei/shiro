package oracle

import (
	"testing"

	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestSignaturePrecheckOrderByInvalidOrdinal(t *testing.T) {
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}, Alias: "c0"},
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "v"}}, Alias: "c1"},
		},
		From: generator.FromClause{BaseTable: "t0"},
		OrderBy: []generator.OrderBy{
			{Expr: generator.LiteralExpr{Value: 3}},
		},
	}
	skipReason, reason := signaturePrecheck(query, nil, "eet")
	if skipReason != "eet:order_by_invalid_ordinal" || reason != "order_by_invalid_ordinal" {
		t.Fatalf("unexpected precheck result: skip=%q reason=%q", skipReason, reason)
	}
}

func TestSignaturePrecheckUnknownQualifiedColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
		},
		Where: generator.BinaryExpr{
			Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "missing"}},
			Op:    "=",
			Right: generator.LiteralExpr{Value: 1},
		},
	}
	skipReason, reason := signaturePrecheck(query, state, "tlp")
	if skipReason != "tlp:column_visibility_unknown_column" || reason != "column_visibility_unknown_column" {
		t.Fatalf("unexpected precheck result: skip=%q reason=%q", skipReason, reason)
	}
}

func TestSignaturePrecheckUnknownUsingColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t1",
					Using: []string{"k0"},
				},
			},
		},
	}
	skipReason, reason := signaturePrecheck(query, state, "eet")
	if skipReason != "eet:column_visibility_unknown_using_column" || reason != "column_visibility_unknown_using_column" {
		t.Fatalf("unexpected precheck result: skip=%q reason=%q", skipReason, reason)
	}
}
