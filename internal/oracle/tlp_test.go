package oracle

import (
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestTLPSkipReasonLimit(t *testing.T) {
	limit := 5
	query := &generator.SelectQuery{Limit: &limit}
	if got := tlpSkipReason(query); got != "tlp:limit" {
		t.Fatalf("expected tlp:limit, got %q", got)
	}
}

func TestTLPSkipReasonSetOps(t *testing.T) {
	query := &generator.SelectQuery{
		SetOps: []generator.SetOperation{
			{Type: generator.SetOperationUnion, Query: &generator.SelectQuery{}},
		},
	}
	if got := tlpSkipReason(query); got != "tlp:set_ops" {
		t.Fatalf("expected tlp:set_ops, got %q", got)
	}
}

func TestTLPSkipReasonUsingQualified(t *testing.T) {
	state := &schema.State{Tables: []schema.Table{
		{Name: "t0", Columns: []schema.Column{{Name: "k0", Type: schema.TypeInt}}},
		{Name: "t1", Columns: []schema.Column{{Name: "k0", Type: schema.TypeInt}}},
		{Name: "t3", Columns: []schema.Column{{Name: "k0", Type: schema.TypeInt}}},
	}}
	gen := generator.New(config.Config{}, state, 1)
	query := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t1",
					Using: []string{"k0"},
				},
				{
					Type:  generator.JoinInner,
					Table: "t3",
					On: generator.BinaryExpr{
						Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Name: "k0"}},
						Op:    "=",
						Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t3", Name: "k0"}},
					},
				},
			},
		},
	}
	tlpNormalizeUsingRefs(gen, query)
	if len(query.From.Joins) == 0 || len(query.From.Joins[0].Using) > 0 {
		t.Fatalf("expected USING to be rewritten into ON")
	}
	cols := query.From.Joins[1].On.Columns()
	if len(cols) == 0 || cols[0].Table == "" {
		t.Fatalf("expected unqualified column to be rewritten with table qualifier")
	}
}
