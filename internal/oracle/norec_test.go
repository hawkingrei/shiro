package oracle

import (
	"context"
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestNoRECNoTablesSkip(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	state := schema.State{}
	gen := generator.New(cfg, &state, 2)
	res := (NoREC{}).Run(context.Background(), nil, gen, &state)
	if res.OK != true {
		t.Fatalf("expected OK skip")
	}
	if res.Details["skip_reason"] == nil {
		t.Fatalf("expected skip reason")
	}
}

func TestNoRECQueryGuardReasonRejectsSetOps(t *testing.T) {
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  generator.FromClause{BaseTable: "t0"},
		Where: generator.BinaryExpr{
			Left:  generator.LiteralExpr{Value: 1},
			Op:    "=",
			Right: generator.LiteralExpr{Value: 1},
		},
		SetOps: []generator.SetOperation{{
			Type: generator.SetOperationUnion,
			Query: &generator.SelectQuery{
				Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 2}, Alias: "c0"}},
				From:  generator.FromClause{BaseTable: "t1"},
			},
		}},
	}

	ok, reason := noRECQueryGuardReason(query)
	if ok {
		t.Fatalf("expected guard to reject set operations")
	}
	if reason != "constraint:set_ops" {
		t.Fatalf("expected constraint:set_ops, got %s", reason)
	}
}
