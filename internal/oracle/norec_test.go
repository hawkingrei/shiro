package oracle

import (
	"context"
	"errors"
	"testing"

	"shiro/internal/config"
	"shiro/internal/db"
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

func TestNoRECErrorReturnsSQLFeaturesForCountQueries(t *testing.T) {
	gen := newProfileTestGenerator(t)
	expectedErr := errors.New("observe failure")
	exec := &db.DB{
		Validate: func(string) error {
			return expectedErr
		},
	}

	res := (NoREC{}).Run(context.Background(), exec, gen, gen.State)
	if res.Err == nil {
		t.Fatalf("expected validation error result")
	}
	if len(res.SQL) != 2 {
		t.Fatalf("expected count SQL pair, got %v", res.SQL)
	}
	for _, sqlText := range res.SQL {
		if _, ok := res.SQLFeatures[sqlText]; !ok {
			t.Fatalf("missing SQL features for returned SQL %q", sqlText)
		}
	}
}
