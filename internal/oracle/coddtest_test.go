package oracle

import (
	"context"
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestCODDTestNoTablesSkip(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	state := schema.State{}
	gen := generator.New(cfg, &state, 4)
	res := (CODDTest{}).Run(context.Background(), nil, gen, &state)
	if res.OK != true {
		t.Fatalf("expected OK skip")
	}
	if res.Details["skip_reason"] == nil {
		t.Fatalf("expected skip reason")
	}
}

func TestOnlySupportedCODDColumns(t *testing.T) {
	columns := []generator.ColumnRef{
		{Table: "t0", Name: "i0", Type: schema.TypeInt},
		{Table: "t0", Name: "d0", Type: schema.TypeDecimal},
		{Table: "t0", Name: "s0", Type: schema.TypeVarchar},
		{Table: "t0", Name: "ts", Type: schema.TypeTimestamp},
		{Table: "t0", Name: "b0", Type: schema.TypeBool},
	}
	if !onlySupportedCODDColumns(columns) {
		t.Fatalf("expected supported scalar columns")
	}
}

func TestOnlySupportedCODDColumnsRejectsUnknownType(t *testing.T) {
	columns := []generator.ColumnRef{
		{Table: "t0", Name: "x", Type: schema.ColumnType(999)},
	}
	if onlySupportedCODDColumns(columns) {
		t.Fatalf("expected unknown column type to be rejected")
	}
}

func TestCODDTestPredicatePrecheckReason(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt, Nullable: false},
					{Name: "c0", Type: schema.TypeInt, Nullable: true},
				},
			},
		},
	}

	if reason := coddtestPredicatePrecheckReason(state, &generator.SelectQuery{}); reason != "constraint:no_where" {
		t.Fatalf("expected constraint:no_where, got %q", reason)
	}

	safe := &generator.SelectQuery{
		Where: generator.BinaryExpr{
			Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
			Op:    "=",
			Right: generator.LiteralExpr{Value: 1},
		},
	}
	if reason := coddtestPredicatePrecheckReason(state, safe); reason != "" {
		t.Fatalf("expected empty reason, got %q", reason)
	}

	nullable := &generator.SelectQuery{
		Where: generator.BinaryExpr{
			Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}},
			Op:    "=",
			Right: generator.LiteralExpr{Value: 1},
		},
	}
	if reason := coddtestPredicatePrecheckReason(state, nullable); reason != "constraint:null_guard" {
		t.Fatalf("expected constraint:null_guard, got %q", reason)
	}

	unsupported := &generator.SelectQuery{
		Where: generator.BinaryExpr{
			Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id", Type: schema.ColumnType(999)}},
			Op:    "=",
			Right: generator.LiteralExpr{Value: 1},
		},
	}
	if reason := coddtestPredicatePrecheckReason(state, unsupported); reason != "constraint:type_guard" {
		t.Fatalf("expected constraint:type_guard, got %q", reason)
	}
}
