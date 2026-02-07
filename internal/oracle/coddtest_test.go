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
