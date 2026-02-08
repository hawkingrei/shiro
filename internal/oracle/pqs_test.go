package oracle

import (
	"reflect"
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestPQSPredicateExprForValue(t *testing.T) {
	intCol := schema.Column{Name: "c0", Type: schema.TypeInt}
	ref := generator.ColumnRef{Table: "t0", Name: "c0", Type: intCol.Type}
	val := pqsPivotValue{Column: intCol, Raw: "12"}
	expr := pqsPredicateExprForValue(ref, val)
	if got := buildExpr(expr); got != "t0.c0 = 12" {
		t.Fatalf("expected numeric predicate, got %s", got)
	}

	nullVal := pqsPivotValue{Column: intCol, Null: true}
	nullExpr := pqsPredicateExprForValue(ref, nullVal)
	if got := buildExpr(nullExpr); got != "t0.c0 IS NULL" {
		t.Fatalf("expected null predicate, got %s", got)
	}

	strCol := schema.Column{Name: "c1", Type: schema.TypeVarchar}
	strRef := generator.ColumnRef{Table: "t0", Name: "c1", Type: strCol.Type}
	strVal := pqsPivotValue{Column: strCol, Raw: "hi"}
	strExpr := pqsPredicateExprForValue(strRef, strVal)
	if got := buildExpr(strExpr); got != "t0.c1 = 'hi'" {
		t.Fatalf("expected string predicate, got %s", got)
	}
}

func TestPQSPredicateForPivotSingleColumn(t *testing.T) {
	gen := newPQSTestGenerator(t, 1)
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeInt},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{tbl},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {"c0": {Column: tbl.Columns[0], Raw: "7"}},
		},
	}
	expr := pqsPredicateForPivot(gen, pivot)
	if got := buildExpr(expr); got != "t0.c0 = 7" {
		t.Fatalf("expected predicate, got %s", got)
	}
}

func TestPQSMatchExpr(t *testing.T) {
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeInt},
			{Name: "c1", Type: schema.TypeVarchar},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{tbl},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"c0": {Column: tbl.Columns[0], Raw: "3"},
				"c1": {Column: tbl.Columns[1], Null: true},
			},
		},
	}
	query, aliases := buildPQSQuery(pivot)
	if query == nil || len(aliases) != 2 {
		t.Fatalf("expected aliases for pivot query")
	}
	match := pqsMatchExpr(pivot, aliases)
	if got := buildExpr(match); got != "c0 = 3 AND c1 IS NULL" {
		t.Fatalf("expected match expr, got %s", got)
	}
}

func TestPQSPivotValueMap(t *testing.T) {
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeInt},
			{Name: "c1", Type: schema.TypeVarchar},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{tbl},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"c0": {Column: tbl.Columns[0], Raw: "5"},
				"c1": {Column: tbl.Columns[1], Null: true},
			},
		},
	}
	got := pqsPivotValueMap(pivot)
	want := map[string]any{
		"t0.c0": "5",
		"t0.c1": nil,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected pivot value map: %+v", got)
	}
}

func TestPQSMatchExprMultiTable(t *testing.T) {
	left := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "id", Type: schema.TypeBigInt},
			{Name: "c0", Type: schema.TypeInt},
		},
	}
	right := schema.Table{
		Name: "t1",
		Columns: []schema.Column{
			{Name: "id", Type: schema.TypeBigInt},
			{Name: "c1", Type: schema.TypeVarchar},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{left, right},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"id": {Column: left.Columns[0], Raw: "1"},
				"c0": {Column: left.Columns[1], Raw: "9"},
			},
			"t1": {
				"id": {Column: right.Columns[0], Raw: "1"},
				"c1": {Column: right.Columns[1], Null: true},
			},
		},
	}
	query, aliases := buildPQSQuery(pivot)
	if query == nil || len(aliases) != 4 {
		t.Fatalf("expected aliases for multi-table pivot query")
	}
	match := pqsMatchExpr(pivot, aliases)
	if got := buildExpr(match); got != "t0_id = 1 AND t0_c0 = 9 AND t1_id = 1 AND t1_c1 IS NULL" {
		t.Fatalf("expected match expr, got %s", got)
	}
}

func TestPQSLiteralValueBool(t *testing.T) {
	col := schema.Column{Name: "c0", Type: schema.TypeBool}
	value := pqsLiteralValue(col, "1")
	if value == nil {
		t.Fatalf("expected literal value")
	}
	switch value.(type) {
	case int64, int, bool:
	default:
		t.Fatalf("unexpected bool literal type %T", value)
	}
}

func newPQSTestGenerator(t *testing.T, seed int64) *generator.Generator {
	t.Helper()
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return generator.New(cfg, &schema.State{}, seed)
}
