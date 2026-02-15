package oracle

import (
	"fmt"
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
	if got := buildExpr(expr); got != "(t0.c0 = 12)" {
		t.Fatalf("expected numeric predicate, got %s", got)
	}

	nullVal := pqsPivotValue{Column: intCol, Null: true}
	nullExpr := pqsPredicateExprForValue(ref, nullVal)
	if got := buildExpr(nullExpr); got != "(t0.c0 IS NULL)" {
		t.Fatalf("expected null predicate, got %s", got)
	}

	strCol := schema.Column{Name: "c1", Type: schema.TypeVarchar}
	strRef := generator.ColumnRef{Table: "t0", Name: "c1", Type: strCol.Type}
	strVal := pqsPivotValue{Column: strCol, Raw: "hi"}
	strExpr := pqsPredicateExprForValue(strRef, strVal)
	if got := buildExpr(strExpr); got != "(t0.c1 = 'hi')" {
		t.Fatalf("expected string predicate, got %s", got)
	}
}

func TestPQSEvalRectify(t *testing.T) {
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeInt},
			{Name: "c1", Type: schema.TypeInt},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{tbl},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"c0": {Column: tbl.Columns[0], Raw: "1"},
				"c1": {Column: tbl.Columns[1], Null: true},
			},
		},
	}
	refC0 := generator.ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}
	refC1 := generator.ColumnRef{Table: "t0", Name: "c1", Type: schema.TypeInt}

	trueExpr := generator.BinaryExpr{
		Left:  generator.ColumnExpr{Ref: refC0},
		Op:    "=",
		Right: generator.LiteralExpr{Value: int64(1)},
	}
	if got := pqsEvalExpr(trueExpr, pivot); got != pqsTruthTrue {
		t.Fatalf("expected true eval, got %v", got)
	}
	rectTrue := pqsRectifyExpr(trueExpr, pqsTruthTrue)
	if got := buildExpr(rectTrue); got != "(t0.c0 = 1)" {
		t.Fatalf("expected rectified true, got %s", got)
	}

	falseExpr := generator.BinaryExpr{
		Left:  generator.ColumnExpr{Ref: refC0},
		Op:    "=",
		Right: generator.LiteralExpr{Value: int64(2)},
	}
	if got := pqsEvalExpr(falseExpr, pivot); got != pqsTruthFalse {
		t.Fatalf("expected false eval, got %v", got)
	}
	rectFalse := pqsRectifyExpr(falseExpr, pqsTruthFalse)
	if got := buildExpr(rectFalse); got != "NOT (t0.c0 = 2)" {
		t.Fatalf("expected rectified false, got %s", got)
	}

	nullExpr := generator.BinaryExpr{
		Left:  generator.ColumnExpr{Ref: refC1},
		Op:    "=",
		Right: generator.LiteralExpr{Value: int64(1)},
	}
	if got := pqsEvalExpr(nullExpr, pivot); got != pqsTruthNull {
		t.Fatalf("expected null eval, got %v", got)
	}
	rectNull := pqsRectifyExpr(nullExpr, pqsTruthNull)
	if got := buildExpr(rectNull); got != "((t0.c1 = 1) IS NULL)" {
		t.Fatalf("expected rectified null, got %s", got)
	}

	unknownExpr := generator.BinaryExpr{
		Left:  generator.ColumnExpr{Ref: refC0},
		Op:    "LIKE",
		Right: generator.LiteralExpr{Value: "%"},
	}
	if got := pqsEvalExpr(unknownExpr, pivot); got != pqsTruthUnknown {
		t.Fatalf("expected unknown eval, got %v", got)
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
	if got := buildExpr(expr); got != "(t0.c0 = 7)" {
		t.Fatalf("expected predicate, got %s", got)
	}
}

func TestPQSPredicateForPivotRange(t *testing.T) {
	gen := newPQSTestGenerator(t, 2)
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeInt},
			{Name: "c1", Type: schema.TypeInt},
			{Name: "c2", Type: schema.TypeInt},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{tbl},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"c0": {Column: tbl.Columns[0], Raw: "1"},
				"c1": {Column: tbl.Columns[1], Raw: "2"},
				"c2": {Column: tbl.Columns[2], Raw: "3"},
			},
		},
	}
	single := pqsPredicateForPivotWithRange(gen, pivot, 1, 1)
	if single == nil {
		t.Fatalf("expected single-column predicate")
	}
	if got := countColumnExpr(single); got != 1 {
		t.Fatalf("expected 1 column expr, got %d", got)
	}
	multi := pqsPredicateForPivotWithRange(gen, pivot, 2, 3)
	if multi == nil {
		t.Fatalf("expected multi-column predicate")
	}
	if got := countColumnExpr(multi); got < 2 {
		t.Fatalf("expected at least 2 column exprs, got %d", got)
	}
}

func TestPQSPredicateSkipsFloatColumns(t *testing.T) {
	gen := newPQSTestGenerator(t, 3)
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeFloat},
			{Name: "c1", Type: schema.TypeDouble},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{tbl},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"c0": {Column: tbl.Columns[0], Raw: "1.23"},
				"c1": {Column: tbl.Columns[1], Raw: "4.56"},
			},
		},
	}
	expr := pqsPredicateForPivotWithRange(gen, pivot, 1, 2)
	if expr != nil {
		t.Fatalf("expected nil predicate for float-only columns")
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
	if got := buildExpr(match); got != "((c0 = 3) AND (c1 IS NULL))" {
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
	if query == nil || len(aliases) != 2 {
		t.Fatalf("expected aliases for multi-table pivot query")
	}
	match := pqsMatchExpr(pivot, aliases)
	if got := buildExpr(match); got != "((t0_id = 1) AND (t1_id = 1))" {
		t.Fatalf("expected match expr, got %s", got)
	}
}

func TestPQSJoinContainmentSQL(t *testing.T) {
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
				"c0": {Column: left.Columns[1], Raw: "7"},
			},
			"t1": {
				"id": {Column: right.Columns[0], Raw: "1"},
				"c1": {Column: right.Columns[1], Raw: "hi"},
			},
		},
	}
	query, aliases := buildPQSQuery(pivot)
	if query == nil || len(aliases) != 2 {
		t.Fatalf("expected aliases for join containment")
	}
	aliases = pqsCompactUsingIDColumns(query, aliases)
	query.Where = pqsPredicateExprForValue(
		generator.ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt},
		pivot.Values["t0"]["c0"],
	)
	querySQL := query.SQLString()
	matchSQL := buildExpr(pqsMatchExpr(pivot, aliases))
	containSQL := fmt.Sprintf("SELECT 1 FROM (%s) pqs WHERE %s LIMIT 1", querySQL, matchSQL)
	expectedQuery := "SELECT id AS t0_id FROM t0 JOIN t1 USING (id) WHERE (t0.c0 = 7)"
	if querySQL != expectedQuery {
		t.Fatalf("unexpected join query: %s", querySQL)
	}
	expectedContain := "SELECT 1 FROM (" + expectedQuery + ") pqs WHERE (t0_id = 1) LIMIT 1"
	if containSQL != expectedContain {
		t.Fatalf("unexpected containment SQL: %s", containSQL)
	}
}

func TestPQSCompactUsingIDColumns(t *testing.T) {
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
			{Name: "c1", Type: schema.TypeInt},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{left, right},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"id": {Column: left.Columns[0], Raw: "1"},
				"c0": {Column: left.Columns[1], Raw: "7"},
			},
			"t1": {
				"id": {Column: right.Columns[0], Raw: "1"},
				"c1": {Column: right.Columns[1], Raw: "9"},
			},
		},
	}
	query, aliases := buildPQSQuery(pivot)
	aliases = pqsCompactUsingIDColumns(query, aliases)
	if got := query.SQLString(); got != "SELECT id AS t0_id FROM t0 JOIN t1 USING (id)" {
		t.Fatalf("unexpected compact query: %s", got)
	}
	if len(aliases) != 1 {
		t.Fatalf("unexpected alias count: %d", len(aliases))
	}
	if got := buildExpr(pqsMatchExpr(pivot, aliases)); got != "(t0_id = 1)" {
		t.Fatalf("unexpected compact match expr: %s", got)
	}
}

func TestPQSJoinOnPredicate(t *testing.T) {
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
			{Name: "c1", Type: schema.TypeInt},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{left, right},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"id": {Column: left.Columns[0], Raw: "1"},
				"c0": {Column: left.Columns[1], Raw: "7"},
			},
			"t1": {
				"id": {Column: right.Columns[0], Raw: "1"},
				"c1": {Column: right.Columns[1], Raw: "9"},
			},
		},
	}
	expr, ok := pqsJoinOnExpr(nil, pivot, left, right, nil)
	if !ok {
		t.Fatalf("expected join-on predicate")
	}
	if got := buildExpr(expr); got != "((t0.id = t1.id) AND (t1.c1 = 9))" {
		t.Fatalf("unexpected join-on predicate: %s", got)
	}
}

func TestPQSSubqueryPredicateExists(t *testing.T) {
	gen := newPQSTestGenerator(t, 4)
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeInt},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{tbl},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"c0": {Column: tbl.Columns[0], Null: true},
			},
		},
	}
	expr, meta := pqsBuildSubqueryPredicateForKind(gen, pivot, "exists")
	if expr == nil {
		t.Fatalf("expected subquery predicate")
	}
	if meta.Kind != "exists" {
		t.Fatalf("expected exists subquery, got %s", meta.Kind)
	}
	expected := "EXISTS (SELECT 1 AS c0 FROM t0 WHERE (t0.c0 IS NULL))"
	if got := buildExpr(expr); got != expected {
		t.Fatalf("unexpected exists predicate: %s", got)
	}
}

func TestPQSSubqueryPredicateIn(t *testing.T) {
	gen := newPQSTestGenerator(t, 6)
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeInt},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{tbl},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"c0": {Column: tbl.Columns[0], Raw: "7"},
			},
		},
	}
	expr, meta := pqsBuildSubqueryPredicateForKind(gen, pivot, "in")
	if expr == nil {
		t.Fatalf("expected subquery predicate")
	}
	if meta.Kind != "in" {
		t.Fatalf("expected in subquery, got %s", meta.Kind)
	}
	expected := "(t0.c0 IN ((SELECT t0.c0 AS c0 FROM t0 WHERE (t0.c0 = 7))))"
	if got := buildExpr(expr); got != expected {
		t.Fatalf("unexpected in predicate: %s", got)
	}
}

func TestPQSSubqueryPredicateAnyAll(t *testing.T) {
	gen := newPQSTestGenerator(t, 7)
	gen.Config.Features.QuantifiedSubqueries = true
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeInt},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{tbl},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"c0": {Column: tbl.Columns[0], Raw: "7"},
			},
		},
	}
	anyExpr, anyMeta := pqsBuildSubqueryPredicateForKind(gen, pivot, "any")
	if anyExpr == nil {
		t.Fatalf("expected any subquery predicate")
	}
	if anyMeta.Kind != "any" {
		t.Fatalf("expected any subquery, got %s", anyMeta.Kind)
	}
	expectedAny := "(t0.c0 = ANY (SELECT t0.c0 AS c0 FROM t0 WHERE (t0.c0 = 7)))"
	if got := buildExpr(anyExpr); got != expectedAny {
		t.Fatalf("unexpected any predicate: %s", got)
	}
	allExpr, allMeta := pqsBuildSubqueryPredicateForKind(gen, pivot, "all")
	if allExpr == nil {
		t.Fatalf("expected all subquery predicate")
	}
	if allMeta.Kind != "all" {
		t.Fatalf("expected all subquery, got %s", allMeta.Kind)
	}
	expectedAll := "(t0.c0 = ALL (SELECT t0.c0 AS c0 FROM t0 WHERE (t0.c0 = 7)))"
	if got := buildExpr(allExpr); got != expectedAll {
		t.Fatalf("unexpected all predicate: %s", got)
	}
}

func TestPQSDerivedTableQuery(t *testing.T) {
	gen := newPQSTestGenerator(t, 5)
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeInt},
		},
	}
	pivot := &pqsPivotRow{
		Tables: []schema.Table{tbl},
		Values: map[string]map[string]pqsPivotValue{
			"t0": {
				"c0": {Column: tbl.Columns[0], Raw: "7"},
			},
		},
	}
	query := pqsDerivedTableQuery(gen, pivot, tbl)
	if query == nil {
		t.Fatalf("expected derived table query")
	}
	expected := "SELECT t0.c0 AS c0 FROM t0 WHERE (t0.c0 = 7)"
	if got := query.SQLString(); got != expected {
		t.Fatalf("unexpected derived table query: %s", got)
	}
}

func TestPQSLiteralValueBool(t *testing.T) {
	col := schema.Column{Name: "c0", Type: schema.TypeBool}
	value := pqsLiteralValue(col, "1")
	if value == nil {
		t.Fatalf("expected literal value")
	}
	boolVal, ok := value.(bool)
	if !ok {
		t.Fatalf("unexpected bool literal type %T", value)
	}
	if !boolVal {
		t.Fatalf("expected true for bool literal")
	}
}

func newPQSTestGenerator(t *testing.T, seed int64) *generator.Generator {
	t.Helper()
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return generator.New(cfg, &schema.State{}, seed)
}

func countColumnExpr(expr generator.Expr) int {
	switch e := expr.(type) {
	case generator.BinaryExpr:
		return countColumnExpr(e.Left) + countColumnExpr(e.Right)
	case generator.UnaryExpr:
		return countColumnExpr(e.Expr)
	case generator.ColumnExpr:
		return 1
	default:
		return 0
	}
}
