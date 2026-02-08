package oracle

import (
	"testing"

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
