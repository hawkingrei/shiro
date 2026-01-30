package generator

import "testing"

func TestColumnExprBuildUnqualified(t *testing.T) {
	expr := ColumnExpr{Ref: ColumnRef{Name: "c0"}}
	builder := SQLBuilder{}
	expr.Build(&builder)
	if got := builder.String(); got != "c0" {
		t.Fatalf("expected unqualified column, got %q", got)
	}
}
