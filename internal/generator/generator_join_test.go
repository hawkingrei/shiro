package generator

import (
	"strings"
	"testing"

	"shiro/internal/config"
	"shiro/internal/schema"
)

func TestCollectJoinColumnsDSGFallback(t *testing.T) {
	gen := &Generator{Config: config.Config{Features: config.Features{DSG: true}}}
	tbl := schema.Table{
		Name: "t1",
		Columns: []schema.Column{
			{Name: "k0", Type: schema.TypeBigInt},
			{Name: "x", Type: schema.TypeBigInt, HasIndex: true},
		},
		Indexes: []schema.Index{
			{Name: "idx_x", Columns: []string{"x"}},
		},
	}

	cols := gen.collectJoinColumns(tbl, true)
	if len(cols) == 0 {
		t.Fatalf("expected join columns, got none")
	}
	for _, col := range cols {
		if !strings.HasPrefix(col.Name, "k") {
			t.Fatalf("expected DSG join columns to prefer k*, got %s", col.Name)
		}
	}
}

func TestJoinConditionFromUsingFallback(t *testing.T) {
	left := []schema.Table{
		{Name: "t0", Columns: []schema.Column{{Name: "a", Type: schema.TypeInt}}},
	}
	right := schema.Table{Name: "t1", Columns: []schema.Column{{Name: "a", Type: schema.TypeInt}}}

	expr := joinConditionFromUsing(left, right, []string{"a"})
	bin, ok := expr.(BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", expr)
	}
	leftExpr, ok := bin.Left.(ColumnExpr)
	if !ok {
		t.Fatalf("expected left ColumnExpr, got %T", bin.Left)
	}
	rightExpr, ok := bin.Right.(ColumnExpr)
	if !ok {
		t.Fatalf("expected right ColumnExpr, got %T", bin.Right)
	}
	if leftExpr.Ref.Table != "t0" || leftExpr.Ref.Name != "a" {
		t.Fatalf("unexpected left ref %s.%s", leftExpr.Ref.Table, leftExpr.Ref.Name)
	}
	if rightExpr.Ref.Table != "t1" || rightExpr.Ref.Name != "a" {
		t.Fatalf("unexpected right ref %s.%s", rightExpr.Ref.Table, rightExpr.Ref.Name)
	}
	if bin.Op != "=" {
		t.Fatalf("unexpected op %s", bin.Op)
	}
}

func TestNaturalJoinAllowedRejectsDuplicateNames(t *testing.T) {
	gen := &Generator{}
	left := []schema.Table{
		{Name: "t0", Columns: []schema.Column{{Name: "id", Type: schema.TypeBigInt}}},
		{Name: "t1", Columns: []schema.Column{{Name: "id", Type: schema.TypeBigInt}}},
	}
	right := schema.Table{Name: "t2", Columns: []schema.Column{{Name: "id", Type: schema.TypeBigInt}}}
	if gen.naturalJoinAllowed(left, right) {
		t.Fatalf("expected natural join to be disallowed for duplicate left columns")
	}
}

func TestNaturalJoinAllowedAcceptsUniqueNames(t *testing.T) {
	gen := &Generator{}
	left := []schema.Table{
		{Name: "t0", Columns: []schema.Column{{Name: "id", Type: schema.TypeBigInt}}},
	}
	right := schema.Table{Name: "t1", Columns: []schema.Column{{Name: "id", Type: schema.TypeBigInt}}}
	if !gen.naturalJoinAllowed(left, right) {
		t.Fatalf("expected natural join to be allowed with unique left columns")
	}
}

// GroundTruth retries queries when no join columns are present, so we do not
// relax join column pairing here.
