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
