package generator

import (
	"strings"
	"testing"

	"shiro/internal/config"
	"shiro/internal/schema"
)

func TestInsertSQLSkipsWhenForeignKeyIDExhausted(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name:   "t0",
				HasPK:  true,
				NextID: 3, // Existing parent IDs: 1,2.
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt, Nullable: false},
				},
			},
			{
				Name:   "t1",
				HasPK:  true,
				NextID: 4, // Child would need id=4, but parent max id is 2.
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt, Nullable: false},
				},
				ForeignKeys: []schema.ForeignKey{
					{Name: "fk_1", Table: "t1", Column: "id", RefTable: "t0", RefColumn: "id"},
				},
			},
		},
	}
	gen := newDMLFKTestGenerator(t, state)
	sql := gen.InsertSQL(&state.Tables[1])
	if strings.TrimSpace(sql) != "" {
		t.Fatalf("expected empty insert SQL when fk id is exhausted, got: %s", sql)
	}
	if state.Tables[1].NextID != 4 {
		t.Fatalf("unexpected child next id advance: %d", state.Tables[1].NextID)
	}
}

func TestInsertSQLKeepsForeignKeyIDWithinParentRange(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name:   "t0",
				HasPK:  true,
				NextID: 5, // Existing parent IDs: 1..4.
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt, Nullable: false},
				},
			},
			{
				Name:   "t1",
				HasPK:  true,
				NextID: 2,
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt, Nullable: false},
					{Name: "c0", Type: schema.TypeInt, Nullable: true},
				},
				ForeignKeys: []schema.ForeignKey{
					{Name: "fk_2", Table: "t1", Column: "id", RefTable: "t0", RefColumn: "id"},
				},
			},
		},
	}
	gen := newDMLFKTestGenerator(t, state)
	sql := gen.InsertSQL(&state.Tables[1])
	if strings.TrimSpace(sql) == "" {
		t.Fatalf("expected non-empty insert SQL")
	}
	if !strings.HasPrefix(sql, "INSERT INTO t1 ") {
		t.Fatalf("unexpected insert SQL: %s", sql)
	}
	if state.Tables[1].NextID <= 2 {
		t.Fatalf("expected child next id to advance, got %d", state.Tables[1].NextID)
	}
	if state.Tables[1].NextID > state.Tables[0].NextID {
		t.Fatalf("child next id exceeded parent range, child=%d parent=%d", state.Tables[1].NextID, state.Tables[0].NextID)
	}
}

func TestPickUpdatableColumnSkipsForeignKeyColumns(t *testing.T) {
	tbl := schema.Table{
		Name: "t1",
		Columns: []schema.Column{
			{Name: "id", Type: schema.TypeBigInt, Nullable: false},
			{Name: "c0", Type: schema.TypeBigInt, Nullable: false},
			{Name: "c1", Type: schema.TypeInt, Nullable: true},
		},
		ForeignKeys: []schema.ForeignKey{
			{Name: "fk_3", Table: "t1", Column: "c0", RefTable: "t0", RefColumn: "id"},
		},
	}
	state := &schema.State{Tables: []schema.Table{tbl}}
	gen := newDMLFKTestGenerator(t, state)
	for i := 0; i < 20; i++ {
		col, ok := gen.pickUpdatableColumn(tbl)
		if !ok {
			t.Fatalf("expected updatable column")
		}
		if col.Name == "c0" {
			t.Fatalf("foreign key column should not be selected for update")
		}
	}
}

func newDMLFKTestGenerator(t *testing.T, state *schema.State) *Generator {
	t.Helper()
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return New(cfg, state, 21)
}
