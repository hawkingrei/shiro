package groundtruth

import (
	"testing"

	"shiro/internal/schema"
)

func TestJoinEdgesFromSQLWithAliases(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{Name: "t0", Columns: []schema.Column{{Name: "k0", Type: schema.TypeInt}}},
			{Name: "t1", Columns: []schema.Column{{Name: "k0", Type: schema.TypeInt}}},
		},
	}
	sqlText := "SELECT * FROM t0 AS a JOIN t1 AS b ON a.k0 = b.k0"
	edges := JoinEdgesFromSQL(sqlText, state)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	if edges[0].LeftTable != "t0" || edges[0].RightTable != "t1" {
		t.Fatalf("unexpected tables %s=%s", edges[0].LeftTable, edges[0].RightTable)
	}
	if edges[0].LeftKey != "k0" || edges[0].RightKey != "k0" {
		t.Fatalf("expected join key k0, got %s=%s", edges[0].LeftKey, edges[0].RightKey)
	}
}

func TestRefineJoinEdgesWithSQL(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{Name: "t0", Columns: []schema.Column{{Name: "k0", Type: schema.TypeInt}}},
			{Name: "t1", Columns: []schema.Column{{Name: "k0", Type: schema.TypeInt}}},
		},
	}
	edges := []JoinEdge{
		{LeftTable: "t0", RightTable: "t1", JoinType: JoinInner},
	}
	sqlText := "SELECT * FROM t0 JOIN t1 ON t0.k0 = t1.k0"
	refined := RefineJoinEdgesWithSQL(sqlText, state, edges, 1)
	if refined[0].LeftKey != "k0" || refined[0].RightKey != "k0" {
		t.Fatalf("expected refined join key k0, got %s=%s", refined[0].LeftKey, refined[0].RightKey)
	}
}

func TestJoinEdgesFromSQLUsingCompositeKeys(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{Name: "t0", Columns: []schema.Column{{Name: "a", Type: schema.TypeInt}, {Name: "b", Type: schema.TypeInt}}},
			{Name: "t1", Columns: []schema.Column{{Name: "a", Type: schema.TypeInt}, {Name: "b", Type: schema.TypeInt}}},
		},
	}
	sqlText := "SELECT * FROM t0 JOIN t1 USING (a, b)"
	edges := JoinEdgesFromSQL(sqlText, state)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	if len(edges[0].LeftKeys) != 2 || len(edges[0].RightKeys) != 2 {
		t.Fatalf("expected composite keys, got %d/%d", len(edges[0].LeftKeys), len(edges[0].RightKeys))
	}
}
