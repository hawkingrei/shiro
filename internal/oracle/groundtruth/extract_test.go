package groundtruth

import (
	"testing"

	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestJoinEdgesFromQueryUsingSelectsValidKey(t *testing.T) {
	state := makeState(map[string][]string{
		"t0": {"a"},
		"t1": {"a", "b"},
	})
	query := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Using: []string{"b", "a"}},
			},
		},
	}
	edges := JoinEdgesFromQuery(query, state)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	if edges[0].LeftKey != "a" || edges[0].RightKey != "a" {
		t.Fatalf("expected USING to select key a, got %s=%s", edges[0].LeftKey, edges[0].RightKey)
	}
}

func TestJoinEdgesFromQueryUsingCompositeKeys(t *testing.T) {
	state := makeState(map[string][]string{
		"t0": {"a", "b"},
		"t1": {"a", "b"},
	})
	query := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Using: []string{"a", "b"}},
			},
		},
	}
	edges := JoinEdgesFromQuery(query, state)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	if len(edges[0].LeftKeys) != 2 || len(edges[0].RightKeys) != 2 {
		t.Fatalf("expected 2 composite keys, got %d/%d", len(edges[0].LeftKeys), len(edges[0].RightKeys))
	}
}

func TestJoinEdgesFromQueryOnAndPrefersJoinTable(t *testing.T) {
	state := makeState(map[string][]string{
		"t0": {"a", "b"},
		"t1": {"a"},
		"t2": {"b"},
	})
	on := generator.BinaryExpr{
		Left: generator.BinaryExpr{
			Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "a"}},
			Op:    "=",
			Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "a"}},
		},
		Op: "AND",
		Right: generator.BinaryExpr{
			Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "b"}},
			Op:    "=",
			Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t2", Name: "b"}},
		},
	}
	query := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", On: on},
			},
		},
	}
	edges := JoinEdgesFromQuery(query, state)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	if edges[0].LeftKey != "a" || edges[0].RightKey != "a" {
		t.Fatalf("expected join key a, got %s=%s", edges[0].LeftKey, edges[0].RightKey)
	}
}

func TestJoinEdgesFromQueryOnUnqualifiedUniqueColumns(t *testing.T) {
	state := makeState(map[string][]string{
		"t0": {"a"},
		"t1": {"b"},
	})
	on := generator.BinaryExpr{
		Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Name: "a"}},
		Op:    "=",
		Right: generator.ColumnExpr{Ref: generator.ColumnRef{Name: "b"}},
	}
	query := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", On: on},
			},
		},
	}
	edges := JoinEdgesFromQuery(query, state)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	if edges[0].LeftTable != "t0" || edges[0].RightTable != "t1" {
		t.Fatalf("unexpected tables %s=%s", edges[0].LeftTable, edges[0].RightTable)
	}
	if edges[0].LeftKey != "a" || edges[0].RightKey != "b" {
		t.Fatalf("expected join key a=b, got %s=%s", edges[0].LeftKey, edges[0].RightKey)
	}
}

func TestJoinEdgesFromQueryOnUnqualifiedAmbiguousColumns(t *testing.T) {
	state := makeState(map[string][]string{
		"t0": {"id"},
		"t1": {"id"},
	})
	on := generator.BinaryExpr{
		Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Name: "id"}},
		Op:    "=",
		Right: generator.ColumnExpr{Ref: generator.ColumnRef{Name: "id"}},
	}
	query := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", On: on},
			},
		},
	}
	edges := JoinEdgesFromQuery(query, state)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	if edges[0].LeftKey != "" || edges[0].RightKey != "" {
		t.Fatalf("expected ambiguous join keys to be unresolved, got %s=%s", edges[0].LeftKey, edges[0].RightKey)
	}
}

func TestJoinEdgesFromQueryOnDoubleNotAndNullEQ(t *testing.T) {
	state := makeState(map[string][]string{
		"t0": {"k0"},
		"t1": {"k0"},
	})
	on := generator.UnaryExpr{
		Op: "NOT",
		Expr: generator.UnaryExpr{
			Op: "NOT",
			Expr: generator.BinaryExpr{
				Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "k0"}},
				Op:    "<=>",
				Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "k0"}},
			},
		},
	}
	query := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", On: on},
			},
		},
	}
	edges := JoinEdgesFromQuery(query, state)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	if edges[0].LeftKey != "k0" || edges[0].RightKey != "k0" {
		t.Fatalf("expected join key k0, got %s=%s", edges[0].LeftKey, edges[0].RightKey)
	}
}

func makeState(tables map[string][]string) *schema.State {
	out := &schema.State{Tables: make([]schema.Table, 0, len(tables))}
	for name, cols := range tables {
		tbl := schema.Table{Name: name, Columns: make([]schema.Column, 0, len(cols))}
		for _, col := range cols {
			tbl.Columns = append(tbl.Columns, schema.Column{Name: col, Type: schema.TypeInt})
		}
		out.Tables = append(out.Tables, tbl)
	}
	return out
}
