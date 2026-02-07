package generator

import "testing"

func TestTemplateFromCanUseDerivedTablesWhenEnabled(t *testing.T) {
	gen := newTestGenerator(t)
	gen.Config.Features.DerivedTables = true
	tables := gen.State.Tables
	if len(tables) < 2 {
		t.Fatalf("need at least two tables for template from test")
	}

	foundDerived := false
	for i := 0; i < 200; i++ {
		query := &SelectQuery{}
		gen.applyTemplateFrom(query, tables)
		if query.From.BaseQuery != nil {
			foundDerived = true
			break
		}
		for _, join := range query.From.Joins {
			if join.TableQuery != nil {
				foundDerived = true
				break
			}
		}
		if foundDerived {
			break
		}
	}

	if !foundDerived {
		t.Fatalf("expected template from path to produce derived tables when feature is enabled")
	}
}
