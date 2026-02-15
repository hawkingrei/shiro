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

func TestTemplateJoinPredicateStrategyJoinOnly(t *testing.T) {
	gen := newTestGenerator(t)
	gen.Config.Weights.Features.TemplateJoinOnlyWeight = 100
	gen.Config.Weights.Features.TemplateJoinFilterWeight = 0
	query := &SelectQuery{}
	gen.applyTemplateJoinPredicate(query, gen.State.Tables)
	if query.TemplateJoinPredicateStrategy != templateJoinPredicateStrategyJoinOnly {
		t.Fatalf("expected strategy %q, got %q", templateJoinPredicateStrategyJoinOnly, query.TemplateJoinPredicateStrategy)
	}
	if query.Where != nil {
		t.Fatalf("expected join-only strategy to keep WHERE nil")
	}
}

func TestTemplateJoinPredicateStrategyJoinFilter(t *testing.T) {
	gen := newTestGenerator(t)
	gen.Config.Weights.Features.TemplateJoinOnlyWeight = 0
	gen.Config.Weights.Features.TemplateJoinFilterWeight = 100
	gen.SetPredicateMode(PredicateModeSimpleColumns)
	query := &SelectQuery{}
	gen.applyTemplateJoinPredicate(query, gen.State.Tables)
	if query.TemplateJoinPredicateStrategy != templateJoinPredicateStrategyJoinFilter {
		t.Fatalf("expected strategy %q, got %q", templateJoinPredicateStrategyJoinFilter, query.TemplateJoinPredicateStrategy)
	}
}

func TestTemplateJoinPredicateStrategyPropagatesToFeatures(t *testing.T) {
	gen := newTestGenerator(t)
	query := &SelectQuery{
		Items:                         []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From:                          FromClause{BaseTable: "t0"},
		TemplateJoinPredicateStrategy: templateJoinPredicateStrategyJoinFilter,
	}
	gen.setLastFeatures(query, false, "")
	if gen.LastFeatures == nil {
		t.Fatalf("expected LastFeatures to be set")
	}
	if got := gen.LastFeatures.TemplateJoinPredicateStrategy; got != templateJoinPredicateStrategyJoinFilter {
		t.Fatalf("unexpected strategy in features: got=%q want=%q", got, templateJoinPredicateStrategyJoinFilter)
	}
}
