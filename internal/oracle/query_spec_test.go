package oracle

import (
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestApplyProfileToSpec(t *testing.T) {
	constraints := generator.SelectQueryConstraints{}
	profile := Profile{
		Features: FeatureOverrides{
			SetOperations: BoolPtr(false),
		},
		AllowSubquery: BoolPtr(false),
		MinJoinTables: IntPtr(2),
	}

	applyProfileToSpec(&constraints, &profile)

	if !constraints.DisallowSetOps {
		t.Fatalf("expected DisallowSetOps")
	}
	if !constraints.DisallowSubquery {
		t.Fatalf("expected DisallowSubquery")
	}
	if !constraints.MinJoinTablesSet || constraints.MinJoinTables != 2 {
		t.Fatalf("expected MinJoinTables=2, got %d (set=%v)", constraints.MinJoinTables, constraints.MinJoinTablesSet)
	}
}

func TestApplyProfileToSpecDoesNotRelaxSubquery(t *testing.T) {
	constraints := generator.SelectQueryConstraints{DisallowSubquery: true}
	profile := Profile{AllowSubquery: BoolPtr(true)}

	applyProfileToSpec(&constraints, &profile)

	if !constraints.DisallowSubquery {
		t.Fatalf("expected DisallowSubquery to remain true")
	}
}

func TestBuildQueryWithSpecAppliesProfileConstraints(t *testing.T) {
	gen := newProfileTestGenerator(t)
	spec := QuerySpec{
		Oracle: "profile_test",
		Profile: &Profile{
			Features: FeatureOverrides{
				SetOperations: BoolPtr(false),
			},
			AllowSubquery: BoolPtr(false),
			MinJoinTables: IntPtr(2),
		},
		MaxTries: 50,
	}

	query, details := buildQueryWithSpec(gen, spec)
	if query == nil {
		t.Fatalf("expected query, details=%v", details)
	}
	if len(query.SetOps) > 0 {
		t.Fatalf("unexpected set operations")
	}
	if generator.AnalyzeQueryFeatures(query).HasSubquery {
		t.Fatalf("unexpected subquery")
	}
	if len(query.From.Joins)+1 < 2 {
		t.Fatalf("expected at least 2 tables, got %d", len(query.From.Joins)+1)
	}
}

func newProfileTestGenerator(t *testing.T) *generator.Generator {
	t.Helper()
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.Joins = true
	cfg.Features.CTE = true
	cfg.Features.Subqueries = true
	cfg.Features.Aggregates = true
	cfg.Features.GroupBy = true
	cfg.Features.Having = true
	cfg.Features.OrderBy = true
	cfg.Features.Distinct = true
	cfg.Features.WindowFuncs = true
	cfg.Features.SetOperations = true

	state := schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeVarchar},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeDouble},
					{Name: "c1", Type: schema.TypeDatetime},
				},
			},
		},
	}
	return generator.New(cfg, &state, 11)
}
