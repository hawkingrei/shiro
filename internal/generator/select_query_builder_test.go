package generator

import (
	"strings"
	"testing"

	"shiro/internal/config"
	"shiro/internal/schema"
)

func TestSelectQueryBuilderRequireWhere(t *testing.T) {
	gen := newTestGenerator(t)
	query := NewSelectQueryBuilder(gen).
		RequireWhere().
		PredicateMode(PredicateModeSimple).
		RequireDeterministic().
		MaxTries(20).
		Build()
	if query == nil || query.Where == nil {
		t.Fatalf("expected query with where")
	}
	if !QueryDeterministic(query) {
		t.Fatalf("expected deterministic query")
	}
}

func TestSelectQueryBuilderPredicateGuard(t *testing.T) {
	gen := newTestGenerator(t)
	query := NewSelectQueryBuilder(gen).
		RequireWhere().
		PredicateMode(PredicateModeSimple).
		PredicateGuard(isSimplePredicateForTest).
		MaxTries(20).
		Build()
	if query == nil || query.Where == nil {
		t.Fatalf("expected query with where")
	}
	if !isSimplePredicateForTest(query.Where) {
		t.Fatalf("predicate guard not satisfied")
	}
}

func TestSelectQueryBuilderDisallowSubquery(t *testing.T) {
	gen := newTestGenerator(t)
	gen.Config.Features.Subqueries = true
	query := NewSelectQueryBuilder(gen).
		DisallowSubquery().
		MaxTries(50).
		Build()
	if query == nil {
		t.Fatalf("expected query")
	}
	if AnalyzeQueryFeatures(query).HasSubquery {
		t.Fatalf("unexpected subquery")
	}
}

func TestSelectQueryBuilderDisallowAggregate(t *testing.T) {
	gen := newTestGenerator(t)
	gen.Config.Features.Aggregates = true
	gen.Config.Features.GroupBy = true
	gen.Config.Features.Having = true
	query := NewSelectQueryBuilder(gen).
		DisallowAggregate().
		MaxTries(50).
		Build()
	if query == nil {
		t.Fatalf("expected query")
	}
	features := AnalyzeQueryFeatures(query)
	if features.HasAggregate || len(query.GroupBy) > 0 || query.Having != nil {
		t.Fatalf("unexpected aggregate")
	}
}

func TestSelectQueryBuilderDisallowLimit(t *testing.T) {
	gen := newTestGenerator(t)
	gen.Config.Features.Limit = true
	query := NewSelectQueryBuilder(gen).
		DisallowLimit().
		MaxTries(50).
		Build()
	if query == nil {
		t.Fatalf("expected query")
	}
	if query.Limit != nil {
		t.Fatalf("unexpected limit")
	}
}

func TestSelectQueryBuilderDisallowWindow(t *testing.T) {
	gen := newTestGenerator(t)
	gen.Config.Features.WindowFuncs = true
	query := NewSelectQueryBuilder(gen).
		DisallowWindow().
		MaxTries(50).
		Build()
	if query == nil {
		t.Fatalf("expected query")
	}
	if AnalyzeQueryFeatures(query).HasWindow {
		t.Fatalf("unexpected window")
	}
}

func TestSelectQueryBuilderMinJoinTables(t *testing.T) {
	gen := newTestGenerator(t)
	query := NewSelectQueryBuilder(gen).
		MinJoinTables(2).
		MaxTries(50).
		Build()
	if query == nil {
		t.Fatalf("expected query")
	}
	if len(query.From.Joins)+1 < 2 {
		t.Fatalf("expected at least 2 tables, got %d", len(query.From.Joins)+1)
	}
}

func newTestGenerator(t *testing.T) *Generator {
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

	state := schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeVarchar},
					{Name: "c2", Type: schema.TypeDate},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeDouble},
					{Name: "c1", Type: schema.TypeDatetime},
					{Name: "c2", Type: schema.TypeBool},
				},
			},
		},
	}
	return New(cfg, &state, 7)
}

func isSimplePredicateForTest(expr Expr) bool {
	switch e := expr.(type) {
	case BinaryExpr:
		op := strings.ToUpper(strings.TrimSpace(e.Op))
		if op == "AND" {
			return isSimplePredicateForTest(e.Left) && isSimplePredicateForTest(e.Right)
		}
		if !isComparisonOpForTest(op) {
			return false
		}
		return isSimpleOperandForTest(e.Left) && isSimpleOperandForTest(e.Right)
	case UnaryExpr:
		return false
	default:
		return false
	}
}

func isComparisonOpForTest(op string) bool {
	switch op {
	case "=", "<>", "<", "<=", ">", ">=", "<=>":
		return true
	default:
		return false
	}
}

func isSimpleOperandForTest(expr Expr) bool {
	switch expr.(type) {
	case ColumnExpr, LiteralExpr:
		return true
	default:
		return false
	}
}
