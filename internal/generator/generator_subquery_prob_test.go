package generator

import (
	"math/rand"
	"testing"

	"shiro/internal/schema"
)

func TestPickQuantifiedComparisonAllReducesEqualsProbability(t *testing.T) {
	const samples = 5000

	allGen := &Generator{Rand: rand.New(rand.NewSource(1))}
	anyGen := &Generator{Rand: rand.New(rand.NewSource(1))}

	allEq := 0
	anyEq := 0
	for i := 0; i < samples; i++ {
		if allGen.pickQuantifiedComparison("ALL") == "=" {
			allEq++
		}
		if anyGen.pickQuantifiedComparison("ANY") == "=" {
			anyEq++
		}
	}

	if allEq == 0 {
		t.Fatalf("expected '=' to still appear occasionally for ALL")
	}
	if allEq >= anyEq/2 {
		t.Fatalf("expected '=' frequency for ALL to be much lower, got all=%d any=%d", allEq, anyEq)
	}
}

func TestPickInnerTableForTypePrefersNonOuterTable(t *testing.T) {
	const samples = 5000

	t0 := schema.Table{
		Name:    "t0",
		Columns: []schema.Column{{Name: "c0", Type: schema.TypeBigInt}},
	}
	t1 := schema.Table{
		Name:    "t1",
		Columns: []schema.Column{{Name: "c0", Type: schema.TypeBigInt}},
	}

	gen := &Generator{
		Rand:  rand.New(rand.NewSource(2)),
		State: &schema.State{Tables: []schema.Table{t0, t1}},
	}

	sameNameCount := 0
	for i := 0; i < samples; i++ {
		picked, ok := gen.pickInnerTableForType([]schema.Table{t0}, schema.TypeBigInt)
		if !ok {
			t.Fatalf("expected an inner table to be picked")
		}
		if picked.Name == "t0" {
			sameNameCount++
		}
	}

	if sameNameCount == 0 {
		t.Fatalf("expected same-table picks to still happen occasionally")
	}
	if sameNameCount >= samples/3 {
		t.Fatalf("expected same-table picks to be reduced, got %d/%d", sameNameCount, samples)
	}
}

func TestPickInnerTableForTypeFallsBackWhenOnlyOuterExists(t *testing.T) {
	t0 := schema.Table{
		Name:    "t0",
		Columns: []schema.Column{{Name: "c0", Type: schema.TypeBigInt}},
	}
	gen := &Generator{
		Rand:  rand.New(rand.NewSource(3)),
		State: &schema.State{Tables: []schema.Table{t0}},
	}

	picked, ok := gen.pickInnerTableForType([]schema.Table{t0}, schema.TypeBigInt)
	if !ok {
		t.Fatalf("expected fallback pick when only outer table exists")
	}
	if picked.Name != "t0" {
		t.Fatalf("expected fallback to t0, got %s", picked.Name)
	}
}

func TestGenerateSelectQueryKeepsPredicateSubqueriesWhenScalarSubqueriesDisabled(t *testing.T) {
	gen := newTestGenerator(t)
	gen.Config.TQS.Enabled = true
	gen.Config.Features.CTE = false
	gen.Config.Features.Aggregates = false
	gen.Config.Features.GroupBy = false
	gen.Config.Features.Having = false
	gen.Config.Features.Distinct = false
	gen.Config.Features.OrderBy = false
	gen.Config.Features.Limit = false
	gen.Config.Features.WindowFuncs = false
	gen.Config.Features.SetOperations = false
	gen.Config.Features.DerivedTables = false
	gen.SetDisallowScalarSubquery(true)
	gen.SetAdaptiveWeights(AdaptiveWeights{SubqCount: 100})

	for i := 0; i < 200; i++ {
		query := gen.GenerateSelectQuery()
		if query == nil || query.Where == nil {
			continue
		}
		if !exprHasPredicateSubquery(query.Where) {
			continue
		}
		if exprHasScalarSubquery(query.Where) {
			t.Fatalf("unexpected scalar subquery in predicate: %s", exprSQLForTest(query.Where))
		}
		for _, item := range query.Items {
			if exprHasScalarSubquery(item.Expr) {
				t.Fatalf("unexpected scalar subquery in select item: %s", exprSQLForTest(item.Expr))
			}
		}
		if gen.LastFeatures == nil {
			t.Fatalf("expected last features to be recorded")
		}
		if !gen.LastFeatures.SubqueryAllowed {
			t.Fatalf("expected predicate subqueries to remain allowed")
		}
		if gen.LastFeatures.SubqueryDisallowReason != "scalar_subquery_off" {
			t.Fatalf("unexpected disallow reason: %q", gen.LastFeatures.SubqueryDisallowReason)
		}
		return
	}

	t.Fatalf("expected a predicate subquery after repeated attempts")
}

func exprHasPredicateSubquery(expr Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case ExistsExpr:
		return true
	case *ExistsExpr:
		return e != nil
	case CompareSubqueryExpr:
		return true
	case *CompareSubqueryExpr:
		return e != nil
	case InExpr:
		for _, item := range e.List {
			switch item.(type) {
			case SubqueryExpr, *SubqueryExpr:
				return true
			}
			if exprHasPredicateSubquery(item) {
				return true
			}
		}
		return exprHasPredicateSubquery(e.Left)
	case *InExpr:
		if e == nil {
			return false
		}
		return exprHasPredicateSubquery(*e)
	case UnaryExpr:
		return exprHasPredicateSubquery(e.Expr)
	case *UnaryExpr:
		if e == nil {
			return false
		}
		return exprHasPredicateSubquery(e.Expr)
	case BinaryExpr:
		return exprHasPredicateSubquery(e.Left) || exprHasPredicateSubquery(e.Right)
	case *BinaryExpr:
		if e == nil {
			return false
		}
		return exprHasPredicateSubquery(e.Left) || exprHasPredicateSubquery(e.Right)
	case FuncExpr:
		for _, arg := range e.Args {
			if exprHasPredicateSubquery(arg) {
				return true
			}
		}
	case *FuncExpr:
		if e == nil {
			return false
		}
		for _, arg := range e.Args {
			if exprHasPredicateSubquery(arg) {
				return true
			}
		}
	case GroupByOrdinalExpr:
		return exprHasPredicateSubquery(e.Expr)
	case *GroupByOrdinalExpr:
		if e == nil {
			return false
		}
		return exprHasPredicateSubquery(e.Expr)
	}
	return false
}

func exprHasScalarSubquery(expr Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case SubqueryExpr:
		return true
	case *SubqueryExpr:
		return e != nil
	case ExistsExpr:
		return false
	case *ExistsExpr:
		return false
	case CompareSubqueryExpr:
		return exprHasScalarSubquery(e.Left)
	case *CompareSubqueryExpr:
		if e == nil {
			return false
		}
		return exprHasScalarSubquery(e.Left)
	case InExpr:
		if exprHasScalarSubquery(e.Left) {
			return true
		}
		for _, item := range e.List {
			switch item.(type) {
			case SubqueryExpr, *SubqueryExpr:
				continue
			}
			if exprHasScalarSubquery(item) {
				return true
			}
		}
		return false
	case *InExpr:
		if e == nil {
			return false
		}
		return exprHasScalarSubquery(*e)
	case UnaryExpr:
		return exprHasScalarSubquery(e.Expr)
	case *UnaryExpr:
		if e == nil {
			return false
		}
		return exprHasScalarSubquery(e.Expr)
	case BinaryExpr:
		return exprHasScalarSubquery(e.Left) || exprHasScalarSubquery(e.Right)
	case *BinaryExpr:
		if e == nil {
			return false
		}
		return exprHasScalarSubquery(e.Left) || exprHasScalarSubquery(e.Right)
	case FuncExpr:
		for _, arg := range e.Args {
			if exprHasScalarSubquery(arg) {
				return true
			}
		}
		return false
	case *FuncExpr:
		if e == nil {
			return false
		}
		for _, arg := range e.Args {
			if exprHasScalarSubquery(arg) {
				return true
			}
		}
		return false
	case GroupByOrdinalExpr:
		return exprHasScalarSubquery(e.Expr)
	case *GroupByOrdinalExpr:
		if e == nil {
			return false
		}
		return exprHasScalarSubquery(e.Expr)
	}
	return false
}

func exprSQLForTest(expr Expr) string {
	var b SQLBuilder
	expr.Build(&b)
	return b.String()
}
