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
