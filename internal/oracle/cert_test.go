package oracle

import (
	"context"
	"errors"
	"testing"

	"shiro/internal/config"
	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestCERTNoTablesSkip(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	state := schema.State{}
	gen := generator.New(cfg, &state, 3)
	res := (CERT{}).Run(context.Background(), nil, gen, &state)
	if res.OK != true {
		t.Fatalf("expected OK skip")
	}
	if res.Details["skip_reason"] == nil {
		t.Fatalf("expected skip reason")
	}
}

func TestCERTSelectConstraintsGuardrails(t *testing.T) {
	c := certSelectConstraints()
	if !c.RequireWhere {
		t.Fatalf("expected RequireWhere")
	}
	if c.PredicateMode != generator.PredicateModeSimple {
		t.Fatalf("unexpected predicate mode: %v", c.PredicateMode)
	}
	if !c.RequireDeterministic {
		t.Fatalf("expected RequireDeterministic")
	}
	if !c.DisallowAggregate || !c.DisallowDistinct || !c.DisallowGroupBy || !c.DisallowHaving || !c.DisallowOrderBy || !c.DisallowSetOps || !c.DisallowWindow {
		t.Fatalf("expected CERT guardrails to disallow aggregate/distinct/group/order/having/setops/window")
	}
}

func TestCERTExplainErrorReturnsSQLFeaturesForReturnedSQL(t *testing.T) {
	gen := newProfileTestGenerator(t)
	expectedErr := errors.New("explain validation failure")
	exec := &db.DB{
		Validate: func(string) error {
			return expectedErr
		},
	}

	res := (CERT{}).Run(context.Background(), exec, gen, gen.State)
	if res.Err == nil {
		t.Fatalf("expected validation error result")
	}
	if len(res.SQL) != 1 {
		t.Fatalf("expected single EXPLAIN SQL, got %v", res.SQL)
	}
	if _, ok := res.SQLFeatures[res.SQL[0]]; !ok {
		t.Fatalf("missing SQL features for returned SQL %q", res.SQL[0])
	}
}
