package runner

import (
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestApplyOracleOverridesGroundTruth(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.Views = true
	cfg.Features.DerivedTables = true
	cfg.Features.SetOperations = true
	cfg.Features.Subqueries = true
	cfg.Features.NaturalJoins = true
	cfg.Features.FullJoinEmulation = true
	state := &schema.State{}
	r := &Runner{gen: generator.New(cfg, state, 1)}

	restore := r.applyOracleOverrides("GroundTruth")
	defer restore()

	if r.gen.Config.Features.Views {
		t.Fatalf("groundtruth override should disable views")
	}
	if r.gen.Config.Features.DerivedTables {
		t.Fatalf("groundtruth override should disable derived tables")
	}
	if r.gen.Config.Features.SetOperations {
		t.Fatalf("groundtruth override should disable set operations")
	}
	if r.gen.Config.Features.Subqueries {
		t.Fatalf("groundtruth override should disable subqueries")
	}
	if r.gen.Config.Features.NaturalJoins {
		t.Fatalf("groundtruth override should disable natural joins")
	}
	if r.gen.Config.Features.FullJoinEmulation {
		t.Fatalf("groundtruth override should disable full join emulation")
	}
	if r.gen.PredicateMode() != generator.PredicateModeNone {
		t.Fatalf("groundtruth override should set predicate mode none")
	}
}

func TestApplyOracleOverridesCODDTest(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.Subqueries = true
	cfg.Features.Aggregates = true
	cfg.Features.Views = true
	cfg.Features.WindowFuncs = true
	state := &schema.State{}
	r := &Runner{gen: generator.New(cfg, state, 2)}

	restore := r.applyOracleOverrides("CODDTest")
	defer restore()

	if r.gen.Config.Features.Subqueries {
		t.Fatalf("coddtest override should disable subqueries")
	}
	if r.gen.Config.Features.Aggregates {
		t.Fatalf("coddtest override should disable aggregates")
	}
	if r.gen.Config.Features.Views {
		t.Fatalf("coddtest override should disable views")
	}
	if r.gen.Config.Features.WindowFuncs {
		t.Fatalf("coddtest override should disable window funcs")
	}
	if r.gen.PredicateMode() != generator.PredicateModeSimpleColumns {
		t.Fatalf("coddtest override should set predicate mode simple-columns")
	}
}

func TestApplyOracleOverridesPQS(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.Subqueries = true
	cfg.Features.SetOperations = true
	cfg.Features.Views = true
	cfg.Features.OrderBy = true
	state := &schema.State{}
	r := &Runner{gen: generator.New(cfg, state, 3)}

	restore := r.applyOracleOverrides("PQS")
	defer restore()

	if r.gen.Config.Features.Subqueries {
		t.Fatalf("pqs override should disable subqueries")
	}
	if r.gen.Config.Features.SetOperations {
		t.Fatalf("pqs override should disable set operations")
	}
	if r.gen.Config.Features.Views {
		t.Fatalf("pqs override should disable views")
	}
	if r.gen.Config.Features.OrderBy {
		t.Fatalf("pqs override should disable order by")
	}
	if r.gen.PredicateMode() != generator.PredicateModeNone {
		t.Fatalf("pqs override should set predicate mode none")
	}
}

func TestApplyOracleOverridesAllowSubquery(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.Subqueries = false
	cfg.Features.NotExists = false
	cfg.Features.NotIn = false
	state := &schema.State{}
	r := &Runner{gen: generator.New(cfg, state, 4)}

	restore := r.applyOracleOverrides("EET")

	if !r.gen.Config.Features.Subqueries {
		t.Fatalf("eet override should enable subqueries")
	}
	if !r.gen.Config.Features.NotExists {
		t.Fatalf("eet override should enable not exists")
	}
	if !r.gen.Config.Features.NotIn {
		t.Fatalf("eet override should enable not in")
	}

	restore()

	if r.gen.Config.Features.Subqueries != cfg.Features.Subqueries {
		t.Fatalf("expected subqueries restored")
	}
	if r.gen.Config.Features.NotExists != cfg.Features.NotExists {
		t.Fatalf("expected not exists restored")
	}
	if r.gen.Config.Features.NotIn != cfg.Features.NotIn {
		t.Fatalf("expected not in restored")
	}
}

func TestApplyOracleOverridesJoinUsingProbMin(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.JoinUsingProb = 0
	state := &schema.State{}
	r := &Runner{gen: generator.New(cfg, state, 5)}

	restore := r.applyOracleOverrides("GroundTruth")

	if r.gen.Config.Oracles.JoinUsingProb < 100 {
		t.Fatalf("groundtruth override should raise join using prob, got %d", r.gen.Config.Oracles.JoinUsingProb)
	}

	restore()

	if r.gen.Config.Oracles.JoinUsingProb != cfg.Oracles.JoinUsingProb {
		t.Fatalf("expected join using prob restored")
	}
}
