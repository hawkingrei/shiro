package runner

import (
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestApplyOracleOverridesGroundTruth(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.Views = true
	cfg.Features.DerivedTables = true
	cfg.Features.SetOperations = true
	cfg.Features.Subqueries = true
	cfg.Features.NaturalJoins = true
	cfg.Features.FullJoinEmulation = true
	cfg.MaxJoinTables = 15
	state := &schema.State{}
	r := &Runner{gen: generator.New(cfg, state, 1)}

	restore := r.applyOracleOverrides("GroundTruth")

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
	if r.gen.Config.MaxJoinTables != 4 {
		t.Fatalf("groundtruth override should cap max_join_tables to 4, got %d", r.gen.Config.MaxJoinTables)
	}

	restore()

	if r.gen.Config.MaxJoinTables != cfg.MaxJoinTables {
		t.Fatalf("expected max_join_tables restored")
	}
}

func TestApplyOracleOverridesCODDTest(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
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
	cfg, err := config.Load("../../config.example.yaml")
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

	if !r.gen.Config.Features.Subqueries {
		t.Fatalf("pqs override should enable subqueries")
	}
	if !r.gen.Config.Features.QuantifiedSubqueries {
		t.Fatalf("pqs override should enable quantified subqueries")
	}
	if r.gen.Config.Features.NotExists {
		t.Fatalf("pqs override should disable not exists")
	}
	if r.gen.Config.Features.NotIn {
		t.Fatalf("pqs override should disable not in")
	}
	if !r.gen.Config.Features.DerivedTables {
		t.Fatalf("pqs override should enable derived tables")
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

func TestApplyOracleOverridesDQPComplexityProfile(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.NaturalJoins = true
	cfg.Features.DerivedTables = true
	cfg.Features.SetOperations = true
	cfg.Oracles.JoinOnPolicy = "complex"
	state := &schema.State{}
	r := &Runner{gen: generator.New(cfg, state, 11)}

	restore := r.applyOracleOverrides("DQP")
	defer restore()

	if r.gen.Config.Features.DerivedTables {
		t.Fatalf("dqp override should disable derived tables")
	}
	if r.gen.Config.Features.SetOperations {
		t.Fatalf("dqp override should disable set operations")
	}
	if r.gen.Config.Features.NaturalJoins {
		t.Fatalf("dqp override should disable natural joins")
	}
	if r.gen.Config.Oracles.JoinOnPolicy != "simple" {
		t.Fatalf("dqp override should force simple join-on policy, got %q", r.gen.Config.Oracles.JoinOnPolicy)
	}
}

func TestApplyOracleOverridesNoRECDisablesNaturalJoins(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.NaturalJoins = true
	state := &schema.State{}
	r := &Runner{gen: generator.New(cfg, state, 12)}

	restore := r.applyOracleOverrides("NoREC")
	defer restore()

	if r.gen.Config.Features.NaturalJoins {
		t.Fatalf("norec override should disable natural joins")
	}
}

func TestApplyOracleOverridesTLPDisablesNaturalJoins(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.NaturalJoins = true
	state := &schema.State{}
	r := &Runner{gen: generator.New(cfg, state, 13)}

	restore := r.applyOracleOverrides("TLP")
	defer restore()

	if r.gen.Config.Features.NaturalJoins {
		t.Fatalf("tlp override should disable natural joins")
	}
}

func TestApplyOracleOverridesAllowSubquery(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
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

func TestApplyOracleOverridesGroupByExtensionFallback(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.SetOperations = true
	cfg.Features.GroupBy = true
	cfg.Features.GroupByRollup = true
	cfg.Features.GroupByCube = true
	cfg.Features.GroupByGroupingSets = true
	state := &schema.State{}
	r := &Runner{gen: generator.New(cfg, state, 6)}

	restore := r.applyOracleOverrides("EET")

	if !r.gen.Config.Features.GroupBy {
		t.Fatalf("eet override should keep group_by enabled")
	}
	if !r.gen.Config.Features.GroupByRollup {
		t.Fatalf("eet override should keep rollup fallback enabled")
	}
	if r.gen.Config.Features.SetOperations {
		t.Fatalf("eet override should disable set operations")
	}
	if r.gen.Config.Features.GroupByCube {
		t.Fatalf("eet override should disable group_by_cube for fallback")
	}
	if r.gen.Config.Features.GroupByGroupingSets {
		t.Fatalf("eet override should disable group_by_grouping_sets for fallback")
	}

	restore()

	if r.gen.Config.Features.GroupByCube != cfg.Features.GroupByCube {
		t.Fatalf("expected group_by_cube restored")
	}
	if r.gen.Config.Features.GroupByGroupingSets != cfg.Features.GroupByGroupingSets {
		t.Fatalf("expected group_by_grouping_sets restored")
	}
	if r.gen.Config.Features.SetOperations != cfg.Features.SetOperations {
		t.Fatalf("expected set_operations restored")
	}
}

func TestApplyOracleOverridesJoinUsingProbMin(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
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

func TestApplyOracleOverridesThroughputGuardDisablesHeavyFeatures(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.MaxJoinTables = 15
	cfg.Features.SetOperations = true
	cfg.Features.DerivedTables = true
	cfg.Features.QuantifiedSubqueries = true
	cfg.Features.WindowFuncs = true
	cfg.Features.WindowFrames = true
	state := &schema.State{}
	r := &Runner{
		gen:                 generator.New(cfg, state, 7),
		oracleTimeoutCounts: make(map[string]int64),
	}
	r.statsMu.Lock()
	r.throughputGuardTTL = 2
	r.statsMu.Unlock()

	restore := r.applyOracleOverrides("DQP")
	defer restore()

	if r.gen.Config.Features.SetOperations {
		t.Fatalf("throughput guard should disable set operations")
	}
	if r.gen.Config.Features.DerivedTables {
		t.Fatalf("throughput guard should disable derived tables")
	}
	if r.gen.Config.Features.QuantifiedSubqueries {
		t.Fatalf("throughput guard should disable quantified subqueries")
	}
	if r.gen.Config.Features.WindowFuncs {
		t.Fatalf("throughput guard should disable window funcs")
	}
	if r.gen.Config.Features.WindowFrames {
		t.Fatalf("throughput guard should disable window frames")
	}
	if r.gen.Config.MaxJoinTables > 4 {
		t.Fatalf("throughput guard should cap max join tables, got %d", r.gen.Config.MaxJoinTables)
	}
}
