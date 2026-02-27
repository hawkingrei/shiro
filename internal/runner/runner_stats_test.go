package runner

import (
	"reflect"
	"testing"

	"shiro/internal/generator"
	"shiro/internal/oracle"
	"shiro/internal/tqs"
)

func TestCollectGroundTruthDSGMismatchReasons(t *testing.T) {
	delta := map[string]oracleFunnel{
		"GroundTruth": {
			SkipReasons: map[string]int64{
				"groundtruth:dsg_key_mismatch_right_key": 2,
				"groundtruth:dsg_key_mismatch_left_key":  1,
				"groundtruth:dsg_key_mismatch":           3,
				"groundtruth:dsg_key_mismatch:right_key": 4,
				"groundtruth:key_missing":                5,
			},
		},
	}
	got := collectGroundTruthDSGMismatchReasons(delta)
	want := map[string]int64{
		"right_key": 6,
		"left_key":  1,
		"unknown":   3,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected mismatch reasons: got=%v want=%v", got, want)
	}
}

func TestCollectGroundTruthDSGMismatchReasonsEmpty(t *testing.T) {
	if got := collectGroundTruthDSGMismatchReasons(nil); got != nil {
		t.Fatalf("expected nil for empty funnel, got %v", got)
	}
	if got := collectGroundTruthDSGMismatchReasons(map[string]oracleFunnel{}); got != nil {
		t.Fatalf("expected nil for empty map, got %v", got)
	}
	if got := collectGroundTruthDSGMismatchReasons(map[string]oracleFunnel{
		"GroundTruth": {SkipReasons: map[string]int64{"groundtruth:key_missing": 1}},
	}); got != nil {
		t.Fatalf("expected nil for no dsg mismatch reasons, got %v", got)
	}
}

func TestNormalizeMinimizeStatus(t *testing.T) {
	cases := map[string]string{
		"":            "unknown",
		"  ":          "unknown",
		"in_progress": "in_progress",
		"in-progress": "in_progress",
		"inprogress":  "in_progress",
		"SUCCESS":     "success",
	}
	for input, expect := range cases {
		if got := normalizeMinimizeStatus(input); got != expect {
			t.Fatalf("normalizeMinimizeStatus(%q)=%q want=%q", input, got, expect)
		}
	}
}

func TestDiffCountMap(t *testing.T) {
	current := map[string]int64{
		"skipped":     5,
		"success":     2,
		"in_progress": 1,
	}
	previous := map[string]int64{
		"skipped":     3,
		"success":     2,
		"in_progress": 0,
	}
	got := diffCountMap(current, previous)
	want := map[string]int64{
		"skipped":     2,
		"in_progress": 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("diffCountMap() got=%v want=%v", got, want)
	}
}

func TestNormalizeTQSBaselineResetOnRewind(t *testing.T) {
	stats := tqs.Stats{
		Covered: 5,
		Steps:   10,
		Edges:   8,
	}
	covered, steps, edges := normalizeTQSBaseline(stats, 9, 12, 10)
	if covered != 0 || steps != 0 || edges != 0 {
		t.Fatalf("expected baseline reset, got covered=%d steps=%d edges=%d", covered, steps, edges)
	}
}

func TestNormalizeTQSBaselineKeepForwardProgress(t *testing.T) {
	stats := tqs.Stats{
		Covered: 9,
		Steps:   12,
		Edges:   10,
	}
	covered, steps, edges := normalizeTQSBaseline(stats, 9, 12, 10)
	if covered != 9 || steps != 12 || edges != 10 {
		t.Fatalf("expected baseline unchanged, got covered=%d steps=%d edges=%d", covered, steps, edges)
	}
}

func TestNormalizeTemplateJoinPredicateStrategy(t *testing.T) {
	cases := map[string]string{
		"":            "",
		"join_only":   "join_only",
		"JOIN_ONLY":   "join_only",
		"join_filter": "join_filter",
		"unknown":     "",
	}
	for input, expect := range cases {
		if got := generator.NormalizeTemplateJoinPredicateStrategy(input); got != expect {
			t.Fatalf("NormalizeTemplateJoinPredicateStrategy(%q)=%q want=%q", input, got, expect)
		}
	}
}

func TestObserveReproducibilitySummary(t *testing.T) {
	r := &Runner{
		capturedMinimizeStatus:  make(map[string]int64),
		capturedMinimizeReasons: make(map[string]int64),
	}
	r.observeReproducibilitySummary("in-progress", "")
	r.observeReproducibilitySummary("skipped", "base_replay_not_reproducible")
	r.observeReproducibilitySummary("skipped", "base_replay_not_reproducible")
	if r.capturedCases != 3 {
		t.Fatalf("capturedCases=%d want=3", r.capturedCases)
	}
	if r.capturedMinimizeStatus["in_progress"] != 1 {
		t.Fatalf("in_progress=%d want=1", r.capturedMinimizeStatus["in_progress"])
	}
	if r.capturedMinimizeStatus["skipped"] != 2 {
		t.Fatalf("skipped=%d want=2", r.capturedMinimizeStatus["skipped"])
	}
	if r.capturedMinimizeReasons["base_replay_not_reproducible"] != 2 {
		t.Fatalf("base_replay_not_reproducible=%d want=2", r.capturedMinimizeReasons["base_replay_not_reproducible"])
	}
}

func TestObserveJoinSignatureTemplateStrategy(t *testing.T) {
	r := &Runner{
		templateJoinPredicateStrategies: make(map[string]int64),
		subqueryOracleStats:             make(map[string]*subqueryOracleStats),
	}
	r.observeJoinSignature(&generator.QueryFeatures{TemplateJoinPredicateStrategy: "join_filter"}, "")
	r.observeJoinSignature(&generator.QueryFeatures{TemplateJoinPredicateStrategy: "JOIN_ONLY"}, "")
	r.observeJoinSignature(&generator.QueryFeatures{TemplateJoinPredicateStrategy: "invalid"}, "")
	if r.templateJoinPredicateStrategies["join_filter"] != 1 {
		t.Fatalf("join_filter=%d want=1", r.templateJoinPredicateStrategies["join_filter"])
	}
	if r.templateJoinPredicateStrategies["join_only"] != 1 {
		t.Fatalf("join_only=%d want=1", r.templateJoinPredicateStrategies["join_only"])
	}
	if _, ok := r.templateJoinPredicateStrategies["invalid"]; ok {
		t.Fatalf("invalid strategy should not be recorded")
	}
}

func TestObserveJoinSignatureWindowFrameAndIntervalArith(t *testing.T) {
	r := &Runner{
		templateJoinPredicateStrategies: make(map[string]int64),
		subqueryOracleStats:             make(map[string]*subqueryOracleStats),
	}
	r.observeJoinSignature(&generator.QueryFeatures{
		HasWindow:        true,
		HasWindowFrame:   true,
		HasIntervalArith: true,
	}, "")
	if r.genSQLWindow != 1 {
		t.Fatalf("genSQLWindow=%d want=1", r.genSQLWindow)
	}
	if r.genSQLWindowFrame != 1 {
		t.Fatalf("genSQLWindowFrame=%d want=1", r.genSQLWindowFrame)
	}
	if r.genSQLIntervalArith != 1 {
		t.Fatalf("genSQLIntervalArith=%d want=1", r.genSQLIntervalArith)
	}
}

func TestObserveJoinSignatureSetOpDerivedQuantified(t *testing.T) {
	r := &Runner{
		templateJoinPredicateStrategies: make(map[string]int64),
		subqueryOracleStats:             make(map[string]*subqueryOracleStats),
	}
	r.observeJoinSignature(&generator.QueryFeatures{
		HasSetOperations:        true,
		HasDerivedTables:        true,
		HasQuantifiedSubqueries: true,
	}, "")
	if r.genSQLSetOperations != 1 {
		t.Fatalf("genSQLSetOperations=%d want=1", r.genSQLSetOperations)
	}
	if r.genSQLDerivedTables != 1 {
		t.Fatalf("genSQLDerivedTables=%d want=1", r.genSQLDerivedTables)
	}
	if r.genSQLQuantifiedSubquery != 1 {
		t.Fatalf("genSQLQuantifiedSubquery=%d want=1", r.genSQLQuantifiedSubquery)
	}
}

func TestObserveJoinSignatureFullJoinAttemptReject(t *testing.T) {
	r := &Runner{
		templateJoinPredicateStrategies: make(map[string]int64),
		subqueryOracleStats:             make(map[string]*subqueryOracleStats),
		genSQLFullJoinRejected:          make(map[string]int64),
	}
	r.observeJoinSignature(&generator.QueryFeatures{
		FullJoinEmulationAttempted:    true,
		FullJoinEmulationRejectReason: "probability_gate",
	}, "")
	if r.genSQLFullJoinAttempted != 1 {
		t.Fatalf("genSQLFullJoinAttempted=%d want=1", r.genSQLFullJoinAttempted)
	}
	if r.genSQLFullJoinRejected["probability_gate"] != 1 {
		t.Fatalf("probability_gate=%d want=1", r.genSQLFullJoinRejected["probability_gate"])
	}
}

func TestObserveJoinSignatureFullJoinAttemptEmitted(t *testing.T) {
	r := &Runner{
		templateJoinPredicateStrategies: make(map[string]int64),
		subqueryOracleStats:             make(map[string]*subqueryOracleStats),
		genSQLFullJoinRejected:          make(map[string]int64),
	}
	r.observeJoinSignature(&generator.QueryFeatures{
		HasFullJoinEmulation:       true,
		FullJoinEmulationAttempted: true,
	}, "")
	if r.genSQLFullJoinAttempted != 1 {
		t.Fatalf("genSQLFullJoinAttempted=%d want=1", r.genSQLFullJoinAttempted)
	}
	if r.genSQLFullJoinEmulation != 1 {
		t.Fatalf("genSQLFullJoinEmulation=%d want=1", r.genSQLFullJoinEmulation)
	}
	if len(r.genSQLFullJoinRejected) != 0 {
		t.Fatalf("expected no reject reason for emitted full join, got %v", r.genSQLFullJoinRejected)
	}
}

func TestCountMapTotal(t *testing.T) {
	if got := countMapTotal(map[string]int64{"a": 2, "b": 3}); got != 5 {
		t.Fatalf("countMapTotal=%d want=5", got)
	}
}

func TestApplyResultMetricsDQPVariantCounters(t *testing.T) {
	r := &Runner{}
	r.applyResultMetrics(oracle.Result{
		Oracle: "DQP",
		Metrics: map[string]int64{
			"dqp_hint_injected_total":   9,
			"dqp_hint_fallback_total":   1,
			"dqp_set_var_variant_total": 5,
		},
	})
	r.applyResultMetrics(oracle.Result{
		Oracle: "DQP",
		Metrics: map[string]int64{
			"dqp_hint_injected_total":   3,
			"dqp_set_var_variant_total": 2,
		},
	})

	if r.dqpHintInjectedTotal != 12 {
		t.Fatalf("dqpHintInjectedTotal=%d want=12", r.dqpHintInjectedTotal)
	}
	if r.dqpHintFallbackTotal != 1 {
		t.Fatalf("dqpHintFallbackTotal=%d want=1", r.dqpHintFallbackTotal)
	}
	if r.dqpSetVarVariantTotal != 7 {
		t.Fatalf("dqpSetVarVariantTotal=%d want=7", r.dqpSetVarVariantTotal)
	}
}
