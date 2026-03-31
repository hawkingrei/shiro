package runner

import (
	"errors"
	"reflect"
	"testing"

	"shiro/internal/db"
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

func TestNormalizeErrorSignature(t *testing.T) {
	cases := map[string]string{
		"":   "",
		"  ": "",
		"Impo:Missing_Column|Cant_Find_Column_In_Schema":      "impo:missing_column|cant_find_column_in_schema",
		"  EET:missing_column | cant_find_column_in_schema  ": "eet:missing_column|cant_find_column_in_schema",
	}
	for input, expect := range cases {
		if got := normalizeErrorSignature(input); got != expect {
			t.Fatalf("normalizeErrorSignature(%q)=%q want=%q", input, got, expect)
		}
	}
}

func TestNormalizeReplayFailureStage(t *testing.T) {
	cases := map[string]string{
		"":               "",
		"  ":             "",
		"apply_schema":   "apply_schema",
		"apply-schema":   "apply_schema",
		" exec case sql": "exec_case_sql",
	}
	for input, expect := range cases {
		if got := normalizeReplayFailureStage(input); got != expect {
			t.Fatalf("normalizeReplayFailureStage(%q)=%q want=%q", input, got, expect)
		}
	}
}

func TestObserveOracleResultSeparatesSkipErrors(t *testing.T) {
	r := &Runner{
		oracleStats: make(map[string]*oracleFunnel),
	}
	r.observeOracleResult("EET", oracle.Result{
		Oracle: "EET",
		OK:     true,
		Err:    errors.New("Error 1105 (HY000): Can't find column c1 in schema"),
		Details: map[string]any{
			"skip_error_reason": "eet:missing_column",
		},
	}, "eet:missing_column", false, false)
	r.observeOracleResult("EET", oracle.Result{
		Oracle: "EET",
		Err:    errors.New("Error 1105 (HY000): planner bug"),
		Details: map[string]any{
			"error_reason": "eet:planner_bug",
		},
	}, "", false, false)

	stat := r.oracleStats["EET"]
	if stat == nil {
		t.Fatalf("expected oracle stats for EET")
	}
	if stat.Skips != 1 {
		t.Fatalf("Skips=%d want=1", stat.Skips)
	}
	if stat.Errors != 1 {
		t.Fatalf("Errors=%d want=1", stat.Errors)
	}
	if stat.SkipErrors != 1 {
		t.Fatalf("SkipErrors=%d want=1", stat.SkipErrors)
	}
	if stat.ErrorReasons["eet:planner_bug"] != 1 {
		t.Fatalf("ErrorReasons[eet:planner_bug]=%d want=1", stat.ErrorReasons["eet:planner_bug"])
	}
	if stat.SkipErrorReasons["eet:missing_column"] != 1 {
		t.Fatalf("SkipErrorReasons[eet:missing_column]=%d want=1", stat.SkipErrorReasons["eet:missing_column"])
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
		capturedMinimizeStatus:        make(map[string]int64),
		capturedMinimizeReasons:       make(map[string]int64),
		capturedErrorSignatures:       make(map[string]int64),
		capturedReplayFailureStages:   make(map[string]int64),
		capturedReplaySetupSignatures: make(map[string]int64),
	}
	r.observeReproducibilitySummary("in-progress", "", "  Impo:Missing_Column|Cant_Find_Column_In_Schema  ", nil)
	r.observeReproducibilitySummary(
		"skipped",
		"base_replay_not_reproducible",
		"impo:missing_column|cant_find_column_in_schema",
		map[string]any{
			"minimize_base_replay_failure_stage":       "apply-schema",
			"minimize_base_replay_outcome":             "setup_error",
			"minimize_base_replay_actual_error_reason": "eet:sql_error_1824",
		},
	)
	r.observeReproducibilitySummary(
		"skipped",
		"base_replay_not_reproducible",
		"",
		map[string]any{
			"minimize_base_replay_failure_stage":          "exec_case_sql",
			"minimize_base_replay_outcome":                "error_mismatch",
			"minimize_base_replay_actual_error_signature": "replay:no_error",
		},
	)
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
	if r.capturedErrorSignatures["impo:missing_column|cant_find_column_in_schema"] != 2 {
		t.Fatalf("impo signature=%d want=2", r.capturedErrorSignatures["impo:missing_column|cant_find_column_in_schema"])
	}
	if len(r.capturedErrorSignatures) != 1 {
		t.Fatalf("capturedErrorSignatures=%v want single normalized signature", r.capturedErrorSignatures)
	}
	if r.capturedReplayFailureStages["apply_schema"] != 1 {
		t.Fatalf("apply_schema=%d want=1", r.capturedReplayFailureStages["apply_schema"])
	}
	if r.capturedReplayFailureStages["exec_case_sql"] != 1 {
		t.Fatalf("exec_case_sql=%d want=1", r.capturedReplayFailureStages["exec_case_sql"])
	}
	if r.capturedReplaySetupSignatures["eet:sql_error_1824"] != 1 {
		t.Fatalf("setup signature=%d want=1", r.capturedReplaySetupSignatures["eet:sql_error_1824"])
	}
	if len(r.capturedReplaySetupSignatures) != 1 {
		t.Fatalf("capturedReplaySetupSignatures=%v want single setup signature", r.capturedReplaySetupSignatures)
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

func TestObserveJoinSignatureScalarSubqueryDisallowReason(t *testing.T) {
	r := &Runner{
		templateJoinPredicateStrategies: make(map[string]int64),
		subqueryDisallowReasons:         make(map[string]int64),
		scalarSubqueryDisallowReasons:   make(map[string]int64),
		subqueryOracleStats:             make(map[string]*subqueryOracleStats),
	}
	r.observeJoinSignature(&generator.QueryFeatures{
		SubqueryAllowed:              true,
		ScalarSubqueryAllowed:        false,
		ScalarSubqueryDisallowReason: "scalar_subquery_off",
	}, "PQS")
	if r.subqueryAllowed != 1 {
		t.Fatalf("subqueryAllowed=%d want=1", r.subqueryAllowed)
	}
	if r.scalarSubqueryDisallowed != 1 {
		t.Fatalf("scalarSubqueryDisallowed=%d want=1", r.scalarSubqueryDisallowed)
	}
	if r.scalarSubqueryDisallowReasons["scalar_subquery_off"] != 1 {
		t.Fatalf("scalar_subquery_off=%d want=1", r.scalarSubqueryDisallowReasons["scalar_subquery_off"])
	}
}

func TestObserveSQLUsesPrecomputedFeatures(t *testing.T) {
	r := &Runner{}
	r.observeSQL("SELECT * FROM t0 WHERE EXISTS (SELECT 1)", nil, &db.SQLSubqueryFeatures{
		HasExistsSubquery: true,
		HasInSubquery:     true,
	})
	if r.sqlTotal != 1 {
		t.Fatalf("sqlTotal=%d want=1", r.sqlTotal)
	}
	if r.sqlValid != 1 {
		t.Fatalf("sqlValid=%d want=1", r.sqlValid)
	}
	if r.sqlExists != 1 {
		t.Fatalf("sqlExists=%d want=1", r.sqlExists)
	}
	if r.sqlIn != 1 {
		t.Fatalf("sqlIn=%d want=1", r.sqlIn)
	}
	if r.sqlInSubquery != 1 {
		t.Fatalf("sqlInSubquery=%d want=1", r.sqlInSubquery)
	}
	if r.sqlParseCalls != 0 {
		t.Fatalf("sqlParseCalls=%d want=0", r.sqlParseCalls)
	}
}

func TestObserveVariantSubqueryCountsUsesPrecomputedFeatures(t *testing.T) {
	r := &Runner{}
	sqlText := "SELECT * FROM t0 WHERE id IN (SELECT id FROM t1)"
	r.observeVariantSubqueryCounts([]string{sqlText}, map[string]db.SQLSubqueryFeatures{
		sqlText: {HasInSubquery: true},
	})
	if r.sqlInSubqueryVariant != 1 {
		t.Fatalf("sqlInSubqueryVariant=%d want=1", r.sqlInSubqueryVariant)
	}
	if r.sqlNotInSubqueryVariant != 0 {
		t.Fatalf("sqlNotInSubqueryVariant=%d want=0", r.sqlNotInSubqueryVariant)
	}
	if r.sqlParseVariantCalls != 0 {
		t.Fatalf("sqlParseVariantCalls=%d want=0", r.sqlParseVariantCalls)
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
			"dqp_hint_length_min":       10,
			"dqp_hint_length_max":       30,
			"dqp_hint_length_sum":       70,
			"dqp_hint_length_count":     2,
		},
	})
	r.applyResultMetrics(oracle.Result{
		Oracle: "DQP",
		Metrics: map[string]int64{
			"dqp_hint_injected_total":   3,
			"dqp_set_var_variant_total": 2,
			"dqp_hint_length_min":       8,
			"dqp_hint_length_max":       20,
			"dqp_hint_length_sum":       28,
			"dqp_hint_length_count":     2,
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
	if r.dqpHintLengthSumTotal != 98 {
		t.Fatalf("dqpHintLengthSumTotal=%d want=98", r.dqpHintLengthSumTotal)
	}
	if r.dqpHintLengthCountTotal != 4 {
		t.Fatalf("dqpHintLengthCountTotal=%d want=4", r.dqpHintLengthCountTotal)
	}
	if r.dqpHintLengthMinTotal != 8 {
		t.Fatalf("dqpHintLengthMinTotal=%d want=8", r.dqpHintLengthMinTotal)
	}
	if r.dqpHintLengthMaxTotal != 30 {
		t.Fatalf("dqpHintLengthMaxTotal=%d want=30", r.dqpHintLengthMaxTotal)
	}
	if r.dqpHintLengthIntervalSum != 98 {
		t.Fatalf("dqpHintLengthIntervalSum=%d want=98", r.dqpHintLengthIntervalSum)
	}
	if r.dqpHintLengthIntervalCount != 4 {
		t.Fatalf("dqpHintLengthIntervalCount=%d want=4", r.dqpHintLengthIntervalCount)
	}
	if r.dqpHintLengthIntervalMin != 8 {
		t.Fatalf("dqpHintLengthIntervalMin=%d want=8", r.dqpHintLengthIntervalMin)
	}
	if r.dqpHintLengthIntervalMax != 30 {
		t.Fatalf("dqpHintLengthIntervalMax=%d want=30", r.dqpHintLengthIntervalMax)
	}
}
