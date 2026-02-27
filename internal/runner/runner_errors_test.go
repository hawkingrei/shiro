package runner

import (
	"errors"
	"testing"

	"shiro/internal/oracle"

	"github.com/go-sql-driver/mysql"
)

func TestClassifyResultErrorPlanCacheMissingColumn(t *testing.T) {
	err := errors.New("Error 1105 (HY000): Can't find column Column#384 in schema")
	reason, hint := classifyResultError("PlanCache", err)
	if reason != "plancache:missing_column" {
		t.Fatalf("unexpected reason: %s", reason)
	}
	if hint != "tidb:schema_column_missing" {
		t.Fatalf("unexpected hint: %s", hint)
	}
}

func TestClassifyResultErrorUsesMySQLErrorCode(t *testing.T) {
	err := &mysql.MySQLError{
		Number:  1054,
		Message: "Unknown column 't1.k0' in 'on clause'",
	}
	reason, hint := classifyResultError("PlanCache", err)
	if reason != "plancache:sql_error_1054" {
		t.Fatalf("unexpected reason: %s", reason)
	}
	if hint != "" {
		t.Fatalf("unexpected hint: %s", hint)
	}
}

func TestClassifyResultErrorInfraUnhealthy(t *testing.T) {
	err := errors.New("Error 9005 (HY000): Region is unavailable")
	reason, hint := classifyResultError("TLP", err)
	if reason != "tlp:region_unavailable" {
		t.Fatalf("unexpected reason: %s", reason)
	}
	if hint != "tidb:infra_unhealthy" {
		t.Fatalf("unexpected hint: %s", hint)
	}
}

func TestClassifyResultErrorPlanRefMissingUsesCanonicalHint(t *testing.T) {
	err := errors.New("Cannot find the reference from its child")
	reason, hint := classifyResultError("EET", err)
	if reason != "eet:plan_ref_missing" {
		t.Fatalf("unexpected reason: %s", reason)
	}
	if hint != "tidb:plan_reference_missing" {
		t.Fatalf("unexpected hint: %s", hint)
	}
}

func TestClassifyResultErrorPQSRuntime1105(t *testing.T) {
	err := &mysql.MySQLError{
		Number:  mysqlErrCodeRuntimeGeneric,
		Message: "index out of range [0] with length 0",
	}
	reason, hint := classifyResultError("PQS", err)
	if reason != "pqs:runtime_1105" {
		t.Fatalf("unexpected reason: %s", reason)
	}
	if hint != "tidb:runtime_error" {
		t.Fatalf("unexpected hint: %s", hint)
	}
}

func TestClassifyResultErrorPQSRuntime1105KeepsGenericForOtherOracles(t *testing.T) {
	err := &mysql.MySQLError{
		Number:  mysqlErrCodeRuntimeGeneric,
		Message: "index out of range [0] with length 0",
	}
	reason, hint := classifyResultError("DQP", err)
	if reason != "dqp:sql_error_1105" {
		t.Fatalf("unexpected reason: %s", reason)
	}
	if hint != "" {
		t.Fatalf("unexpected hint: %s", hint)
	}
}

func TestDowngradeMissingColumnFalsePositive(t *testing.T) {
	result := oracle.Result{
		Oracle: "EET",
		Err:    errors.New("Error 1105 (HY000): Can't find column Column#884 in schema"),
	}
	changed := downgradeMissingColumnFalsePositive(&result)
	if !changed {
		t.Fatalf("expected missing-column downgrade to apply")
	}
	if !result.OK {
		t.Fatalf("expected downgraded result to be OK")
	}
	if result.Err == nil {
		t.Fatalf("expected downgraded result err to be preserved for minimize replay")
	}
	skip, _ := result.Details["skip_reason"].(string)
	if skip != "eet:missing_column" {
		t.Fatalf("unexpected skip_reason: %s", skip)
	}
	skipErr, _ := result.Details["skip_error_reason"].(string)
	if skipErr != "eet:missing_column" {
		t.Fatalf("unexpected skip_error_reason: %s", skipErr)
	}
	allowMinimize, _ := result.Details[skipAllowMinimizeDetailKey].(bool)
	if !allowMinimize {
		t.Fatalf("expected skip_allow_minimize marker")
	}
	if !shouldCaptureSkipForMinimize(result) {
		t.Fatalf("expected downgraded missing-column result to be captured for minimize")
	}
}

func TestDowngradeMissingColumnFalsePositiveSkipsPlanCache(t *testing.T) {
	result := oracle.Result{
		Oracle: "PlanCache",
		Err:    errors.New("Error 1105 (HY000): Can't find column Column#884 in schema"),
	}
	changed := downgradeMissingColumnFalsePositive(&result)
	if changed {
		t.Fatalf("expected plan cache missing-column to stay reportable")
	}
	if result.Err == nil {
		t.Fatalf("expected original error to be preserved")
	}
}

func TestAnnotateResultForReportingKeepsSkipClassificationWhenMinimizeCaptureEnabled(t *testing.T) {
	result := oracle.Result{
		Oracle: "EET",
		OK:     true,
		Err:    errors.New("Error 1105 (HY000): Can't find column Column#884 in schema"),
		Details: map[string]any{
			"skip_reason":              "eet:missing_column",
			skipAllowMinimizeDetailKey: true,
		},
	}
	annotateResultForReporting(&result)
	if !result.OK {
		t.Fatalf("expected skip-classified result to stay OK")
	}
	if _, ok := result.Details["error_reason"]; ok {
		t.Fatalf("unexpected error_reason for skip-classified result")
	}
	if _, ok := result.Details["bug_hint"]; ok {
		t.Fatalf("unexpected bug_hint for skip-classified result")
	}
}

func TestDowngradeGroundTruthLowConfidenceFalsePositive(t *testing.T) {
	result := oracle.Result{
		Oracle:   "GroundTruth",
		OK:       false,
		Expected: "truth count=5000",
		Actual:   "db count=50",
		Truth: &oracle.GroundTruthMetrics{
			Enabled:  true,
			Mismatch: true,
		},
		Details: map[string]any{
			"groundtruth_confidence": "fallback_dsg",
		},
	}
	changed := downgradeGroundTruthLowConfidenceFalsePositive(&result)
	if !changed {
		t.Fatalf("expected low-confidence GroundTruth mismatch to downgrade")
	}
	if !result.OK {
		t.Fatalf("expected downgraded result to be OK")
	}
	skip, _ := result.Details["skip_reason"].(string)
	if skip != "groundtruth:low_confidence_fallback" {
		t.Fatalf("unexpected skip_reason: %s", skip)
	}
	skipErr, _ := result.Details["skip_error_reason"].(string)
	if skipErr != "groundtruth:count_mismatch" {
		t.Fatalf("unexpected skip_error_reason: %s", skipErr)
	}
}

func TestDowngradeGroundTruthLowConfidenceFalsePositiveKeepsStrict(t *testing.T) {
	result := oracle.Result{
		Oracle: "GroundTruth",
		Truth: &oracle.GroundTruthMetrics{
			Enabled:  true,
			Mismatch: true,
		},
		Details: map[string]any{
			"groundtruth_confidence": "strict_dsg",
		},
	}
	changed := downgradeGroundTruthLowConfidenceFalsePositive(&result)
	if changed {
		t.Fatalf("expected strict DSG mismatch to remain reportable")
	}
}

func TestDowngradeDQPTimeoutFalsePositive(t *testing.T) {
	result := oracle.Result{
		Oracle: "DQP",
		Err:    errors.New("context deadline exceeded"),
	}
	changed := downgradeDQPTimeoutFalsePositive(&result)
	if !changed {
		t.Fatalf("expected DQP timeout downgrade to apply")
	}
	if !result.OK {
		t.Fatalf("expected downgraded DQP timeout to be OK")
	}
	if result.Err != nil {
		t.Fatalf("expected downgraded DQP timeout err to be cleared")
	}
	skip, _ := result.Details["skip_reason"].(string)
	if skip != "dqp:timeout" {
		t.Fatalf("unexpected skip_reason: %s", skip)
	}
}

func TestDowngradeDQPTimeoutFalsePositiveByMySQLErrorCode(t *testing.T) {
	result := oracle.Result{
		Oracle: "DQP",
		Err: &mysql.MySQLError{
			Number:  3024,
			Message: "Query execution was interrupted, maximum statement execution time exceeded",
		},
	}
	changed := downgradeDQPTimeoutFalsePositive(&result)
	if !changed {
		t.Fatalf("expected DQP timeout downgrade to apply for MySQL timeout code")
	}
}

func TestDowngradeDQPTimeoutFalsePositiveKeepsOtherOracles(t *testing.T) {
	result := oracle.Result{
		Oracle: "NoREC",
		Err:    errors.New("context deadline exceeded"),
	}
	changed := downgradeDQPTimeoutFalsePositive(&result)
	if changed {
		t.Fatalf("expected non-DQP timeout to remain reportable")
	}
}

func TestAnnotateResultForReportingGroundTruthMismatch(t *testing.T) {
	result := oracle.Result{
		Oracle: "GroundTruth",
		Truth: &oracle.GroundTruthMetrics{
			Enabled:  true,
			Mismatch: true,
		},
	}
	annotateResultForReporting(&result)
	reason, _ := result.Details["error_reason"].(string)
	if reason != "groundtruth:count_mismatch" {
		t.Fatalf("unexpected mismatch reason: %s", reason)
	}
	hint, _ := result.Details["bug_hint"].(string)
	if hint != "tidb:result_inconsistency" {
		t.Fatalf("unexpected mismatch hint: %s", hint)
	}
}

func TestAnnotateResultForReportingSkipReasonTakesPrecedence(t *testing.T) {
	result := oracle.Result{
		Oracle: "GroundTruth",
		Truth: &oracle.GroundTruthMetrics{
			Enabled:  true,
			Mismatch: true,
		},
		Details: map[string]any{
			"skip_reason": "groundtruth:low_confidence_fallback",
		},
	}
	annotateResultForReporting(&result)
	if _, ok := result.Details["error_reason"]; ok {
		t.Fatalf("unexpected error_reason for skipped result")
	}
}

func TestAnnotateResultForReportingKeepsExistingReason(t *testing.T) {
	result := oracle.Result{
		Oracle: "PlanCache",
		Err:    errors.New("runtime error: mock"),
		Details: map[string]any{
			"error_reason": "custom_reason",
		},
	}
	annotateResultForReporting(&result)
	reason, _ := result.Details["error_reason"].(string)
	if reason != "custom_reason" {
		t.Fatalf("existing reason should be preserved, got %s", reason)
	}
}

func TestAnnotateResultForReportingPQSRuntime1105OverridesGenericReason(t *testing.T) {
	result := oracle.Result{
		Oracle: "PQS",
		Err: &mysql.MySQLError{
			Number:  mysqlErrCodeRuntimeGeneric,
			Message: "runtime error: index out of range [2] with length 2",
		},
		Details: map[string]any{
			"error_reason": "pqs:sql_error_1105",
		},
	}
	annotateResultForReporting(&result)
	reason, _ := result.Details["error_reason"].(string)
	if reason != "pqs:runtime_1105" {
		t.Fatalf("expected pqs runtime reason override, got %s", reason)
	}
	hint, _ := result.Details["bug_hint"].(string)
	if hint != "tidb:runtime_error" {
		t.Fatalf("expected runtime bug_hint, got %s", hint)
	}
	if result.OK {
		t.Fatalf("expected error result to remain non-OK")
	}
}

func TestAnnotateResultForReportingPQSRuntime1105OverridesHint(t *testing.T) {
	result := oracle.Result{
		Oracle: "PQS",
		Err: &mysql.MySQLError{
			Number:  mysqlErrCodeRuntimeGeneric,
			Message: "runtime error: index out of range [0] with length 0",
		},
		Details: map[string]any{
			"error_reason": "pqs:sql_error_1105",
			"bug_hint":     "custom_hint",
		},
	}
	annotateResultForReporting(&result)
	reason, _ := result.Details["error_reason"].(string)
	if reason != "pqs:runtime_1105" {
		t.Fatalf("expected pqs runtime reason override, got %s", reason)
	}
	hint, _ := result.Details["bug_hint"].(string)
	if hint != "tidb:runtime_error" {
		t.Fatalf("expected runtime bug_hint override, got %s", hint)
	}
}

func TestAnnotateResultForReportingCanonicalReasonOverridesGenericReason(t *testing.T) {
	result := oracle.Result{
		Oracle: "PlanCache",
		Err:    errors.New("Error 1105 (HY000): Can't find column Column#123 in schema"),
		Details: map[string]any{
			"error_reason": "plancache:sql_error_1105",
			"bug_hint":     "custom_hint",
		},
	}
	annotateResultForReporting(&result)
	reason, _ := result.Details["error_reason"].(string)
	if reason != "plancache:missing_column" {
		t.Fatalf("expected canonical reason override, got %s", reason)
	}
	hint, _ := result.Details["bug_hint"].(string)
	if hint != "tidb:schema_column_missing" {
		t.Fatalf("expected canonical bug_hint override, got %s", hint)
	}
}

func TestAnnotateResultForReportingKeepsNonGenericReasonWhenCanonicalIsSpecific(t *testing.T) {
	result := oracle.Result{
		Oracle: "PlanCache",
		Err:    errors.New("Error 1105 (HY000): Can't find column Column#456 in schema"),
		Details: map[string]any{
			"error_reason": "plancache:base_signature_error",
			"bug_hint":     "custom_hint",
		},
	}
	annotateResultForReporting(&result)
	reason, _ := result.Details["error_reason"].(string)
	if reason != "plancache:base_signature_error" {
		t.Fatalf("non-generic reason should be preserved, got %s", reason)
	}
	hint, _ := result.Details["bug_hint"].(string)
	if hint != "custom_hint" {
		t.Fatalf("non-generic bug_hint should be preserved, got %s", hint)
	}
}

func TestAnnotateResultForReportingKeepsGenericReasonWhenCanonicalIsGeneric(t *testing.T) {
	result := oracle.Result{
		Oracle: "PlanCache",
		Err: &mysql.MySQLError{
			Number:  1054,
			Message: "Unknown column 't.k' in 'field list'",
		},
		Details: map[string]any{
			"error_reason": "plancache:sql_error_1105",
		},
	}
	annotateResultForReporting(&result)
	reason, _ := result.Details["error_reason"].(string)
	if reason != "plancache:sql_error_1105" {
		t.Fatalf("generic reason should be preserved when canonical is generic, got %s", reason)
	}
}
