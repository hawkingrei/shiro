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
