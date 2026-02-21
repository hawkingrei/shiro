package runner

import (
	"testing"

	"shiro/internal/report"

	"github.com/go-sql-driver/mysql"
)

func TestGroundTruthDSGMismatchReasonFromDetails(t *testing.T) {
	cases := []struct {
		name    string
		details map[string]any
		want    string
	}{
		{
			name: "direct detail",
			details: map[string]any{
				"groundtruth_dsg_mismatch_reason": "right_key",
			},
			want: "right_key",
		},
		{
			name: "skip reason underscore",
			details: map[string]any{
				"skip_reason": "groundtruth:dsg_key_mismatch_left_table",
			},
			want: "left_table",
		},
		{
			name: "skip reason colon",
			details: map[string]any{
				"skip_reason": "groundtruth:dsg_key_mismatch:right_key",
			},
			want: "right_key",
		},
		{
			name: "skip reason unknown",
			details: map[string]any{
				"skip_reason": "groundtruth:dsg_key_mismatch",
			},
			want: "unknown",
		},
		{
			name: "non dsg skip reason",
			details: map[string]any{
				"skip_reason": "groundtruth:key_missing",
			},
			want: "",
		},
		{
			name:    "nil details",
			details: nil,
			want:    "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := groundTruthDSGMismatchReasonFromDetails(tc.details)
			if got != tc.want {
				t.Fatalf("groundTruthDSGMismatchReasonFromDetails()=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestApplyMinimizeOutcomeFlakyBaseReplay(t *testing.T) {
	summary := report.Summary{
		MinimizeStatus: "in_progress",
		Flaky:          false,
	}
	details := map[string]any{}
	applyMinimizeOutcome(&summary, details, minimizeOutput{
		status: "skipped",
		reason: minimizeReasonBaseReplayNotReproducible,
		flaky:  true,
	}, nil)
	if summary.MinimizeStatus != "skipped" {
		t.Fatalf("MinimizeStatus=%q want=skipped", summary.MinimizeStatus)
	}
	if !summary.Flaky {
		t.Fatalf("Flaky=false want=true")
	}
	if got := details["minimize_reason"]; got != minimizeReasonBaseReplayNotReproducible {
		t.Fatalf("minimize_reason=%v want=%q", got, minimizeReasonBaseReplayNotReproducible)
	}
	if got := details["flaky_reason"]; got != minimizeReasonBaseReplayNotReproducible {
		t.Fatalf("flaky_reason=%v want=%q", got, minimizeReasonBaseReplayNotReproducible)
	}
}

func TestApplyMinimizeOutcomeSuccess(t *testing.T) {
	summary := report.Summary{
		MinimizeStatus: "in_progress",
	}
	details := map[string]any{}
	applyMinimizeOutcome(&summary, details, minimizeOutput{
		minimized: true,
		status:    "success",
	}, nil)
	if summary.MinimizeStatus != "success" {
		t.Fatalf("MinimizeStatus=%q want=success", summary.MinimizeStatus)
	}
	if summary.Flaky {
		t.Fatalf("Flaky=true want=false")
	}
	if _, ok := details["minimize_reason"]; ok {
		t.Fatalf("unexpected minimize_reason detail")
	}
}

func TestApplyMinimizeOutcomeFlakyErrno(t *testing.T) {
	summary := report.Summary{
		MinimizeStatus: "in_progress",
		Flaky:          false,
	}
	details := map[string]any{}
	err := &mysql.MySQLError{
		Number:  1064,
		Message: "syntax error",
	}
	applyMinimizeOutcome(&summary, details, minimizeOutput{
		status: "skipped",
		reason: minimizeReasonBaseReplayNotReproducible,
		flaky:  true,
	}, err)
	if summary.Flaky {
		t.Errorf("Flaky=true want=false")
	}
	if _, ok := details["flaky_reason"]; ok {
		t.Errorf("unexpected flaky_reason detail")
	}
}

func TestApplyRuntime1105ReproMetaAnnotatesUnreproducible(t *testing.T) {
	summary := report.Summary{
		MinimizeStatus: "skipped",
		BugHint:        "tidb:runtime_error",
	}
	details := map[string]any{
		"error_reason":    "pqs:runtime_1105",
		"bug_hint":        "tidb:runtime_error",
		"minimize_reason": minimizeReasonBaseReplayNotReproducible,
	}
	applyRuntime1105ReproMeta(&summary, details)
	if summary.BugHint != "tidb:runtime_error" {
		t.Fatalf("BugHint=%q want tidb:runtime_error", summary.BugHint)
	}
	if hint, _ := details["bug_hint"].(string); hint != "tidb:runtime_error" {
		t.Fatalf("bug_hint=%q want tidb:runtime_error", hint)
	}
	gated, _ := details["runtime_bug_hint_gated"].(bool)
	if !gated {
		t.Fatalf("runtime_bug_hint_gated=false want=true")
	}
	reproducible, _ := details["runtime_bug_reproducible"].(bool)
	if reproducible {
		t.Fatalf("runtime_bug_reproducible=true want=false")
	}
}

func TestApplyRuntime1105ReproMetaKeepsSuccessful(t *testing.T) {
	summary := report.Summary{
		MinimizeStatus: "success",
		BugHint:        "tidb:runtime_error",
	}
	details := map[string]any{
		"error_reason": "pqs:runtime_1105",
		"bug_hint":     "tidb:runtime_error",
	}
	applyRuntime1105ReproMeta(&summary, details)
	if summary.BugHint != "tidb:runtime_error" {
		t.Fatalf("BugHint=%q want tidb:runtime_error", summary.BugHint)
	}
	if _, ok := details["runtime_bug_hint_gated"]; ok {
		t.Fatalf("unexpected runtime_bug_hint_gated marker for successful repro")
	}
	reproducible, _ := details["runtime_bug_reproducible"].(bool)
	if !reproducible {
		t.Fatalf("runtime_bug_reproducible=false want=true")
	}
}

func TestApplyRuntime1105ReproMetaSkipsNonRuntime1105(t *testing.T) {
	summary := report.Summary{
		MinimizeStatus: "skipped",
		BugHint:        "tidb:schema_column_missing",
	}
	details := map[string]any{
		"error_reason": "plancache:missing_column",
		"bug_hint":     "tidb:schema_column_missing",
	}
	applyRuntime1105ReproMeta(&summary, details)
	if summary.BugHint != "tidb:schema_column_missing" {
		t.Fatalf("BugHint=%q want unchanged", summary.BugHint)
	}
	if _, ok := details["runtime_bug_hint_gated"]; ok {
		t.Fatalf("unexpected runtime_bug_hint_gated marker")
	}
	if _, ok := details["runtime_bug_reproducible"]; ok {
		t.Fatalf("unexpected runtime_bug_reproducible marker")
	}
}

func TestApplyRuntime1105ReproMetaAnnotatesDisabled(t *testing.T) {
	summary := report.Summary{
		MinimizeStatus: "disabled",
		BugHint:        "tidb:runtime_error",
	}
	details := map[string]any{
		"error_reason": "pqs:runtime_1105",
		"bug_hint":     "tidb:runtime_error",
	}
	applyRuntime1105ReproMeta(&summary, details)
	if summary.BugHint != "tidb:runtime_error" {
		t.Fatalf("BugHint=%q want tidb:runtime_error", summary.BugHint)
	}
	if hint, _ := details["bug_hint"].(string); hint != "tidb:runtime_error" {
		t.Fatalf("bug_hint=%q want tidb:runtime_error", hint)
	}
	gated, _ := details["runtime_bug_hint_gated"].(bool)
	if !gated {
		t.Fatalf("runtime_bug_hint_gated=false want=true")
	}
	reproducible, _ := details["runtime_bug_reproducible"].(bool)
	if reproducible {
		t.Fatalf("runtime_bug_reproducible=true want=false")
	}
	reason, _ := details["runtime_bug_hint_gate_reason"].(string)
	if reason != "requires_repro" {
		t.Fatalf("runtime_bug_hint_gate_reason=%q want=requires_repro", reason)
	}
}

func TestApplyRuntime1105ReproMetaAnnotatesNotApplicableKeepsGateReason(t *testing.T) {
	summary := report.Summary{
		MinimizeStatus: "not_applicable",
		BugHint:        "tidb:runtime_error",
	}
	details := map[string]any{
		"error_reason":                  "pqs:runtime_1105",
		"bug_hint":                      "tidb:runtime_error",
		"runtime_bug_hint_gate_reason": "manual_triage",
	}
	applyRuntime1105ReproMeta(&summary, details)
	if summary.BugHint != "tidb:runtime_error" {
		t.Fatalf("BugHint=%q want tidb:runtime_error", summary.BugHint)
	}
	if hint, _ := details["bug_hint"].(string); hint != "tidb:runtime_error" {
		t.Fatalf("bug_hint=%q want tidb:runtime_error", hint)
	}
	gated, _ := details["runtime_bug_hint_gated"].(bool)
	if !gated {
		t.Fatalf("runtime_bug_hint_gated=false want=true")
	}
	reproducible, _ := details["runtime_bug_reproducible"].(bool)
	if reproducible {
		t.Fatalf("runtime_bug_reproducible=true want=false")
	}
	reason, _ := details["runtime_bug_hint_gate_reason"].(string)
	if reason != "manual_triage" {
		t.Fatalf("runtime_bug_hint_gate_reason=%q want=manual_triage", reason)
	}
}
