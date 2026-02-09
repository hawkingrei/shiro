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
		t.Fatalf("Flaky=true want=false")
	}
	if _, ok := details["flaky_reason"]; ok {
		t.Fatalf("unexpected flaky_reason detail")
	}
}
