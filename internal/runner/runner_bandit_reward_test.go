package runner

import (
	"context"
	"errors"
	"math"
	"testing"

	"shiro/internal/oracle"
)

func TestOracleBanditImmediateReward(t *testing.T) {
	if got := oracleBanditImmediateReward(oracle.Result{OK: true}, ""); got <= 0 {
		t.Fatalf("expected positive reward for effective run, got %v", got)
	}
	if got := oracleBanditImmediateReward(oracle.Result{OK: true}, "eet:no_transform"); got <= 0 {
		t.Fatalf("expected small positive reward for non-timeout skip, got %v", got)
	}
	if got := oracleBanditImmediateReward(oracle.Result{OK: true}, "dqp:timeout"); got != 0 {
		t.Fatalf("expected zero reward for timeout skip, got %v", got)
	}
	if got := oracleBanditImmediateReward(oracle.Result{OK: false}, ""); got != 1 {
		t.Fatalf("expected full reward for stable wrong-result mismatch, got %v", got)
	}
	if got := oracleBanditImmediateReward(oracle.Result{
		OK: false,
		Details: map[string]any{
			"expected_explain": "Projection\n└─TableFullScan",
			"actual_explain":   "Projection\n└─TableFullScan",
		},
	}, ""); got != 0.25 {
		t.Fatalf("expected reduced reward for explain-same mismatch, got %v", got)
	}
	if got := oracleBanditImmediateReward(oracle.Result{OK: false, Err: errors.New("Error 1105 (HY000): missing column")}, ""); got != 0.15 {
		t.Fatalf("expected reduced reward for execution error bug, got %v", got)
	}
	if got := oracleBanditImmediateReward(
		oracle.Result{OK: true, Err: errors.New("Error 1105 (HY000): missing column")},
		"eet:missing_column",
	); got != 0.05 {
		t.Fatalf("expected skip reward for downgraded missing-column false positive, got %v", got)
	}
	if got := oracleBanditImmediateReward(
		oracle.Result{OK: true, Err: context.DeadlineExceeded},
		"dqp:timeout",
	); got != 0 {
		t.Fatalf("expected zero reward for downgraded timeout skip, got %v", got)
	}
	if got := oracleBanditImmediateReward(oracle.Result{OK: false, Err: context.DeadlineExceeded}, ""); got != 0 {
		t.Fatalf("expected zero reward for timeout bug case, got %v", got)
	}
	if got := oracleBanditImmediateReward(oracle.Result{OK: true, Err: errors.New("panic: test")}, ""); got != 0.4 {
		t.Fatalf("expected reduced reward for panic run, got %v", got)
	}
}

func TestOracleBanditFunnelReward(t *testing.T) {
	stat := oracleFunnel{
		Runs:        10,
		Mismatches:  3,
		ExplainSame: 1,
		Errors:      2,
		Panics:      1,
		Skips:       2,
	}
	got := oracleBanditFunnelReward(stat)
	want := (2.0 + 0.25 + 0.3 + 0.4 - 0.04) / 10.0
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("oracleBanditFunnelReward()=%v want=%v", got, want)
	}
}
