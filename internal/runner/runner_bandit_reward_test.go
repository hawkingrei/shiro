package runner

import (
	"errors"
	"testing"

	"shiro/internal/oracle"
)

func TestOracleBanditImmediateReward(t *testing.T) {
	if got := oracleBanditImmediateReward(oracle.Result{OK: true}, ""); got <= 0 {
		t.Fatalf("expected positive reward for effective run, got %v", got)
	}
	if got := oracleBanditImmediateReward(oracle.Result{OK: true}, "eet:no_transform"); got != 0 {
		t.Fatalf("expected zero reward for skipped run, got %v", got)
	}
	if got := oracleBanditImmediateReward(oracle.Result{OK: false}, ""); got != 1 {
		t.Fatalf("expected full reward for mismatch run, got %v", got)
	}
	if got := oracleBanditImmediateReward(oracle.Result{OK: true, Err: errors.New("panic: test")}, ""); got != 1 {
		t.Fatalf("expected full reward for panic run, got %v", got)
	}
}
