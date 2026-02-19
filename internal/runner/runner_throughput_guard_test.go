package runner

import (
	"context"
	"testing"
	"time"

	"shiro/internal/config"

	"github.com/go-sql-driver/mysql"
)

func TestUpdateThroughputControlsActivatesGuard(t *testing.T) {
	r := &Runner{oracleTimeoutCounts: make(map[string]int64)}

	state := r.updateThroughputControlsForInterval(120)
	if !state.LowSample || state.LowSampleStreak != 1 {
		t.Fatalf("unexpected first low-sample state: %+v", state)
	}
	if state.GuardActive || state.GuardActivated {
		t.Fatalf("guard should not activate on first low-sample interval")
	}

	state = r.updateThroughputControlsForInterval(100)
	if !state.GuardActive || !state.GuardActivated {
		t.Fatalf("guard should activate on low-sample streak: %+v", state)
	}
	if state.GuardTTL != throughputGuardTTLIntervals {
		t.Fatalf("unexpected guard ttl=%d", state.GuardTTL)
	}
	if !state.DQPCooldownActive {
		t.Fatalf("expected dqp cooldown to activate with guard")
	}
}

func TestObserveOracleTimeoutControlSetsDQPCooldown(t *testing.T) {
	r := &Runner{oracleTimeoutCounts: make(map[string]int64)}
	r.observeOracleTimeoutControl("DQP", context.DeadlineExceeded)

	if !r.isDQPTimeoutCooldownActive() {
		t.Fatalf("expected dqp timeout cooldown active")
	}
	r.statsMu.Lock()
	count := r.oracleTimeoutCounts["dqp:timeout"]
	r.statsMu.Unlock()
	if count != 1 {
		t.Fatalf("unexpected timeout count: %d", count)
	}
}

func TestOracleWeightByNameWithGuardAndCooldown(t *testing.T) {
	r := &Runner{
		cfg: config.Config{
			Weights: config.Weights{
				Oracles: config.OracleWeights{
					DQP:         10,
					GroundTruth: 8,
				},
			},
		},
		oracleTimeoutCounts: make(map[string]int64),
	}

	r.statsMu.Lock()
	r.throughputGuardTTL = 2
	r.statsMu.Unlock()
	if got := r.oracleWeightByName("DQP"); got != 1 {
		t.Fatalf("expected guarded dqp weight=1, got %d", got)
	}
	if got := r.oracleWeightByName("GroundTruth"); got != 2 {
		t.Fatalf("expected guarded groundtruth weight=2, got %d", got)
	}

	r.statsMu.Lock()
	r.dqpTimeoutCooldownTTL = 1
	r.statsMu.Unlock()
	if got := r.oracleWeightByName("DQP"); got != 0 {
		t.Fatalf("expected dqp cooldown to force weight 0, got %d", got)
	}
}

func TestOracleWeightByNameWithInfraUnhealthy(t *testing.T) {
	r := &Runner{
		cfg: config.Config{
			Weights: config.Weights{
				Oracles: config.OracleWeights{
					DQP:         10,
					GroundTruth: 8,
					TLP:         6,
					EET:         4,
				},
			},
		},
		oracleTimeoutCounts: make(map[string]int64),
		infraErrorCounts:    make(map[string]int64),
	}

	r.statsMu.Lock()
	r.infraUnhealthyTTL = 3
	r.statsMu.Unlock()

	if got := r.oracleWeightByName("DQP"); got != 0 {
		t.Fatalf("expected infra-unhealthy dqp weight=0, got %d", got)
	}
	if got := r.oracleWeightByName("GroundTruth"); got != 0 {
		t.Fatalf("expected infra-unhealthy groundtruth weight=0, got %d", got)
	}
	if got := r.oracleWeightByName("TLP"); got != 1 {
		t.Fatalf("expected infra-unhealthy tlp weight=1, got %d", got)
	}
	if got := r.oracleWeightByName("EET"); got != 2 {
		t.Fatalf("expected infra-unhealthy eet weight=2, got %d", got)
	}
}

func TestObserveInfraErrorControlSetsInfraUnhealthy(t *testing.T) {
	r := &Runner{infraErrorCounts: make(map[string]int64)}

	r.observeInfraErrorControl(context.DeadlineExceeded)
	if r.isInfraUnhealthyActive() {
		t.Fatalf("timeout should not mark infra unhealthy")
	}

	r.observeInfraErrorControl(context.Canceled)
	if r.isInfraUnhealthyActive() {
		t.Fatalf("canceled should not mark infra unhealthy")
	}

	r.observeInfraErrorControl(&mysql.MySQLError{
		Number:  9005,
		Message: "Region is unavailable",
	})
	if !r.isInfraUnhealthyActive() {
		t.Fatalf("expected infra unhealthy to activate")
	}
	r.statsMu.Lock()
	count := r.infraErrorCounts["region_unavailable"]
	r.statsMu.Unlock()
	if count != 1 {
		t.Fatalf("unexpected infra error count: %d", count)
	}
}

func TestWithTimeoutForOracleCapsDQP(t *testing.T) {
	r := &Runner{
		cfg: config.Config{
			StatementTimeoutMs: 15000,
		},
	}
	ctx := context.Background()
	dqpCtx, dqpCancel := r.withTimeoutForOracle(ctx, "DQP")
	defer dqpCancel()
	dqpDeadline, ok := dqpCtx.Deadline()
	if !ok {
		t.Fatalf("expected dqp context deadline")
	}
	dqpRemaining := time.Until(dqpDeadline)
	if dqpRemaining > 6*time.Second || dqpRemaining < 4*time.Second {
		t.Fatalf("unexpected dqp timeout window: %v", dqpRemaining)
	}

	baseCtx, baseCancel := r.withTimeoutForOracle(ctx, "NoREC")
	defer baseCancel()
	baseDeadline, ok := baseCtx.Deadline()
	if !ok {
		t.Fatalf("expected base context deadline")
	}
	baseRemaining := time.Until(baseDeadline)
	if baseRemaining > 16*time.Second || baseRemaining < 14*time.Second {
		t.Fatalf("unexpected base timeout window: %v", baseRemaining)
	}
}

func TestWithTimeoutForOracleCapsInfraUnhealthy(t *testing.T) {
	r := &Runner{
		cfg: config.Config{
			StatementTimeoutMs: 15000,
		},
	}
	r.statsMu.Lock()
	r.infraUnhealthyTTL = 2
	r.statsMu.Unlock()

	ctx := context.Background()
	baseCtx, baseCancel := r.withTimeoutForOracle(ctx, "NoREC")
	defer baseCancel()
	baseDeadline, ok := baseCtx.Deadline()
	if !ok {
		t.Fatalf("expected base context deadline")
	}
	baseRemaining := time.Until(baseDeadline)
	if baseRemaining > 4*time.Second || baseRemaining < 2*time.Second {
		t.Fatalf("unexpected infra-unhealthy timeout window: %v", baseRemaining)
	}

	dqpCtx, dqpCancel := r.withTimeoutForOracle(ctx, "DQP")
	defer dqpCancel()
	dqpDeadline, ok := dqpCtx.Deadline()
	if !ok {
		t.Fatalf("expected dqp context deadline")
	}
	dqpRemaining := time.Until(dqpDeadline)
	if dqpRemaining > 4*time.Second || dqpRemaining < 2*time.Second {
		t.Fatalf("unexpected infra-unhealthy dqp timeout window: %v", dqpRemaining)
	}
}
