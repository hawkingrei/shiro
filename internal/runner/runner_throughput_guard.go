package runner

import "strings"

const minSQLTotalPerInterval int64 = 200
const lowSampleStreakTrigger int64 = 2
const throughputGuardTTLIntervals int64 = 3
const dqpTimeoutCooldownTTLIntervals int64 = 2
const dqpOracleTimeoutCapMs = 5000
const infraUnhealthyTTLIntervals int64 = 6
const infraOracleTimeoutCapMs = 3000

type throughputControlState struct {
	LowSample           bool
	LowSampleStreak     int64
	GuardTTL            int64
	GuardActive         bool
	GuardActivated      bool
	GuardRecovered      bool
	GuardActivations    int64
	DQPCooldownTTL      int64
	DQPCooldownActive   bool
	InfraUnhealthyTTL   int64
	InfraUnhealthy      bool
	OracleTimeoutCounts map[string]int64
	InfraErrorCounts    map[string]int64
}

func (r *Runner) isThroughputGuardActive() bool {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	return r.throughputGuardTTL > 0
}

func (r *Runner) isDQPTimeoutCooldownActive() bool {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	return r.dqpTimeoutCooldownTTL > 0
}

func (r *Runner) isInfraUnhealthyActive() bool {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	return r.infraUnhealthyTTL > 0
}

func (r *Runner) observeOracleTimeoutControl(oracleName string, err error) {
	if strings.TrimSpace(oracleName) == "" || err == nil || !isTimeoutError(err) {
		return
	}
	timeoutReason := strings.ToLower(strings.TrimSpace(oracleName)) + ":timeout"
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	if r.oracleTimeoutCounts == nil {
		r.oracleTimeoutCounts = make(map[string]int64)
	}
	r.oracleTimeoutCounts[timeoutReason]++
	if strings.EqualFold(oracleName, "DQP") && r.dqpTimeoutCooldownTTL < dqpTimeoutCooldownTTLIntervals {
		r.dqpTimeoutCooldownTTL = dqpTimeoutCooldownTTLIntervals
	}
}

func (r *Runner) observeInfraErrorControl(err error) {
	if err == nil {
		return
	}
	reason, ok := classifyInfraIssue(err)
	if !ok {
		return
	}
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	if r.infraErrorCounts == nil {
		r.infraErrorCounts = make(map[string]int64)
	}
	r.infraErrorCounts[reason]++
	if r.infraUnhealthyTTL < infraUnhealthyTTLIntervals {
		r.infraUnhealthyTTL = infraUnhealthyTTLIntervals
	}
}

func (r *Runner) updateThroughputControlsForInterval(deltaTotal int64) throughputControlState {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	if deltaTotal < 0 {
		deltaTotal = 0
	}
	prevGuardActive := r.throughputGuardTTL > 0
	prevInfraUnhealthy := r.infraUnhealthyTTL > 0
	if r.throughputGuardTTL > 0 {
		r.throughputGuardTTL--
	}
	if r.dqpTimeoutCooldownTTL > 0 {
		r.dqpTimeoutCooldownTTL--
	}
	if r.infraUnhealthyTTL > 0 {
		r.infraUnhealthyTTL--
	}
	lowSample := deltaTotal < minSQLTotalPerInterval
	if lowSample {
		r.throughputLowSampleStreak++
	} else {
		r.throughputLowSampleStreak = 0
	}
	activated := false
	if lowSample && r.throughputLowSampleStreak >= lowSampleStreakTrigger && r.throughputGuardTTL <= 0 {
		r.throughputGuardTTL = throughputGuardTTLIntervals
		r.throughputGuardActivations++
		activated = true
		if r.dqpTimeoutCooldownTTL < dqpTimeoutCooldownTTLIntervals {
			r.dqpTimeoutCooldownTTL = dqpTimeoutCooldownTTLIntervals
		}
	}
	guardActive := r.throughputGuardTTL > 0 || r.infraUnhealthyTTL > 0
	recovered := (prevGuardActive || prevInfraUnhealthy) && !guardActive
	timeoutCounts := make(map[string]int64, len(r.oracleTimeoutCounts))
	for k, v := range r.oracleTimeoutCounts {
		timeoutCounts[k] = v
	}
	infraCounts := make(map[string]int64, len(r.infraErrorCounts))
	for k, v := range r.infraErrorCounts {
		infraCounts[k] = v
	}
	return throughputControlState{
		LowSample:           lowSample,
		LowSampleStreak:     r.throughputLowSampleStreak,
		GuardTTL:            r.throughputGuardTTL,
		GuardActive:         guardActive,
		GuardActivated:      activated,
		GuardRecovered:      recovered,
		GuardActivations:    r.throughputGuardActivations,
		DQPCooldownTTL:      r.dqpTimeoutCooldownTTL,
		DQPCooldownActive:   r.dqpTimeoutCooldownTTL > 0,
		InfraUnhealthyTTL:   r.infraUnhealthyTTL,
		InfraUnhealthy:      r.infraUnhealthyTTL > 0,
		OracleTimeoutCounts: timeoutCounts,
		InfraErrorCounts:    infraCounts,
	}
}
