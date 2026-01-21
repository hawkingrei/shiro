package runner

import (
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"shiro/internal/util"
)

var globalDBSeq atomic.Int64
var notInWrappedPattern = regexp.MustCompile(`(?i)NOT\s*\([^)]*\bIN\s*\(`)

func (r *Runner) observeSQL(sql string, err error) {
	if strings.TrimSpace(sql) == "" {
		return
	}
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	r.sqlTotal++
	if err == nil {
		r.sqlValid++
		upper := strings.ToUpper(sql)
		if strings.Contains(upper, "NOT EXISTS") {
			r.sqlNotEx++
		} else if strings.Contains(upper, "EXISTS") {
			r.sqlExists++
		}
		if strings.Contains(upper, " NOT IN (") || notInWrappedPattern.MatchString(upper) {
			r.sqlNotIn++
		} else if strings.Contains(upper, " IN (") {
			r.sqlIn++
		}
	}
}

func (r *Runner) startStatsLogger() func() {
	interval := time.Duration(r.cfg.Logging.ReportIntervalSeconds) * time.Second
	if interval <= 0 {
		return func() {}
	}
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	go func() {
		var lastTotal int64
		var lastValid int64
		var lastExists int64
		var lastNotEx int64
		var lastIn int64
		var lastNotIn int64
		var lastPlans int
		var lastShapes int
		var lastOps int
		var lastJoins int
		for {
			select {
			case <-ticker.C:
				r.statsMu.Lock()
				total := r.sqlTotal
				valid := r.sqlValid
				exists := r.sqlExists
				notEx := r.sqlNotEx
				inCount := r.sqlIn
				notIn := r.sqlNotIn
				r.statsMu.Unlock()
				deltaTotal := total - lastTotal
				deltaValid := valid - lastValid
				deltaExists := exists - lastExists
				deltaNotEx := notEx - lastNotEx
				deltaIn := inCount - lastIn
				deltaNotIn := notIn - lastNotIn
				lastTotal = total
				lastValid = valid
				lastExists = exists
				lastNotEx = notEx
				lastIn = inCount
				lastNotIn = notIn
				if deltaTotal > 0 {
					util.Infof(
						"sql_valid/total last interval: %d/%d exists=%d not_exists=%d in=%d not_in=%d",
						deltaValid,
						deltaTotal,
						deltaExists,
						deltaNotEx,
						deltaIn,
						deltaNotIn,
					)
					if r.cfg.QPG.Enabled && r.cfg.Logging.Verbose && r.qpgState != nil {
						plans, shapes, ops, joins := r.qpgState.stats()
						deltaPlans := plans - lastPlans
						deltaShapes := shapes - lastShapes
						deltaOps := ops - lastOps
						deltaJoins := joins - lastJoins
						lastPlans = plans
						lastShapes = shapes
						lastOps = ops
						lastJoins = joins
						util.Infof(
							"qpg stats plans=%d(+%d) shapes=%d(+%d) ops=%d(+%d) join_types=%d(+%d)",
							plans,
							deltaPlans,
							shapes,
							deltaShapes,
							ops,
							deltaOps,
							joins,
							deltaJoins,
						)
						if r.qpgState.lastOverride != "" && r.qpgState.lastOverride != r.qpgState.lastOverrideLogged {
							util.Infof("qpg override=%s", r.qpgState.lastOverride)
							r.qpgState.lastOverrideLogged = r.qpgState.lastOverride
						}
					}
				}
			case <-done:
				return
			}
		}
	}()
	return func() {
		close(done)
		ticker.Stop()
	}
}
