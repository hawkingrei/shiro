package runner

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"shiro/internal/oracle"
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

func (r *Runner) applyResultMetrics(result oracle.Result) {
	if len(result.Metrics) == 0 {
		return
	}
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	if v, ok := result.Metrics["impo_total"]; ok {
		r.impoTotal += v
	}
	if v, ok := result.Metrics["impo_skip"]; ok {
		r.impoSkips += v
	}
	if v, ok := result.Metrics["impo_trunc"]; ok {
		r.impoTrunc += v
	}
	if result.Oracle == "Impo" && result.Details != nil {
		if reason, ok := result.Details["impo_skip_reason"].(string); ok && reason != "" {
			if r.impoSkipReasons == nil {
				r.impoSkipReasons = make(map[string]int64)
			}
			r.impoSkipReasons[reason]++
			if reason == "base_exec_failed" {
				codeKey := "other"
				if codeVal, ok := result.Details["impo_base_exec_err_code"]; ok {
					if codeInt, ok := codeVal.(int); ok && codeInt > 0 {
						codeKey = fmt.Sprintf("%d", codeInt)
					}
				}
				if r.impoSkipErrCodes == nil {
					r.impoSkipErrCodes = make(map[string]int64)
				}
				r.impoSkipErrCodes[codeKey]++
				if sqlText, ok := result.Details["impo_init_sql"].(string); ok && strings.TrimSpace(sqlText) != "" {
					r.impoLastFailSQL = sqlText
				}
			}
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
		var lastImpoTotal int64
		var lastImpoSkips int64
		var lastImpoTrunc int64
		var lastPlans int
		var lastShapes int
		var lastOps int
		var lastJoins int
		var lastJoinOrders int
		var lastOpSigs int
		var lastSeenSQL int
		lastImpoSkipReasons := make(map[string]int64)
		lastImpoSkipErrCodes := make(map[string]int64)
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
				impoTotal := r.impoTotal
				impoSkips := r.impoSkips
				impoTrunc := r.impoTrunc
				impoSkipReasons := make(map[string]int64, len(r.impoSkipReasons))
				for k, v := range r.impoSkipReasons {
					impoSkipReasons[k] = v
				}
				impoSkipErrCodes := make(map[string]int64, len(r.impoSkipErrCodes))
				for k, v := range r.impoSkipErrCodes {
					impoSkipErrCodes[k] = v
				}
				r.statsMu.Unlock()
				deltaTotal := total - lastTotal
				deltaValid := valid - lastValid
				deltaExists := exists - lastExists
				deltaNotEx := notEx - lastNotEx
				deltaIn := inCount - lastIn
				deltaNotIn := notIn - lastNotIn
				deltaImpoTotal := impoTotal - lastImpoTotal
				deltaImpoSkips := impoSkips - lastImpoSkips
				deltaImpoTrunc := impoTrunc - lastImpoTrunc
				lastTotal = total
				lastValid = valid
				lastExists = exists
				lastNotEx = notEx
				lastIn = inCount
				lastNotIn = notIn
				lastImpoTotal = impoTotal
				lastImpoSkips = impoSkips
				lastImpoTrunc = impoTrunc
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
					if deltaImpoTotal > 0 || deltaImpoSkips > 0 || deltaImpoTrunc > 0 {
						util.Infof(
							"impo stats total=%d(+%d) skipped=%d(+%d) truncated=%d(+%d)",
							impoTotal,
							deltaImpoTotal,
							impoSkips,
							deltaImpoSkips,
							impoTrunc,
							deltaImpoTrunc,
						)
						if r.cfg.Logging.Verbose && deltaImpoSkips > 0 && len(impoSkipReasons) > 0 {
							type reasonDelta struct {
								reason string
								delta  int64
							}
							reasons := make([]reasonDelta, 0, len(impoSkipReasons))
							for reason, total := range impoSkipReasons {
								if prev, ok := lastImpoSkipReasons[reason]; ok {
									if total > prev {
										reasons = append(reasons, reasonDelta{reason: reason, delta: total - prev})
									}
								} else if total > 0 {
									reasons = append(reasons, reasonDelta{reason: reason, delta: total})
								}
							}
							sort.Slice(reasons, func(i, j int) bool {
								if reasons[i].delta == reasons[j].delta {
									return reasons[i].reason < reasons[j].reason
								}
								return reasons[i].delta > reasons[j].delta
							})
							if len(reasons) > 0 {
								limit := 4
								if len(reasons) < limit {
									limit = len(reasons)
								}
								parts := make([]string, 0, limit)
								for i := 0; i < limit; i++ {
									parts = append(parts, fmt.Sprintf("%s=%d", reasons[i].reason, reasons[i].delta))
								}
								util.Infof("impo skip reasons last interval: %s", strings.Join(parts, " "))
							}
						}
						if r.cfg.Logging.Verbose && deltaImpoSkips > 0 {
							if r.impoLastFailSQL != "" {
								util.Infof("impo base_exec_failed example: %s", compactSQL(r.impoLastFailSQL, 2000))
							}
						}
						if r.cfg.Logging.Verbose && deltaImpoSkips > 0 && len(impoSkipErrCodes) > 0 {
							type codeDelta struct {
								code  string
								delta int64
							}
							codes := make([]codeDelta, 0, len(impoSkipErrCodes))
							for code, total := range impoSkipErrCodes {
								if prev, ok := lastImpoSkipErrCodes[code]; ok {
									if total > prev {
										codes = append(codes, codeDelta{code: code, delta: total - prev})
									}
								} else if total > 0 {
									codes = append(codes, codeDelta{code: code, delta: total})
								}
							}
							sort.Slice(codes, func(i, j int) bool {
								if codes[i].delta == codes[j].delta {
									return codes[i].code < codes[j].code
								}
								return codes[i].delta > codes[j].delta
							})
							if len(codes) > 0 {
								limit := 4
								if len(codes) < limit {
									limit = len(codes)
								}
								parts := make([]string, 0, limit)
								for i := 0; i < limit; i++ {
									parts = append(parts, fmt.Sprintf("%s=%d", codes[i].code, codes[i].delta))
								}
								util.Infof("impo base_exec_failed codes last interval: %s", strings.Join(parts, " "))
							}
						}
						if r.cfg.Logging.Verbose && deltaImpoSkips > 0 {
							lastImpoSkipReasons = impoSkipReasons
							lastImpoSkipErrCodes = impoSkipErrCodes
						}
					}
					if r.cfg.QPG.Enabled && r.cfg.Logging.Verbose && r.qpgState != nil {
						r.qpgMu.Lock()
						plans, shapes, ops, joins, joinOrders, opSigs, seenSQL := r.qpgState.stats()
						deltaPlans := plans - lastPlans
						deltaShapes := shapes - lastShapes
						deltaOps := ops - lastOps
						deltaJoins := joins - lastJoins
						deltaJoinOrders := joinOrders - lastJoinOrders
						deltaOpSigs := opSigs - lastOpSigs
						deltaSeenSQL := seenSQL - lastSeenSQL
						lastPlans = plans
						lastShapes = shapes
						lastOps = ops
						lastJoins = joins
						lastJoinOrders = joinOrders
						lastOpSigs = opSigs
						lastSeenSQL = seenSQL
						util.Infof(
							"qpg stats plans=%d(+%d) shapes=%d(+%d) ops=%d(+%d) join_types=%d(+%d) join_orders=%d(+%d) op_sigs=%d(+%d) seen_sql=%d(+%d)",
							plans,
							deltaPlans,
							shapes,
							deltaShapes,
							ops,
							deltaOps,
							joins,
							deltaJoins,
							joinOrders,
							deltaJoinOrders,
							opSigs,
							deltaOpSigs,
							seenSQL,
							deltaSeenSQL,
						)
						if r.qpgState.lastOverride != "" && r.qpgState.lastOverride != r.qpgState.lastOverrideLogged {
							util.Infof("qpg override=%s", r.qpgState.lastOverride)
							r.qpgState.lastOverrideLogged = r.qpgState.lastOverride
						}
						if r.qpgState.lastTemplate != "" && r.qpgState.lastTemplate != r.qpgState.lastTemplateLogged {
							util.Infof("qpg template_override=%s", r.qpgState.lastTemplate)
							r.qpgState.lastTemplateLogged = r.qpgState.lastTemplate
						}
						r.qpgMu.Unlock()
					}
					r.dumpDynamicState()
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

func compactSQL(sqlText string, limit int) string {
	if limit <= 0 {
		limit = 200
	}
	parts := strings.Fields(sqlText)
	if len(parts) == 0 {
		return ""
	}
	compact := strings.Join(parts, " ")
	if len(compact) <= limit {
		return compact
	}
	return compact[:limit] + "..."
}
