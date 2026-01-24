package runner

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"shiro/internal/generator"
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

func (r *Runner) observeJoinCount(query *generator.SelectQuery) {
	if query == nil || query.From == nil {
		return
	}
	joinCount := len(query.From.Joins)
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	r.joinCounts[joinCount]++
}

func (r *Runner) observeJoinCountValue(joinCount int) {
	if joinCount < 0 {
		return
	}
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	if r.joinCounts == nil {
		r.joinCounts = make(map[int]int64)
	}
	r.joinCounts[joinCount]++
}

func (r *Runner) observeJoinSignature(features *generator.QueryFeatures) {
	if features == nil {
		return
	}
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	if r.joinTypeSeqs == nil {
		r.joinTypeSeqs = make(map[string]int64)
	}
	if r.joinGraphSigs == nil {
		r.joinGraphSigs = make(map[string]int64)
	}
	if features.JoinTypeSeq != "" {
		r.joinTypeSeqs[features.JoinTypeSeq]++
	}
	if features.JoinGraphSig != "" {
		r.joinGraphSigs[features.JoinGraphSig]++
	}
	if features.PredicatePairsTotal > 0 {
		r.predicatePairsTotal += features.PredicatePairsTotal
		r.predicatePairsJoin += features.PredicatePairsJoin
	}
}

func (r *Runner) applyResultMetrics(result oracle.Result) {
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
	if result.Truth != nil && result.Truth.Enabled && result.Truth.Mismatch {
		r.truthMismatches++
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
		lastJoinCounts := make(map[int]int64)
		lastJoinTypeSeqs := make(map[string]int64)
		lastJoinGraphSigs := make(map[string]int64)
		var lastTruthMismatches int64
		var lastPredicatePairsTotal int64
		var lastPredicatePairsJoin int64
		lastImpoSkipReasons := make(map[string]int64)
		lastImpoSkipErrCodes := make(map[string]int64)
		lastImpoReasonTotals := make(map[string]int64)
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
				truthMismatches := r.truthMismatches
				joinCounts := make(map[int]int64, len(r.joinCounts))
				for k, v := range r.joinCounts {
					joinCounts[k] = v
				}
				joinTypeSeqs := make(map[string]int64, len(r.joinTypeSeqs))
				for k, v := range r.joinTypeSeqs {
					joinTypeSeqs[k] = v
				}
				joinGraphSigs := make(map[string]int64, len(r.joinGraphSigs))
				for k, v := range r.joinGraphSigs {
					joinGraphSigs[k] = v
				}
				predicatePairsTotal := r.predicatePairsTotal
				predicatePairsJoin := r.predicatePairsJoin
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
				deltaTruthMismatches := truthMismatches - lastTruthMismatches
				deltaPredicatePairsTotal := predicatePairsTotal - lastPredicatePairsTotal
				deltaPredicatePairsJoin := predicatePairsJoin - lastPredicatePairsJoin
				deltaJoinCounts := make(map[int]int64, len(joinCounts))
				for k, v := range joinCounts {
					prev := lastJoinCounts[k]
					if v-prev > 0 {
						deltaJoinCounts[k] = v - prev
					}
				}
				lastJoinCounts = joinCounts
				deltaJoinTypeSeqs := make(map[string]int64, len(joinTypeSeqs))
				for k, v := range joinTypeSeqs {
					prev := lastJoinTypeSeqs[k]
					if v-prev > 0 {
						deltaJoinTypeSeqs[k] = v - prev
					}
				}
				lastJoinTypeSeqs = joinTypeSeqs
				deltaJoinGraphSigs := make(map[string]int64, len(joinGraphSigs))
				for k, v := range joinGraphSigs {
					prev := lastJoinGraphSigs[k]
					if v-prev > 0 {
						deltaJoinGraphSigs[k] = v - prev
					}
				}
				lastJoinGraphSigs = joinGraphSigs
				lastPredicatePairsTotal = predicatePairsTotal
				lastPredicatePairsJoin = predicatePairsJoin
				lastTotal = total
				lastValid = valid
				lastExists = exists
				lastNotEx = notEx
				lastIn = inCount
				lastNotIn = notIn
				lastImpoTotal = impoTotal
				lastImpoSkips = impoSkips
				lastImpoTrunc = impoTrunc
				lastTruthMismatches = truthMismatches
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
					sqlValidRatio := float64(deltaValid) / float64(deltaTotal)
					var impoInvalidRatio float64
					var impoBaseExecRatio float64
					var predicateJoinRatio float64
					var deltaInvalid int64
					var deltaBaseExec int64
					if deltaImpoTotal > 0 {
						if total, ok := impoSkipReasons["invalid_columns"]; ok {
							if prev, ok := lastImpoReasonTotals["invalid_columns"]; ok {
								deltaInvalid = total - prev
							} else {
								deltaInvalid = total
							}
						}
						if total, ok := impoSkipReasons["base_exec_failed"]; ok {
							if prev, ok := lastImpoReasonTotals["base_exec_failed"]; ok {
								deltaBaseExec = total - prev
							} else {
								deltaBaseExec = total
							}
						}
						impoInvalidRatio = float64(deltaInvalid) / float64(deltaImpoTotal)
						impoBaseExecRatio = float64(deltaBaseExec) / float64(deltaImpoTotal)
					}
					if deltaPredicatePairsTotal > 0 {
						predicateJoinRatio = float64(deltaPredicatePairsJoin) / float64(deltaPredicatePairsTotal)
					}
					util.Infof(
						"metrics last interval: sql_valid_ratio=%.3f impo_invalid_columns_ratio=%.3f impo_base_exec_failed_ratio=%.3f predicate_join_pair_ratio=%.3f",
						sqlValidRatio,
						impoInvalidRatio,
						impoBaseExecRatio,
						predicateJoinRatio,
					)
					if deltaTruthMismatches > 0 {
						util.Infof("groundtruth mismatches last interval: %d", deltaTruthMismatches)
					}
					if len(deltaJoinCounts) > 0 {
						keys := make([]int, 0, len(deltaJoinCounts))
						for k := range deltaJoinCounts {
							keys = append(keys, k)
						}
						sort.Ints(keys)
						parts := make([]string, 0, len(keys))
						for _, k := range keys {
							parts = append(parts, fmt.Sprintf("%d=%d", k, deltaJoinCounts[k]))
						}
						util.Infof("join_count last interval: %s", strings.Join(parts, " "))
					}
					if len(deltaJoinTypeSeqs) > 0 {
						keys := make([]string, 0, len(deltaJoinTypeSeqs))
						for k := range deltaJoinTypeSeqs {
							keys = append(keys, k)
						}
						sort.Strings(keys)
						parts := make([]string, 0, len(keys))
						for _, k := range keys {
							parts = append(parts, fmt.Sprintf("%s=%d", k, deltaJoinTypeSeqs[k]))
						}
						util.Infof("join_type_seq last interval: %s", strings.Join(parts, " "))
					}
					if len(deltaJoinGraphSigs) > 0 {
						keys := make([]string, 0, len(deltaJoinGraphSigs))
						for k := range deltaJoinGraphSigs {
							keys = append(keys, k)
						}
						sort.Strings(keys)
						parts := make([]string, 0, len(keys))
						for _, k := range keys {
							parts = append(parts, fmt.Sprintf("%s=%d", k, deltaJoinGraphSigs[k]))
						}
						util.Infof("join_graph_sig last interval: %s", strings.Join(parts, " "))
					}
					thresholds := r.cfg.Logging.Metrics
					if thresholds.SQLValidMinRatio > 0 && sqlValidRatio < thresholds.SQLValidMinRatio {
						util.Warnf(
							"metrics threshold breached: sql_valid_ratio=%.3f < %.3f",
							sqlValidRatio,
							thresholds.SQLValidMinRatio,
						)
					}
					if thresholds.ImpoInvalidColumnsMaxRatio > 0 && impoInvalidRatio > thresholds.ImpoInvalidColumnsMaxRatio {
						util.Warnf(
							"metrics threshold breached: impo_invalid_columns_ratio=%.3f > %.3f",
							impoInvalidRatio,
							thresholds.ImpoInvalidColumnsMaxRatio,
						)
					}
					if thresholds.ImpoBaseExecFailedMaxRatio > 0 && impoBaseExecRatio > thresholds.ImpoBaseExecFailedMaxRatio {
						util.Warnf(
							"metrics threshold breached: impo_base_exec_failed_ratio=%.3f > %.3f",
							impoBaseExecRatio,
							thresholds.ImpoBaseExecFailedMaxRatio,
						)
					}
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
					lastImpoReasonTotals = impoSkipReasons
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
