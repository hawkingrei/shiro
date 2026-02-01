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
	"shiro/internal/tqs"
	"shiro/internal/util"
)

var globalDBSeq atomic.Int64
var notInWrappedPattern = regexp.MustCompile(`(?i)NOT\s*\([^)]*\bIN\s*\(`)
var existsPattern = regexp.MustCompile(`(?i)\bEXISTS\b`)
var notExistsPattern = regexp.MustCompile(`(?i)\bNOT\s+EXISTS\b`)
var inSubqueryPattern = regexp.MustCompile(`(?i)\bIN\s*\(\s*SELECT\b`)
var notInSubqueryPattern = regexp.MustCompile(`(?i)\bNOT\s+IN\s*\(\s*SELECT\b`)

const topJoinSigN = 20
const topOracleReasonsN = 10
const topOracleSummaryN = 3

func ratio(numerator, denominator int64) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

type oracleFunnel struct {
	Runs         int64
	Skips        int64
	Errors       int64
	Mismatches   int64
	Panics       int64
	Reports      int64
	SkipReasons  map[string]int64
	ErrorReasons map[string]int64
}

type subqueryOracleStats struct {
	allowed         int64
	disallowed      int64
	has             int64
	attempted       int64
	built           int64
	failed          int64
	disallowReasons map[string]int64
}

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
		if notExistsPattern.MatchString(sql) {
			r.sqlNotEx++
		} else if existsPattern.MatchString(sql) {
			r.sqlExists++
		}
		if notInSubqueryPattern.MatchString(upper) {
			r.sqlNotInSubquery++
		} else if inSubqueryPattern.MatchString(upper) {
			r.sqlInSubquery++
		}
		if strings.Contains(upper, " NOT IN (") || notInWrappedPattern.MatchString(upper) {
			r.sqlNotIn++
		} else if strings.Contains(upper, " IN (") {
			r.sqlIn++
		}
	}
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

func (r *Runner) observeJoinSignature(features *generator.QueryFeatures, oracleName string) {
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
	if features.ViewCount > 0 {
		r.viewQueries++
		r.viewTableRefs += int64(features.ViewCount)
	}
	if features.PredicatePairsTotal > 0 {
		r.predicatePairsTotal += features.PredicatePairsTotal
		r.predicatePairsJoin += features.PredicatePairsJoin
	}
	if features.SubqueryAllowed {
		r.subqueryAllowed++
	} else {
		r.subqueryDisallowed++
		if features.SubqueryDisallowReason != "" {
			if r.subqueryDisallowReasons == nil {
				r.subqueryDisallowReasons = make(map[string]int64)
			}
			r.subqueryDisallowReasons[features.SubqueryDisallowReason]++
		}
	}
	if features.HasSubquery {
		r.subqueryHas++
	}
	if features.SubqueryAttempts > 0 {
		r.subqueryAttempts += features.SubqueryAttempts
	}
	if features.SubqueryBuilt > 0 {
		r.subqueryBuilt += features.SubqueryBuilt
	}
	if features.SubqueryFailed > 0 {
		r.subqueryFailed += features.SubqueryFailed
	}
	if oracleName != "" {
		perOracle := r.subqueryOracleStats[oracleName]
		if perOracle == nil {
			perOracle = &subqueryOracleStats{
				disallowReasons: make(map[string]int64),
			}
			r.subqueryOracleStats[oracleName] = perOracle
		}
		if features.SubqueryAllowed {
			perOracle.allowed++
		} else {
			perOracle.disallowed++
			if features.SubqueryDisallowReason != "" {
				perOracle.disallowReasons[features.SubqueryDisallowReason]++
			}
		}
		if features.HasSubquery {
			perOracle.has++
		}
		perOracle.attempted += features.SubqueryAttempts
		perOracle.built += features.SubqueryBuilt
		perOracle.failed += features.SubqueryFailed
	}
}

func (r *Runner) observeOracleRun(name string) {
	if name == "" {
		return
	}
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	stat := r.oracleStats[name]
	if stat == nil {
		stat = &oracleFunnel{
			SkipReasons:  make(map[string]int64),
			ErrorReasons: make(map[string]int64),
		}
		r.oracleStats[name] = stat
	}
	stat.Runs++
}

func (r *Runner) observeOracleResult(name string, result oracle.Result, skipReason string, reported bool, isPanic bool) {
	if name == "" {
		return
	}
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	stat := r.oracleStats[name]
	if stat == nil {
		stat = &oracleFunnel{
			SkipReasons:  make(map[string]int64),
			ErrorReasons: make(map[string]int64),
		}
		r.oracleStats[name] = stat
	}
	if skipReason != "" {
		stat.Skips++
		stat.SkipReasons[skipReason]++
	}
	if result.Err != nil {
		stat.Errors++
		if result.Details != nil {
			if reason, ok := result.Details["error_reason"].(string); ok && reason != "" {
				stat.ErrorReasons[reason]++
				if result.Oracle == "CERT" {
					r.certLastErrReason = reason
				}
				if result.Oracle == "TLP" {
					r.tlpLastErrReason = reason
				}
			}
		}
		if result.Oracle == "CERT" {
			if len(result.SQL) > 0 {
				r.certLastErrSQL = result.SQL[len(result.SQL)-1]
			}
			r.certLastErr = result.Err.Error()
		}
		if result.Oracle == "TLP" {
			if len(result.SQL) > 0 {
				r.tlpLastErrSQL = result.SQL[len(result.SQL)-1]
			}
			r.tlpLastErr = result.Err.Error()
		}
	}
	if !result.OK {
		stat.Mismatches++
	}
	if isPanic {
		stat.Panics++
	}
	if reported {
		stat.Reports++
	}
}

func oracleSkipReason(result oracle.Result) string {
	if result.Details == nil {
		return ""
	}
	if v, ok := result.Details["skip_reason"].(string); ok && v != "" {
		return v
	}
	if v, ok := result.Details["groundtruth_skip"].(string); ok && v != "" {
		return "groundtruth:" + v
	}
	if v, ok := result.Details["impo_skip_reason"].(string); ok && v != "" {
		return "impo:" + v
	}
	return ""
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
				if errText, ok := result.Details["impo_base_exec_err"].(string); ok && strings.TrimSpace(errText) != "" {
					r.impoLastFailErr = errText
				}
			}
		}
		if counts, ok := result.Details["impo_mutation_counts"].(map[string]int64); ok {
			for name, val := range counts {
				r.impoMutationCounts[name] += val
			}
		}
		if counts, ok := result.Details["impo_mutation_exec_counts"].(map[string]int64); ok {
			for name, val := range counts {
				r.impoMutationExecCounts[name] += val
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
		var lastInSubquery int64
		var lastNotInSubquery int64
		var lastImpoTotal int64
		var lastImpoSkips int64
		var lastImpoTrunc int64
		var lastViewQueries int64
		var lastViewTableRefs int64
		var lastPlans int
		var lastShapes int
		var lastOps int
		var lastJoins int
		var lastJoinOrders int
		var lastOpSigs int
		var lastSeenSQL int
		var lastTQSSteps int64
		var lastTQSCovered int
		var lastTQSEdges int
		lastJoinCounts := make(map[int]int64)
		lastJoinTypeSeqs := make(map[string]int64)
		lastJoinGraphSigs := make(map[string]int64)
		var lastTruthMismatches int64
		var lastPredicatePairsTotal int64
		var lastPredicatePairsJoin int64
		var lastSubqueryAllowed int64
		var lastSubqueryDisallowed int64
		var lastSubqueryHas int64
		var lastSubqueryAttempts int64
		var lastSubqueryBuilt int64
		var lastSubqueryFailed int64
		lastSubqueryDisallowReasons := make(map[string]int64)
		lastSubqueryOracleStats := make(map[string]subqueryOracleStats)
		lastImpoSkipReasons := make(map[string]int64)
		lastImpoSkipErrCodes := make(map[string]int64)
		lastImpoReasonTotals := make(map[string]int64)
		lastImpoMutationCounts := make(map[string]int64)
		lastImpoMutationExecCounts := make(map[string]int64)
		lastOracleStats := make(map[string]oracleFunnel)
		var lastOraclePickTotal int64
		var lastCertPickTotal int64
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
				inSubquery := r.sqlInSubquery
				notInSubquery := r.sqlNotInSubquery
				impoTotal := r.impoTotal
				impoSkips := r.impoSkips
				impoTrunc := r.impoTrunc
				oraclePickTotal := r.oraclePickTotal
				certPickTotal := r.certPickTotal
				viewQueries := r.viewQueries
				viewTableRefs := r.viewTableRefs
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
				subqueryAllowed := r.subqueryAllowed
				subqueryDisallowed := r.subqueryDisallowed
				subqueryHas := r.subqueryHas
				subqueryAttempts := r.subqueryAttempts
				subqueryBuilt := r.subqueryBuilt
				subqueryFailed := r.subqueryFailed
				subqueryDisallowReasons := make(map[string]int64, len(r.subqueryDisallowReasons))
				for k, v := range r.subqueryDisallowReasons {
					subqueryDisallowReasons[k] = v
				}
				subqueryOracleStatsByName := make(map[string]subqueryOracleStats, len(r.subqueryOracleStats))
				for name, stats := range r.subqueryOracleStats {
					if stats == nil {
						continue
					}
					reasonsCopy := make(map[string]int64, len(stats.disallowReasons))
					for k, v := range stats.disallowReasons {
						reasonsCopy[k] = v
					}
					copyStats := subqueryOracleStats{
						allowed:         stats.allowed,
						disallowed:      stats.disallowed,
						has:             stats.has,
						attempted:       stats.attempted,
						built:           stats.built,
						failed:          stats.failed,
						disallowReasons: reasonsCopy,
					}
					subqueryOracleStatsByName[name] = copyStats
				}
				impoSkipReasons := make(map[string]int64, len(r.impoSkipReasons))
				for k, v := range r.impoSkipReasons {
					impoSkipReasons[k] = v
				}
				impoSkipErrCodes := make(map[string]int64, len(r.impoSkipErrCodes))
				for k, v := range r.impoSkipErrCodes {
					impoSkipErrCodes[k] = v
				}
				impoMutationCounts := make(map[string]int64, len(r.impoMutationCounts))
				for k, v := range r.impoMutationCounts {
					impoMutationCounts[k] = v
				}
				impoMutationExecCounts := make(map[string]int64, len(r.impoMutationExecCounts))
				for k, v := range r.impoMutationExecCounts {
					impoMutationExecCounts[k] = v
				}
				oracleStats := make(map[string]oracleFunnel, len(r.oracleStats))
				for name, stat := range r.oracleStats {
					if stat == nil {
						continue
					}
					copyStat := oracleFunnel{
						Runs:       stat.Runs,
						Skips:      stat.Skips,
						Errors:     stat.Errors,
						Mismatches: stat.Mismatches,
						Panics:     stat.Panics,
						Reports:    stat.Reports,
						SkipReasons: func() map[string]int64 {
							out := make(map[string]int64, len(stat.SkipReasons))
							for k, v := range stat.SkipReasons {
								out[k] = v
							}
							return out
						}(),
						ErrorReasons: func() map[string]int64 {
							out := make(map[string]int64, len(stat.ErrorReasons))
							for k, v := range stat.ErrorReasons {
								out[k] = v
							}
							return out
						}(),
					}
					oracleStats[name] = copyStat
				}
				r.statsMu.Unlock()
				deltaTotal := total - lastTotal
				deltaValid := valid - lastValid
				deltaExists := exists - lastExists
				deltaNotEx := notEx - lastNotEx
				deltaIn := inCount - lastIn
				deltaNotIn := notIn - lastNotIn
				deltaInSubquery := inSubquery - lastInSubquery
				deltaNotInSubquery := notInSubquery - lastNotInSubquery
				deltaImpoTotal := impoTotal - lastImpoTotal
				deltaImpoSkips := impoSkips - lastImpoSkips
				deltaImpoTrunc := impoTrunc - lastImpoTrunc
				deltaOraclePicks := oraclePickTotal - lastOraclePickTotal
				deltaCertPicks := certPickTotal - lastCertPickTotal
				deltaViewQueries := viewQueries - lastViewQueries
				deltaViewTableRefs := viewTableRefs - lastViewTableRefs
				deltaTruthMismatches := truthMismatches - lastTruthMismatches
				deltaPredicatePairsTotal := predicatePairsTotal - lastPredicatePairsTotal
				deltaPredicatePairsJoin := predicatePairsJoin - lastPredicatePairsJoin
				deltaSubqueryAllowed := subqueryAllowed - lastSubqueryAllowed
				deltaSubqueryDisallowed := subqueryDisallowed - lastSubqueryDisallowed
				deltaSubqueryHas := subqueryHas - lastSubqueryHas
				deltaSubqueryAttempts := subqueryAttempts - lastSubqueryAttempts
				deltaSubqueryBuilt := subqueryBuilt - lastSubqueryBuilt
				deltaSubqueryFailed := subqueryFailed - lastSubqueryFailed
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
				deltaImpoMutationCounts := make(map[string]int64, len(impoMutationCounts))
				for k, v := range impoMutationCounts {
					prev := lastImpoMutationCounts[k]
					if v-prev > 0 {
						deltaImpoMutationCounts[k] = v - prev
					}
				}
				lastImpoMutationCounts = impoMutationCounts
				deltaImpoMutationExecCounts := make(map[string]int64, len(impoMutationExecCounts))
				for k, v := range impoMutationExecCounts {
					prev := lastImpoMutationExecCounts[k]
					if v-prev > 0 {
						deltaImpoMutationExecCounts[k] = v - prev
					}
				}
				lastImpoMutationExecCounts = impoMutationExecCounts
				lastPredicatePairsTotal = predicatePairsTotal
				lastPredicatePairsJoin = predicatePairsJoin
				lastSubqueryAllowed = subqueryAllowed
				lastSubqueryDisallowed = subqueryDisallowed
				lastSubqueryHas = subqueryHas
				lastSubqueryAttempts = subqueryAttempts
				lastSubqueryBuilt = subqueryBuilt
				lastSubqueryFailed = subqueryFailed
				lastTotal = total
				lastValid = valid
				lastExists = exists
				lastNotEx = notEx
				lastIn = inCount
				lastNotIn = notIn
				lastInSubquery = inSubquery
				lastNotInSubquery = notInSubquery
				lastImpoTotal = impoTotal
				lastImpoSkips = impoSkips
				lastImpoTrunc = impoTrunc
				lastOraclePickTotal = oraclePickTotal
				lastCertPickTotal = certPickTotal
				lastViewQueries = viewQueries
				lastViewTableRefs = viewTableRefs
				lastTruthMismatches = truthMismatches
				var tqsStats tqs.Stats
				if r.tqsHistory != nil {
					tqsStats = r.tqsHistory.Stats()
				}
				if deltaTotal > 0 {
					sqlValidRatio := float64(deltaValid) / float64(deltaTotal)
					deltaInvalidSQL := deltaTotal - deltaValid
					util.Infof(
						"sql_valid/total last interval: %d/%d (%.3f) invalid=%d exists=%d not_exists=%d in=%d not_in=%d in_subquery=%d not_in_subquery=%d",
						deltaValid,
						deltaTotal,
						sqlValidRatio,
						deltaInvalidSQL,
						deltaExists,
						deltaNotEx,
						deltaIn,
						deltaNotIn,
						deltaInSubquery,
						deltaNotInSubquery,
					)
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
					if deltaSubqueryAllowed > 0 || deltaSubqueryDisallowed > 0 || deltaSubqueryHas > 0 || deltaSubqueryAttempts > 0 || deltaSubqueryBuilt > 0 || deltaSubqueryFailed > 0 {
						subqueryMissing := deltaSubqueryAllowed - deltaSubqueryHas
						if subqueryMissing < 0 {
							subqueryMissing = 0
						}
						util.Infof(
							"subquery last interval: allowed=%d disallowed=%d has=%d missing=%d attempted=%d built=%d failed=%d",
							deltaSubqueryAllowed,
							deltaSubqueryDisallowed,
							deltaSubqueryHas,
							subqueryMissing,
							deltaSubqueryAttempts,
							deltaSubqueryBuilt,
							deltaSubqueryFailed,
						)
					}
					util.Infof(
						"metrics last interval: sql_valid_ratio=%.3f impo_invalid_columns_ratio=%.3f impo_base_exec_failed_ratio=%.3f predicate_join_pair_ratio=%.3f",
						sqlValidRatio,
						impoInvalidRatio,
						impoBaseExecRatio,
						predicateJoinRatio,
					)
					deltaSubqueryDisallowReasons := make(map[string]int64, len(subqueryDisallowReasons))
					for reason, total := range subqueryDisallowReasons {
						prev := lastSubqueryDisallowReasons[reason]
						if total-prev > 0 {
							deltaSubqueryDisallowReasons[reason] = total - prev
						}
					}
					lastSubqueryDisallowReasons = subqueryDisallowReasons
					if len(deltaSubqueryDisallowReasons) > 0 {
						util.Detailf(
							"subquery_disallow_reasons last interval top=%d: %s",
							topOracleReasonsN,
							formatTopJoinSigs(deltaSubqueryDisallowReasons, topOracleReasonsN),
						)
					}
					if len(subqueryOracleStatsByName) > 0 {
						oracleNames := make([]string, 0, len(subqueryOracleStatsByName))
						for name := range subqueryOracleStatsByName {
							oracleNames = append(oracleNames, name)
						}
						sort.Strings(oracleNames)
						for _, name := range oracleNames {
							stats := subqueryOracleStatsByName[name]
							prev := lastSubqueryOracleStats[name]
							deltaAllowed := stats.allowed - prev.allowed
							deltaDisallowed := stats.disallowed - prev.disallowed
							deltaHas := stats.has - prev.has
							deltaAttempted := stats.attempted - prev.attempted
							deltaBuilt := stats.built - prev.built
							deltaFailed := stats.failed - prev.failed
							if deltaAllowed == 0 && deltaDisallowed == 0 && deltaHas == 0 && deltaAttempted == 0 && deltaBuilt == 0 && deltaFailed == 0 {
								continue
							}
							missing := deltaAllowed - deltaHas
							if missing < 0 {
								missing = 0
							}
							util.Detailf(
								"subquery oracle=%s last interval: allowed=%d disallowed=%d has=%d missing=%d attempted=%d built=%d failed=%d",
								name,
								deltaAllowed,
								deltaDisallowed,
								deltaHas,
								missing,
								deltaAttempted,
								deltaBuilt,
								deltaFailed,
							)
							deltaReasons := make(map[string]int64, len(stats.disallowReasons))
							for reason, total := range stats.disallowReasons {
								prevVal := int64(0)
								if prevStats, ok := lastSubqueryOracleStats[name]; ok {
									prevVal = prevStats.disallowReasons[reason]
								}
								if total-prevVal > 0 {
									deltaReasons[reason] = total - prevVal
								}
							}
							if len(deltaReasons) > 0 {
								util.Detailf(
									"subquery oracle=%s disallow_reasons last interval top=%d: %s",
									name,
									topOracleReasonsN,
									formatTopJoinSigs(deltaReasons, topOracleReasonsN),
								)
							}
						}
						lastSubqueryOracleStats = subqueryOracleStatsByName
					}
					if deltaTruthMismatches > 0 {
						util.Infof("groundtruth mismatches last interval: %d", deltaTruthMismatches)
					}
					if len(deltaJoinCounts) > 0 {
						util.Detailf(
							"join_count last interval top=%d total=%d: %s",
							topJoinSigN,
							len(deltaJoinCounts),
							formatTopJoinCount(deltaJoinCounts, topJoinSigN),
						)
					}
					if len(deltaJoinTypeSeqs) > 0 {
						util.Detailf(
							"join_type_seq last interval top=%d total=%d: %s",
							topJoinSigN,
							len(deltaJoinTypeSeqs),
							formatTopJoinSigs(deltaJoinTypeSeqs, topJoinSigN),
						)
					}
					if len(deltaJoinGraphSigs) > 0 {
						util.Detailf(
							"join_graph_sig last interval top=%d total=%d: %s",
							topJoinSigN,
							len(deltaJoinGraphSigs),
							formatTopJoinSigs(deltaJoinGraphSigs, topJoinSigN),
						)
					}
					if len(deltaImpoMutationCounts) > 0 {
						util.Detailf(
							"impo_mutations last interval top=%d total=%d: %s",
							topOracleReasonsN,
							len(deltaImpoMutationCounts),
							formatTopJoinSigs(deltaImpoMutationCounts, topOracleReasonsN),
						)
					}
					if len(deltaImpoMutationExecCounts) > 0 {
						util.Detailf(
							"impo_mutations_exec last interval top=%d total=%d: %s",
							topOracleReasonsN,
							len(deltaImpoMutationExecCounts),
							formatTopJoinSigs(deltaImpoMutationExecCounts, topOracleReasonsN),
						)
					}
					if deltaViewQueries > 0 || deltaViewTableRefs > 0 {
						util.Infof(
							"view usage last interval: queries=%d tables=%d",
							deltaViewQueries,
							deltaViewTableRefs,
						)
					}
					if len(oracleStats) > 0 {
						deltaFunnel := make(map[string]oracleFunnel, len(oracleStats))
						for name, stat := range oracleStats {
							prev := lastOracleStats[name]
							delta := oracleFunnel{
								Runs:       stat.Runs - prev.Runs,
								Skips:      stat.Skips - prev.Skips,
								Errors:     stat.Errors - prev.Errors,
								Mismatches: stat.Mismatches - prev.Mismatches,
								Panics:     stat.Panics - prev.Panics,
								Reports:    stat.Reports - prev.Reports,
								SkipReasons: func() map[string]int64 {
									out := make(map[string]int64)
									for k, v := range stat.SkipReasons {
										prevVal := prev.SkipReasons[k]
										if v-prevVal > 0 {
											out[k] = v - prevVal
										}
									}
									return out
								}(),
								ErrorReasons: func() map[string]int64 {
									out := make(map[string]int64)
									for k, v := range stat.ErrorReasons {
										prevVal := prev.ErrorReasons[k]
										if v-prevVal > 0 {
											out[k] = v - prevVal
										}
									}
									return out
								}(),
							}
							if delta.Runs > 0 || delta.Skips > 0 || delta.Errors > 0 || delta.Mismatches > 0 || delta.Panics > 0 || delta.Reports > 0 {
								deltaFunnel[name] = delta
							}
						}
						if len(deltaFunnel) > 0 {
							util.Detailf("oracle_funnel last interval: %s", formatOracleFunnel(deltaFunnel))
							if r.cfg.Logging.Verbose {
								for name, delta := range deltaFunnel {
									if len(delta.SkipReasons) == 0 {
										continue
									}
									util.Detailf("oracle_skip_reasons last interval oracle=%s top=%d: %s", name, topOracleReasonsN, formatTopJoinSigs(delta.SkipReasons, topOracleReasonsN))
								}
								for name, delta := range deltaFunnel {
									if len(delta.ErrorReasons) == 0 {
										continue
									}
									util.Detailf("oracle_error_reasons last interval oracle=%s top=%d: %s", name, topOracleReasonsN, formatTopJoinSigs(delta.ErrorReasons, topOracleReasonsN))
								}
								if certDelta, ok := deltaFunnel["CERT"]; ok && certDelta.Errors > 0 {
									if r.certLastErrSQL != "" && r.certLastErr != "" {
										util.Detailf(
											"cert error example: reason=%s sql=%s err=%s",
											r.certLastErrReason,
											compactSQL(r.certLastErrSQL, 2000),
											r.certLastErr,
										)
									}
								}
								if tlpDelta, ok := deltaFunnel["TLP"]; ok && tlpDelta.Errors > 0 {
									if r.tlpLastErrSQL != "" && r.tlpLastErr != "" {
										util.Detailf(
											"tlp error example: reason=%s sql=%s err=%s",
											r.tlpLastErrReason,
											compactSQL(r.tlpLastErrSQL, 2000),
											r.tlpLastErr,
										)
									}
								}
							} else {
								for _, name := range []string{"TLP", "CERT", "GroundTruth"} {
									delta, ok := deltaFunnel[name]
									if !ok {
										continue
									}
									if len(delta.SkipReasons) > 0 {
										util.Infof("oracle_skip_reasons last interval oracle=%s top=%d: %s", name, topOracleSummaryN, formatTopJoinSigs(delta.SkipReasons, topOracleSummaryN))
									}
									if len(delta.ErrorReasons) > 0 {
										util.Infof("oracle_error_reasons last interval oracle=%s top=%d: %s", name, topOracleSummaryN, formatTopJoinSigs(delta.ErrorReasons, topOracleSummaryN))
									}
								}
							}
						}
						r.updateOracleBanditFromFunnel(deltaFunnel)
					}
					if deltaOraclePicks > 0 {
						util.Detailf(
							"cert sampling last interval: picks=%d cert=%d ratio=%.8f target=%.8f total_picks=%d total_cert=%d total_ratio=%.8f",
							deltaOraclePicks,
							deltaCertPicks,
							ratio(deltaCertPicks, deltaOraclePicks),
							certSampleRate,
							oraclePickTotal,
							certPickTotal,
							ratio(certPickTotal, oraclePickTotal),
						)
					}
					if tqsStats.Nodes > 0 {
						coveredDelta := tqsStats.Covered - lastTQSCovered
						stepsDelta := tqsStats.Steps - lastTQSSteps
						edgeDelta := tqsStats.Edges - lastTQSEdges
						util.Detailf(
							"tqs stats nodes=%d edges=%d(+%d) covered=%d(+%d) steps=%d(+%d) gamma=%.2f walk_len=%d walk_min=%d walk_max=%d",
							tqsStats.Nodes,
							tqsStats.Edges,
							edgeDelta,
							tqsStats.Covered,
							coveredDelta,
							tqsStats.Steps,
							stepsDelta,
							r.cfg.TQS.Gamma,
							r.cfg.TQS.WalkLength,
							r.cfg.TQS.WalkMin,
							r.cfg.TQS.WalkMax,
						)
						lastTQSSteps = tqsStats.Steps
						lastTQSCovered = tqsStats.Covered
						lastTQSEdges = tqsStats.Edges
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
						if r.impoLastFailSQL != "" {
							if r.impoLastFailErr != "" {
								util.Warnf("impo base_exec_failed example: %s err=%s", compactSQL(r.impoLastFailSQL, 2000), r.impoLastFailErr)
							} else {
								util.Warnf("impo base_exec_failed example: %s", compactSQL(r.impoLastFailSQL, 2000))
							}
						}
					}
					if deltaImpoTotal > 0 || deltaImpoSkips > 0 || deltaImpoTrunc > 0 {
						util.Detailf(
							"impo stats total=%d(+%d) skipped=%d(+%d) truncated=%d(+%d)",
							impoTotal,
							deltaImpoTotal,
							impoSkips,
							deltaImpoSkips,
							impoTrunc,
							deltaImpoTrunc,
						)
						util.Detailf("oracle mode current: %s", r.oracleModeLabel())
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
								util.Detailf("impo skip reasons last interval: %s", strings.Join(parts, " "))
							}
						}
						if r.cfg.Logging.Verbose && deltaImpoSkips > 0 {
							if r.impoLastFailSQL != "" {
								if r.impoLastFailErr != "" {
									util.Detailf("impo base_exec_failed example: %s err=%s", compactSQL(r.impoLastFailSQL, 2000), r.impoLastFailErr)
								} else {
									util.Detailf("impo base_exec_failed example: %s", compactSQL(r.impoLastFailSQL, 2000))
								}
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
								util.Detailf("impo base_exec_failed codes last interval: %s", strings.Join(parts, " "))
							}
						}
						if r.cfg.Logging.Verbose && deltaImpoSkips > 0 {
							lastImpoSkipReasons = impoSkipReasons
							lastImpoSkipErrCodes = impoSkipErrCodes
						}
					}
					lastImpoReasonTotals = impoSkipReasons
					lastOracleStats = oracleStats
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
						util.Detailf(
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
							util.Detailf("qpg override=%s", r.qpgState.lastOverride)
							r.qpgState.lastOverrideLogged = r.qpgState.lastOverride
						}
						if r.qpgState.lastTemplate != "" && r.qpgState.lastTemplate != r.qpgState.lastTemplateLogged {
							util.Detailf("qpg template_override=%s", r.qpgState.lastTemplate)
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

type joinSigStat struct {
	key   string
	count int64
}

func formatTopJoinSigs(stats map[string]int64, topN int) string {
	if len(stats) == 0 || topN <= 0 {
		return ""
	}
	items := make([]joinSigStat, 0, len(stats))
	for k, v := range stats {
		items = append(items, joinSigStat{key: k, count: v})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count == items[j].count {
			return items[i].key < items[j].key
		}
		return items[i].count > items[j].count
	})
	if topN > len(items) {
		topN = len(items)
	}
	parts := make([]string, 0, topN)
	for i := 0; i < topN; i++ {
		parts = append(parts, fmt.Sprintf("%s=%d", items[i].key, items[i].count))
	}
	return strings.Join(parts, " ")
}

func formatTopJoinCount(counts map[int]int64, topN int) string {
	if len(counts) == 0 || topN <= 0 {
		return ""
	}
	type joinCountStat struct {
		key   int
		count int64
	}
	items := make([]joinCountStat, 0, len(counts))
	for k, v := range counts {
		items = append(items, joinCountStat{key: k, count: v})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count == items[j].count {
			return items[i].key < items[j].key
		}
		return items[i].count > items[j].count
	})
	if topN > len(items) {
		topN = len(items)
	}
	parts := make([]string, 0, topN)
	for i := 0; i < topN; i++ {
		parts = append(parts, fmt.Sprintf("%d=%d", items[i].key, items[i].count))
	}
	return strings.Join(parts, " ")
}

func formatOracleFunnel(stats map[string]oracleFunnel) string {
	if len(stats) == 0 {
		return ""
	}
	names := make([]string, 0, len(stats))
	for name := range stats {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		stat := stats[name]
		parts = append(parts, fmt.Sprintf("%s=run/%d skip/%d err/%d mismatch/%d panic/%d report/%d", name, stat.Runs, stat.Skips, stat.Errors, stat.Mismatches, stat.Panics, stat.Reports))
	}
	return strings.Join(parts, " ")
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
