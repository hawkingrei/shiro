package runner

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"shiro/internal/db"
	"shiro/internal/oracle"
	"shiro/internal/util"
)

const maxFirstExecuteRetries = 1

func closePlanCacheConn(conn *sql.Conn) {
	util.CloseWithErr(conn, "plan cache conn")
}

func closePlanCacheStmt(stmt *sql.Stmt) {
	util.CloseWithErr(stmt, "plan cache stmt")
}

func closePlanCacheRows(rows *sql.Rows) {
	util.CloseWithErr(rows, "plan cache rows")
}

func (r *Runner) runPrepared(ctx context.Context) bool {
	pq := r.gen.GeneratePreparedQuery()
	if pq.SQL == "" {
		return false
	}
	concreteSQL := materializeSQL(pq.SQL, pq.Args)
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	conn, err := r.exec.Conn(qctx)
	if err != nil {
		return false
	}
	defer closePlanCacheConn(conn)
	if err := r.prepareConn(qctx, conn, r.cfg.Database); err != nil {
		return false
	}
	concreteSig, ok, bug := r.preparedConcreteSignature(qctx, conn, concreteSQL)
	if !ok {
		return bug
	}
	args2 := r.gen.GeneratePreparedArgsForQuery(pq.Args, pq.ArgTypes)
	concreteSig2 := concreteSig
	if !reflect.DeepEqual(args2, pq.Args) {
		sql2 := materializeSQL(pq.SQL, args2)
		concreteSig2, ok, bug = r.preparedConcreteSignature(qctx, conn, sql2)
		if !ok {
			return bug
		}
	}
	connID, err := r.connectionID(qctx, conn)
	if err != nil {
		return false
	}
	stmt, ok, bug := r.preparePlanCacheStatement(qctx, conn, pq.SQL)
	if !ok {
		return bug
	}
	defer closePlanCacheStmt(stmt)
	baseSig, hit1, hit1Unexpected, ok, bug := r.preparedFirstExecute(qctx, conn, stmt, pq.SQL, concreteSQL, connID, pq.Args, args2)
	if !ok {
		return bug
	}
	cacheRes := r.preparedCacheExecute(qctx, conn, stmt, pq.SQL, concreteSQL, connID, pq.Args, args2)
	if !cacheRes.ok {
		return cacheRes.bug
	}
	preparedSig := cacheRes.sig
	originResult := cacheRes.origin
	hit2 := cacheRes.hit
	warnings := cacheRes.warnings
	warnErr := cacheRes.warnErr
	signatureMismatch := preparedSig != concreteSig2

	hasWarnings := warnErr == nil && len(warnings) > 0
	if baseSig != concreteSig {
		result := oracle.Result{
			OK:       false,
			Oracle:   "PlanCache",
			SQL:      planCacheSQLSequence(concreteSQL, pq.SQL, pq.Args, args2, connID),
			Expected: fmt.Sprintf("cnt=%d checksum=%d", concreteSig.Count, concreteSig.Checksum),
			Actual:   fmt.Sprintf("cnt=%d checksum=%d", baseSig.Count, baseSig.Checksum),
			Details: map[string]any{
				"phase":      "first_execute",
				"replay_sql": concreteSQL,
			},
		}
		r.handleResult(ctx, result)
		return true
	}
	if hit1Unexpected {
		plan, _ := r.explainForConnection(ctx, connID)
		result := oracle.Result{
			OK:       false,
			Oracle:   "PlanCache",
			SQL:      planCacheSQLSequence(concreteSQL, pq.SQL, args2, pq.Args, connID),
			Expected: "last_plan_from_cache=0",
			Actual:   fmt.Sprintf("last_plan_from_cache=%d", hit1),
			Details: map[string]any{
				"origin_result":          originResult,
				"explain_for_connection": plan,
				"replay_sql":             concreteSQL,
			},
		}
		r.handleResult(ctx, result)
		return true
	}
	if signatureMismatch && !hasWarnings {
		result := oracle.Result{
			OK:       false,
			Oracle:   "PlanCache",
			SQL:      planCacheSQLSequence(concreteSQL, pq.SQL, pq.Args, args2, connID),
			Expected: fmt.Sprintf("cnt=%d checksum=%d", concreteSig2.Count, concreteSig2.Checksum),
			Actual:   fmt.Sprintf("cnt=%d checksum=%d", preparedSig.Count, preparedSig.Checksum),
			Details: map[string]any{
				"origin_result": originResult,
				"warnings":      warnings,
				"warnings_err":  warnErr,
				"replay_sql":    concreteSQL,
			},
		}
		r.handleResult(ctx, result)
		return true
	}
	if hit2 == 1 || hasWarnings {
		return false
	}
	plan, _ := r.explainForConnection(ctx, connID)
	result := oracle.Result{
		OK:       false,
		Oracle:   "PlanCache",
		SQL:      planCacheSQLSequence(concreteSQL, pq.SQL, pq.Args, args2, connID),
		Expected: "last_plan_from_cache=1",
		Actual:   "last_plan_from_cache=0",
		Details: map[string]any{
			"origin_result":          originResult,
			"warnings":               warnings,
			"warnings_err":           warnErr,
			"explain_for_connection": plan,
			"replay_sql":             concreteSQL,
		},
	}
	r.handleResult(ctx, result)
	return true
}

func (r *Runner) preparedConcreteSignature(ctx context.Context, conn *sql.Conn, concreteSQL string) (sig db.Signature, ok bool, bug bool) {
	sig, err := r.signatureForSQLOnConn(ctx, conn, concreteSQL, r.planCacheRoundScale())
	if err != nil {
		if logWhitelistedSQLError(concreteSQL, err, r.cfg.Logging.Verbose) {
			return db.Signature{}, false, false
		}
		if isMySQLError(err) && !isPanicError(err) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCache",
				SQL:    []string{concreteSQL},
				Err:    err,
			}
			r.handleResult(ctx, result)
			return db.Signature{}, false, true
		}
		return db.Signature{}, false, false
	}
	return sig, true, false
}

func (r *Runner) preparePlanCacheStatement(ctx context.Context, conn *sql.Conn, sql string) (stmt *sql.Stmt, ok bool, bug bool) {
	stmt, err := conn.PrepareContext(ctx, sql)
	if err != nil {
		if logWhitelistedSQLError(sql, err, r.cfg.Logging.Verbose) {
			return nil, false, false
		}
		if isMySQLError(err) && !isPanicError(err) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCache",
				SQL:    []string{sql},
				Err:    err,
			}
			r.handleResult(ctx, result)
			return nil, false, true
		}
		return nil, false, false
	}
	return stmt, true, false
}

func (r *Runner) preparedFirstExecute(ctx context.Context, conn *sql.Conn, stmt *sql.Stmt, preparedSQL, concreteSQL string, connID int64, argsFirst []any, argsSecond []any) (sig db.Signature, hit int, hitUnexpected bool, ok bool, bug bool) {
	for attempt := 0; attempt < maxFirstExecuteRetries; attempt++ {
		rowsBase, err := stmt.QueryContext(ctx, argsFirst...)
		if err != nil {
			if logWhitelistedSQLError(preparedSQL, err, r.cfg.Logging.Verbose) {
				return db.Signature{}, 0, false, false, false
			}
			if isMySQLError(err) && !isPanicError(err) {
				result := oracle.Result{
					OK:     false,
					Oracle: "PlanCache",
					SQL:    planCacheSQLSequence(concreteSQL, preparedSQL, argsFirst, argsSecond, connID),
					Err:    err,
					Details: map[string]any{
						"replay_sql": concreteSQL,
					},
				}
				r.handleResult(ctx, result)
				return db.Signature{}, 0, false, false, true
			}
			if isPanicError(err) {
				result := oracle.Result{
					OK:     false,
					Oracle: "PlanCache",
					SQL:    []string{concreteSQL, preparedSQL},
					Err:    err,
					Details: map[string]any{
						"replay_sql": concreteSQL,
					},
				}
				r.handleResult(ctx, result)
				return db.Signature{}, 0, false, false, true
			}
			return db.Signature{}, 0, false, false, false
		}
		baseSig, err := signatureFromRows(rowsBase, r.planCacheRoundScale())
		closePlanCacheRows(rowsBase)
		if err != nil {
			return db.Signature{}, 0, false, false, false
		}
		// Capture hit info right after the first EXECUTE to avoid later SELECTs overwriting it.
		hit1, err := r.lastPlanFromCache(ctx, conn)
		if err != nil {
			return db.Signature{}, 0, false, false, false
		}
		// Run the statement again before SHOW WARNINGS so warnings are bound to the EXECUTE,
		// not to the SELECT @@last_plan_from_cache.
		rowsWarn, err := stmt.QueryContext(ctx, argsFirst...)
		if err == nil {
			_ = drainRows(rowsWarn)
			closePlanCacheRows(rowsWarn)
		} else if isPanicError(err) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCache",
				SQL:    planCacheSQLSequence(concreteSQL, preparedSQL, argsFirst, argsSecond, connID),
				Err:    err,
				Details: map[string]any{
					"replay_sql": concreteSQL,
				},
			}
			r.handleResult(ctx, result)
			return db.Signature{}, 0, false, false, true
		}
		warnings, warnErr := r.warningsOnConn(ctx, conn)
		if warnErr != nil {
			return db.Signature{}, 0, false, false, false
		}
		if len(warnings) == 0 {
			return baseSig, hit1, hit1 == 1, true, false
		}
	}
	return db.Signature{}, 0, false, false, false
}

type preparedCacheResult struct {
	sig      db.Signature
	origin   map[string]any
	hit      int
	warnings []string
	warnErr  error
	ok       bool
	bug      bool
}

func (r *Runner) preparedCacheExecute(ctx context.Context, conn *sql.Conn, stmt *sql.Stmt, preparedSQL, concreteSQL string, connID int64, argsFirst []any, argsSecond []any) preparedCacheResult {
	rows2, err := stmt.QueryContext(ctx, argsSecond...)
	if err != nil {
		if logWhitelistedSQLError(preparedSQL, err, r.cfg.Logging.Verbose) {
			return preparedCacheResult{}
		}
		if isMySQLError(err) && !isPanicError(err) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCache",
				SQL:    planCacheSQLSequence(concreteSQL, preparedSQL, argsFirst, argsSecond, connID),
				Err:    err,
				Details: map[string]any{
					"replay_sql": concreteSQL,
				},
			}
			r.handleResult(ctx, result)
			return preparedCacheResult{bug: true}
		}
		if isPanicError(err) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCache",
				SQL:    []string{concreteSQL, preparedSQL},
				Err:    err,
				Details: map[string]any{
					"replay_sql": concreteSQL,
				},
			}
			r.handleResult(ctx, result)
			return preparedCacheResult{bug: true}
		}
		return preparedCacheResult{}
	}
	preparedSig, originCols, originRows, err := signatureAndSampleFromRows(rows2, originSampleLimit, r.planCacheRoundScale())
	closePlanCacheRows(rows2)
	if err != nil {
		return preparedCacheResult{}
	}
	originResult := map[string]any{
		"signature": fmt.Sprintf("cnt=%d checksum=%d", preparedSig.Count, preparedSig.Checksum),
		"columns":   originCols,
		"rows":      originRows,
	}
	hit2, err := r.lastPlanFromCache(ctx, conn)
	if err != nil {
		return preparedCacheResult{}
	}
	rowsWarn, err := stmt.QueryContext(ctx, argsSecond...)
	if err == nil {
		_ = drainRows(rowsWarn)
		closePlanCacheRows(rowsWarn)
	} else if isPanicError(err) {
		result := oracle.Result{
			OK:     false,
			Oracle: "PlanCache",
			SQL:    []string{concreteSQL, preparedSQL},
			Err:    err,
			Details: map[string]any{
				"replay_sql": concreteSQL,
			},
		}
		r.handleResult(ctx, result)
		return preparedCacheResult{bug: true}
	}
	warnings, warnErr := r.warningsOnConn(ctx, conn)
	rowsExplain, err := stmt.QueryContext(ctx, argsSecond...)
	if err == nil {
		_ = drainRows(rowsExplain)
		closePlanCacheRows(rowsExplain)
		r.observePlanForConnection(ctx, connID)
	} else if isPanicError(err) {
		result := oracle.Result{
			OK:     false,
			Oracle: "PlanCache",
			SQL:    []string{concreteSQL, preparedSQL},
			Err:    err,
			Details: map[string]any{
				"replay_sql": concreteSQL,
			},
		}
		r.handleResult(ctx, result)
		return preparedCacheResult{bug: true}
	}
	return preparedCacheResult{
		sig:      preparedSig,
		origin:   originResult,
		hit:      hit2,
		warnings: warnings,
		warnErr:  warnErr,
		ok:       true,
	}
}

func (r *Runner) runPlanCacheOnly(ctx context.Context) error {
	var total int
	var invalid int
	var execErrors int
	var hitSecond int
	var missSecond int
	var missSecondWithWarnings int
	var firstSkipWithWarnings int
	var hitFirstUnexpected int
	warningReasonCounts := make(map[string]int)
nextIteration:
	for i := 0; i < r.cfg.Iterations; i++ {
		total++
		conn, err := r.exec.Conn(ctx)
		if err != nil {
			return err
		}
		connID, err := r.connectionID(ctx, conn)
		if err != nil {
			closePlanCacheConn(conn)
			continue
		}
		if err := r.prepareConn(ctx, conn, r.cfg.Database); err != nil {
			closePlanCacheConn(conn)
			return err
		}
		pq := r.gen.GeneratePreparedQuery()
		if pq.SQL == "" {
			closePlanCacheConn(conn)
			continue
		}
		if err := r.validator.Validate(pq.SQL); err != nil {
			r.observeSQL(pq.SQL, err)
			invalid++
			closePlanCacheConn(conn)
			continue
		}
		r.observeSQL(pq.SQL, nil)
		concreteSQL := materializeSQL(pq.SQL, pq.Args)

		concreteSig, sigErr2 := r.signatureForSQLOnConn(ctx, conn, concreteSQL, r.planCacheRoundScale())
		if sigErr2 != nil && logWhitelistedSQLError(concreteSQL, sigErr2, r.cfg.Logging.Verbose) {
			closePlanCacheConn(conn)
			continue
		}
		if sigErr2 != nil && isMySQLError(sigErr2) && !isPanicError(sigErr2) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCacheOnly",
				SQL:    []string{concreteSQL},
				Err:    sigErr2,
			}
			r.handleResult(ctx, result)
			closePlanCacheConn(conn)
			continue
		}

		stmt, err := conn.PrepareContext(ctx, pq.SQL)
		if err != nil {
			if logWhitelistedSQLError(pq.SQL, err, r.cfg.Logging.Verbose) {
				closePlanCacheConn(conn)
				continue
			}
			if isMySQLError(err) && !isPanicError(err) {
				result := oracle.Result{
					OK:     false,
					Oracle: "PlanCacheOnly",
					SQL:    []string{pq.SQL},
					Err:    err,
				}
				r.handleResult(ctx, result)
				closePlanCacheConn(conn)
				continue
			}
			closePlanCacheConn(conn)
			continue
		}
		var args2 []any
		acceptedFirst := false
		for attempt := 0; attempt < maxFirstExecuteRetries; attempt++ {
			args2 = r.gen.GeneratePreparedArgsForQuery(pq.Args, pq.ArgTypes)
			rows1, err := stmt.QueryContext(ctx, args2...)
			if err != nil {
				if logWhitelistedSQLError(pq.SQL, err, r.cfg.Logging.Verbose) {
					closePlanCacheStmt(stmt)
					closePlanCacheConn(conn)
					continue nextIteration
				}
				if isMySQLError(err) && !isPanicError(err) {
					result := oracle.Result{
						OK:     false,
						Oracle: "PlanCacheOnly",
						SQL:    planCacheSQLSequence(concreteSQL, pq.SQL, args2, pq.Args, connID),
						Err:    err,
						Details: map[string]any{
							"replay_sql": concreteSQL,
						},
					}
					r.handleResult(ctx, result)
					closePlanCacheStmt(stmt)
					closePlanCacheConn(conn)
					continue nextIteration
				}
				warnings, warnErr := r.warningsOnConn(ctx, conn)
				closePlanCacheStmt(stmt)
				closePlanCacheConn(conn)
				execErrors++
				if isPanicError(err) {
					plan, _ := r.explainForConnection(ctx, connID)
					result := oracle.Result{
						OK:     false,
						Oracle: "PlanCacheOnly",
						SQL:    []string{pq.SQL, fmt.Sprintf("EXPLAIN FOR CONNECTION %d", connID)},
						Err:    err,
						Details: map[string]any{
							"warnings":               warnings,
							"warnings_err":           warnErr,
							"explain_for_connection": plan,
							"replay_sql":             concreteSQL,
						},
					}
					r.handleResult(ctx, result)
				}
				continue nextIteration
			}
			if _, err := signatureFromRows(rows1, r.planCacheRoundScale()); err != nil {
				closePlanCacheRows(rows1)
				closePlanCacheStmt(stmt)
				closePlanCacheConn(conn)
				continue nextIteration
			}
			closePlanCacheRows(rows1)
			warnings, warnErr := r.warningsOnConn(ctx, conn)
			if warnErr != nil {
				closePlanCacheStmt(stmt)
				closePlanCacheConn(conn)
				continue nextIteration
			}
			if len(warnings) == 0 {
				acceptedFirst = true
				break
			}
			firstSkipWithWarnings++
			observePlanCacheWarnings(warningReasonCounts, warnings)
		}
		if !acceptedFirst {
			closePlanCacheStmt(stmt)
			closePlanCacheConn(conn)
			continue
		}

		hit1, err := r.lastPlanFromCache(ctx, conn)
		hit1Unexpected := err == nil && hit1 == 1
		if hit1Unexpected {
			hitFirstUnexpected++
		}

		rows2, err := stmt.QueryContext(ctx, pq.Args...)
		if err != nil {
			if logWhitelistedSQLError(pq.SQL, err, r.cfg.Logging.Verbose) {
				closePlanCacheStmt(stmt)
				closePlanCacheConn(conn)
				continue
			}
			if isMySQLError(err) && !isPanicError(err) {
				result := oracle.Result{
					OK:     false,
					Oracle: "PlanCacheOnly",
					SQL:    planCacheSQLSequence(concreteSQL, pq.SQL, args2, pq.Args, connID),
					Err:    err,
					Details: map[string]any{
						"replay_sql": concreteSQL,
					},
				}
				r.handleResult(ctx, result)
				closePlanCacheStmt(stmt)
				closePlanCacheConn(conn)
				continue
			}
			warnings, warnErr := r.warningsOnConn(ctx, conn)
			closePlanCacheStmt(stmt)
			closePlanCacheConn(conn)
			execErrors++
			if isPanicError(err) {
				plan, _ := r.explainForConnection(ctx, connID)
				result := oracle.Result{
					OK:     false,
					Oracle: "PlanCacheOnly",
					SQL:    []string{pq.SQL, fmt.Sprintf("EXPLAIN FOR CONNECTION %d", connID)},
					Err:    err,
					Details: map[string]any{
						"warnings":               warnings,
						"warnings_err":           warnErr,
						"explain_for_connection": plan,
						"replay_sql":             concreteSQL,
					},
				}
				r.handleResult(ctx, result)
			}
			continue
		}
		preparedSig, originCols, originRows, sigErr := signatureAndSampleFromRows(rows2, originSampleLimit, r.planCacheRoundScale())
		closePlanCacheRows(rows2)
		if sigErr != nil {
			closePlanCacheStmt(stmt)
			closePlanCacheConn(conn)
			continue
		}
		originResult := map[string]any{
			"signature": fmt.Sprintf("cnt=%d checksum=%d", preparedSig.Count, preparedSig.Checksum),
			"columns":   originCols,
			"rows":      originRows,
		}
		signatureMismatch := sigErr == nil && sigErr2 == nil && preparedSig != concreteSig

		hit2, err := r.lastPlanFromCache(ctx, conn)
		if err != nil {
			closePlanCacheStmt(stmt)
			closePlanCacheConn(conn)
			continue
		}
		rowsWarn, err := stmt.QueryContext(ctx, pq.Args...)
		if err == nil {
			_ = drainRows(rowsWarn)
			closePlanCacheRows(rowsWarn)
		} else if isPanicError(err) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCacheOnly",
				SQL:    []string{pq.SQL, fmt.Sprintf("EXPLAIN FOR CONNECTION %d", connID)},
				Err:    err,
				Details: map[string]any{
					"replay_sql": concreteSQL,
				},
			}
			r.handleResult(ctx, result)
			closePlanCacheStmt(stmt)
			closePlanCacheConn(conn)
			continue
		}

		warnings, warnErr := r.warningsOnConn(ctx, conn)
		if signatureMismatch && warnErr == nil && len(warnings) == 0 {
			result := oracle.Result{
				OK:       false,
				Oracle:   "PlanCacheOnly",
				SQL:      planCacheSQLSequence(concreteSQL, pq.SQL, args2, pq.Args, connID),
				Expected: fmt.Sprintf("cnt=%d checksum=%d", concreteSig.Count, concreteSig.Checksum),
				Actual:   fmt.Sprintf("cnt=%d checksum=%d", preparedSig.Count, preparedSig.Checksum),
				Details: map[string]any{
					"origin_result": originResult,
					"hit_first":     hit1,
					"hit_second":    hit2,
					"replay_sql":    concreteSQL,
				},
			}
			r.handleResult(ctx, result)
		}

		rowsExplain, err := stmt.QueryContext(ctx, pq.Args...)
		if err == nil && (hit2 == 1 || (warnErr == nil && len(warnings) > 0)) {
			_ = drainRows(rowsExplain)
			closePlanCacheRows(rowsExplain)
			r.observePlanForConnection(ctx, connID)
		} else if err == nil {
			_ = drainRows(rowsExplain)
			closePlanCacheRows(rowsExplain)
		} else if isPanicError(err) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCacheOnly",
				SQL:    []string{pq.SQL, fmt.Sprintf("EXPLAIN FOR CONNECTION %d", connID)},
				Err:    err,
				Details: map[string]any{
					"replay_sql": concreteSQL,
				},
			}
			r.handleResult(ctx, result)
			closePlanCacheStmt(stmt)
			closePlanCacheConn(conn)
			continue
		}
		closePlanCacheStmt(stmt)

		if hit1Unexpected {
			plan, _ := r.explainForConnection(ctx, connID)
			result := oracle.Result{
				OK:       false,
				Oracle:   "PlanCacheOnly",
				SQL:      planCacheSQLSequence(concreteSQL, pq.SQL, args2, pq.Args, connID),
				Expected: "last_plan_from_cache=0",
				Actual:   fmt.Sprintf("last_plan_from_cache=%d", hit1),
				Details: map[string]any{
					"origin_result":          originResult,
					"hit_first":              hit1,
					"hit_second":             hit2,
					"warnings":               warnings,
					"warnings_err":           warnErr,
					"explain_for_connection": plan,
					"replay_sql":             concreteSQL,
				},
			}
			r.handleResult(ctx, result)
		}

		if hit2 != 1 {
			hasWarnings := warnErr == nil && len(warnings) > 0
			missSecond++
			if hasWarnings && r.cfg.Logging.Verbose {
				util.Infof("plan_cache_only miss with warnings: %s", strings.Join(warnings, " | "))
			}
			if hasWarnings {
				missSecondWithWarnings++
				observePlanCacheWarnings(warningReasonCounts, warnings)
			}
			if !hasWarnings {
				plan, _ := r.explainForConnection(ctx, connID)
				result := oracle.Result{
					OK:       false,
					Oracle:   "PlanCacheOnly",
					SQL:      planCacheSQLSequence(concreteSQL, pq.SQL, args2, pq.Args, connID),
					Expected: "last_plan_from_cache=1",
					Actual:   "last_plan_from_cache=0",
					Details: map[string]any{
						"args_first":             formatArgs(pq.Args),
						"args_second":            formatArgs(args2),
						"origin_result":          originResult,
						"hit_first":              hit1,
						"hit_second":             hit2,
						"miss_without_warnings":  true,
						"warnings":               warnings,
						"warnings_err":           warnErr,
						"explain_for_connection": plan,
						"replay_sql":             concreteSQL,
					},
				}
				r.handleResult(ctx, result)
			}
		} else {
			hitSecond++
		}
		closePlanCacheConn(conn)
	}
	util.Infof(
		"plan_cache_only stats total=%d invalid=%d exec_errors=%d hit_first_unexpected=%d hit_second=%d miss_second=%d miss_second_with_warnings=%d first_skip_with_warnings=%d warning_reasons=%s",
		total,
		invalid,
		execErrors,
		hitFirstUnexpected,
		hitSecond,
		missSecond,
		missSecondWithWarnings,
		firstSkipWithWarnings,
		formatPlanCacheWarningReasons(warningReasonCounts),
	)
	return nil
}

func (r *Runner) lastPlanFromCache(ctx context.Context, conn *sql.Conn) (int, error) {
	row := conn.QueryRowContext(ctx, "SELECT @@last_plan_from_cache")
	var v int
	if err := row.Scan(&v); err != nil {
		return 0, err
	}
	return v, nil
}

func (r *Runner) connectionID(ctx context.Context, conn *sql.Conn) (int64, error) {
	row := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()")
	var v int64
	if err := row.Scan(&v); err != nil {
		return 0, err
	}
	return v, nil
}

func (r *Runner) explainForConnection(ctx context.Context, connID int64) (string, error) {
	query := fmt.Sprintf("EXPLAIN FOR CONNECTION %d", connID)
	rows, err := r.exec.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer closePlanCacheRows(rows)

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	values := make([][]byte, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var b strings.Builder
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return "", err
		}
		for i, v := range values {
			if i > 0 {
				b.WriteByte('\t')
			}
			if v == nil {
				b.WriteString("NULL")
			} else {
				b.Write(v)
			}
		}
		b.WriteByte('\n')
	}
	return b.String(), nil
}

func (r *Runner) warningsOnConn(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		return nil, err
	}
	defer closePlanCacheRows(rows)
	var warnings []string
	for rows.Next() {
		var level string
		var code int
		var msg string
		if err := rows.Scan(&level, &code, &msg); err != nil {
			return nil, err
		}
		warnings = append(warnings, fmt.Sprintf("%s:%d:%s", level, code, msg))
	}
	return warnings, rows.Err()
}

func planCacheWarningReason(warning string) string {
	parts := strings.SplitN(warning, ":", 3)
	msg := warning
	if len(parts) == 3 {
		msg = strings.TrimSpace(parts[2])
	}
	msg = strings.ToLower(strings.TrimSpace(msg))
	msg = strings.TrimSpace(strings.TrimPrefix(msg, "skip plan-cache:"))
	msg = strings.TrimSpace(strings.TrimPrefix(msg, "skip non-prepared plan-cache:"))
	if msg == "" {
		return "unknown"
	}
	return msg
}

func observePlanCacheWarnings(reasonCounts map[string]int, warnings []string) {
	if reasonCounts == nil || len(warnings) == 0 {
		return
	}
	seen := make(map[string]struct{}, len(warnings))
	for _, warning := range warnings {
		reason := planCacheWarningReason(warning)
		if _, ok := seen[reason]; ok {
			continue
		}
		seen[reason] = struct{}{}
		reasonCounts[reason]++
	}
}

func formatPlanCacheWarningReasons(reasonCounts map[string]int) string {
	if len(reasonCounts) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(reasonCounts))
	for reason := range reasonCounts {
		keys = append(keys, reason)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, reason := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", reason, reasonCounts[reason]))
	}
	return strings.Join(parts, ",")
}

func planCacheSQLSequence(concreteSQL, preparedSQL string, firstArgs []any, baseArgs []any, connID int64) []string {
	capacity := 8 + len(firstArgs) + 3*len(baseArgs)
	seq := make([]string, 0, capacity)
	seq = append(seq, concreteSQL, formatPrepareSQL(preparedSQL))
	seq = append(seq, formatExecuteSQLWithVars("stmt", firstArgs)...)
	seq = append(seq, "SELECT @@last_plan_from_cache")
	seq = append(seq, formatExecuteSQLWithVars("stmt", baseArgs)...)
	seq = append(seq, "SELECT @@last_plan_from_cache")
	seq = append(seq, formatExecuteSQLWithVars("stmt", baseArgs)...)
	seq = append(seq, "SHOW WARNINGS")
	seq = append(seq, formatExecuteSQLWithVars("stmt", baseArgs)...)
	seq = append(seq, fmt.Sprintf("EXPLAIN FOR CONNECTION %d", connID))
	return seq
}
