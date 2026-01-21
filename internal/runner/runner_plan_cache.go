package runner

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"shiro/internal/db"
	"shiro/internal/oracle"
	"shiro/internal/util"
)

const maxFirstExecuteRetries = 1

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
	defer conn.Close()
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
	defer stmt.Close()
	baseSig, hit1, hit1Unexpected, ok, bug := r.preparedFirstExecute(qctx, conn, stmt, pq.SQL, concreteSQL, connID, pq.Args, args2)
	if !ok {
		return bug
	}
	preparedSig, originResult, hit2, warnings, warnErr, ok, bug := r.preparedCacheExecute(qctx, conn, stmt, pq.SQL, concreteSQL, connID, pq.Args, args2)
	if !ok {
		return bug
	}
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
	return false
}

func (r *Runner) preparedConcreteSignature(ctx context.Context, conn *sql.Conn, concreteSQL string) (db.Signature, bool, bool) {
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

func (r *Runner) preparePlanCacheStatement(ctx context.Context, conn *sql.Conn, sql string) (*sql.Stmt, bool, bool) {
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

func (r *Runner) preparedFirstExecute(ctx context.Context, conn *sql.Conn, stmt *sql.Stmt, preparedSQL, concreteSQL string, connID int64, argsFirst []any, argsSecond []any) (db.Signature, int, bool, bool, bool) {
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
		rowsBase.Close()
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
			rowsWarn.Close()
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

func (r *Runner) preparedCacheExecute(ctx context.Context, conn *sql.Conn, stmt *sql.Stmt, preparedSQL, concreteSQL string, connID int64, argsFirst []any, argsSecond []any) (db.Signature, map[string]any, int, []string, error, bool, bool) {
	rows2, err := stmt.QueryContext(ctx, argsSecond...)
	if err != nil {
		if logWhitelistedSQLError(preparedSQL, err, r.cfg.Logging.Verbose) {
			return db.Signature{}, nil, 0, nil, nil, false, false
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
			return db.Signature{}, nil, 0, nil, nil, false, true
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
			return db.Signature{}, nil, 0, nil, nil, false, true
		}
		return db.Signature{}, nil, 0, nil, nil, false, false
	}
	preparedSig, originCols, originRows, err := signatureAndSampleFromRows(rows2, originSampleLimit, r.planCacheRoundScale())
	rows2.Close()
	if err != nil {
		return db.Signature{}, nil, 0, nil, nil, false, false
	}
	originResult := map[string]any{
		"signature": fmt.Sprintf("cnt=%d checksum=%d", preparedSig.Count, preparedSig.Checksum),
		"columns":   originCols,
		"rows":      originRows,
	}
	hit2, err := r.lastPlanFromCache(ctx, conn)
	if err != nil {
		return db.Signature{}, nil, 0, nil, nil, false, false
	}
	rowsWarn, err := stmt.QueryContext(ctx, argsSecond...)
	if err == nil {
		_ = drainRows(rowsWarn)
		rowsWarn.Close()
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
		return db.Signature{}, nil, 0, nil, nil, false, true
	}
	warnings, warnErr := r.warningsOnConn(ctx, conn)
	rowsExplain, err := stmt.QueryContext(ctx, argsSecond...)
	if err == nil {
		_ = drainRows(rowsExplain)
		rowsExplain.Close()
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
		return db.Signature{}, nil, 0, nil, nil, false, true
	}
	return preparedSig, originResult, hit2, warnings, warnErr, true, false
}

func (r *Runner) runNonPreparedPlanCache(ctx context.Context) (bool, bool) {
	pq := r.gen.GenerateNonPreparedPlanCacheQuery()
	if pq.SQL == "" {
		return false, false
	}
	sql1 := materializeSQL(pq.SQL, pq.Args)
	args2 := r.gen.GeneratePreparedArgsForQuery(pq.Args, pq.ArgTypes)
	sql2 := materializeSQL(pq.SQL, args2)

	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	conn, err := r.exec.Conn(qctx)
	if err != nil {
		return false, false
	}
	defer conn.Close()
	if err := r.prepareConn(qctx, conn, r.cfg.Database); err != nil {
		return false, false
	}

	_ = r.execOnConn(qctx, conn, "SET SESSION tidb_enable_non_prepared_plan_cache = 1")
	rows1, err := conn.QueryContext(qctx, sql1)
	if err != nil {
		if logWhitelistedSQLError(sql1, err, r.cfg.Logging.Verbose) {
			return true, false
		}
		if isMySQLError(err) && !isPanicError(err) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCacheNonPrepared",
				SQL:    planCacheNonPreparedSQLSequence(sql1, sql2),
				Err:    err,
				Details: map[string]any{
					"replay_sql": sql1,
				},
			}
			r.handleResult(ctx, result)
			return true, true
		}
		return true, false
	}
	if _, err := signatureFromRows(rows1, r.planCacheRoundScale()); err != nil {
		rows1.Close()
		return true, false
	}
	rows1.Close()

	rows2, err := conn.QueryContext(qctx, sql2)
	if err != nil {
		if logWhitelistedSQLError(sql2, err, r.cfg.Logging.Verbose) {
			return true, false
		}
		if isMySQLError(err) && !isPanicError(err) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCacheNonPrepared",
				SQL:    planCacheNonPreparedSQLSequence(sql1, sql2),
				Err:    err,
				Details: map[string]any{
					"replay_sql": sql2,
				},
			}
			r.handleResult(ctx, result)
			return true, true
		}
		return true, false
	}
	cacheSig, originCols, originRows, err := signatureAndSampleFromRows(rows2, originSampleLimit, r.planCacheRoundScale())
	rows2.Close()
	if err != nil {
		return true, false
	}
	originResult := map[string]any{
		"signature": fmt.Sprintf("cnt=%d checksum=%d", cacheSig.Count, cacheSig.Checksum),
		"columns":   originCols,
		"rows":      originRows,
	}
	hit2, err := r.lastPlanFromCache(qctx, conn)
	if err != nil {
		return true, false
	}
	rowsWarn, err := conn.QueryContext(qctx, sql2)
	if err == nil {
		_ = drainRows(rowsWarn)
		rowsWarn.Close()
	} else if isPanicError(err) {
		result := oracle.Result{
			OK:     false,
			Oracle: "PlanCacheNonPrepared",
			SQL:    planCacheNonPreparedSQLSequence(sql1, sql2),
			Err:    err,
			Details: map[string]any{
				"replay_sql": sql2,
			},
		}
		r.handleResult(ctx, result)
		return true, true
	}
	warnings, warnErr := r.warningsOnConn(qctx, conn)
	_ = r.execOnConn(qctx, conn, "SET SESSION tidb_enable_non_prepared_plan_cache = 0")
	baselineSig, err := r.signatureForSQLOnConn(qctx, conn, sql2, r.planCacheRoundScale())
	if err != nil {
		if logWhitelistedSQLError(sql2, err, r.cfg.Logging.Verbose) {
			return true, false
		}
		if isMySQLError(err) && !isPanicError(err) {
			result := oracle.Result{
				OK:     false,
				Oracle: "PlanCacheNonPrepared",
				SQL:    []string{sql2},
				Err:    err,
			}
			r.handleResult(ctx, result)
			return true, true
		}
		return true, false
	}
	signatureMismatch := cacheSig != baselineSig

	if signatureMismatch {
		expected := fmt.Sprintf("cnt=%d checksum=%d", baselineSig.Count, baselineSig.Checksum)
		actual := fmt.Sprintf("cnt=%d checksum=%d", cacheSig.Count, cacheSig.Checksum)
		result := oracle.Result{
			OK:       false,
			Oracle:   "PlanCacheNonPrepared",
			SQL:      planCacheNonPreparedSQLSequence(sql1, sql2),
			Expected: expected,
			Actual:   actual,
			Details: map[string]any{
				"origin_result": originResult,
				"warnings":      warnings,
				"warnings_err":  warnErr,
				"hit_second":    hit2,
				"replay_sql":    sql2,
			},
		}
		r.handleResult(ctx, result)
		return true, true
	}
	return true, false
}

func planCacheNonPreparedSQLSequence(sql1, sql2 string) []string {
	return []string{
		"SET SESSION tidb_enable_non_prepared_plan_cache = 1",
		sql1,
		sql2,
		"SELECT @@last_plan_from_cache",
		sql2,
		"SHOW WARNINGS",
		"SET SESSION tidb_enable_non_prepared_plan_cache = 0",
		sql2,
	}
}

func (r *Runner) runPlanCacheOnly(ctx context.Context) error {
	var total int
	var invalid int
	var execErrors int
	var hitSecond int
	var missSecond int
	var hitFirstUnexpected int
nextIteration:
	for i := 0; i < r.cfg.Iterations; i++ {
		total++
		conn, err := r.exec.Conn(ctx)
		if err != nil {
			return err
		}
		connID, err := r.connectionID(ctx, conn)
		if err != nil {
			conn.Close()
			continue
		}
		if err := r.prepareConn(ctx, conn, r.cfg.Database); err != nil {
			conn.Close()
			return err
		}
		_ = r.execOnConn(ctx, conn, "SET SESSION tidb_enable_prepared_plan_cache = 1")

		pq := r.gen.GeneratePreparedQuery()
		if pq.SQL == "" {
			conn.Close()
			continue
		}
		if err := r.validator.Validate(pq.SQL); err != nil {
			r.observeSQL(pq.SQL, err)
			invalid++
			conn.Close()
			continue
		}
		r.observeSQL(pq.SQL, nil)
		concreteSQL := materializeSQL(pq.SQL, pq.Args)

		concreteSig, sigErr2 := r.signatureForSQLOnConn(ctx, conn, concreteSQL, r.planCacheRoundScale())
		if sigErr2 != nil && logWhitelistedSQLError(concreteSQL, sigErr2, r.cfg.Logging.Verbose) {
			conn.Close()
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
			conn.Close()
			continue
		}

		stmt, err := conn.PrepareContext(ctx, pq.SQL)
		if err != nil {
			if logWhitelistedSQLError(pq.SQL, err, r.cfg.Logging.Verbose) {
				conn.Close()
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
				conn.Close()
				continue
			}
			conn.Close()
			continue
		}
		var args2 []any
		acceptedFirst := false
		for attempt := 0; attempt < maxFirstExecuteRetries; attempt++ {
			args2 = r.gen.GeneratePreparedArgsForQuery(pq.Args, pq.ArgTypes)
			rows1, err := stmt.QueryContext(ctx, args2...)
			if err != nil {
				if logWhitelistedSQLError(pq.SQL, err, r.cfg.Logging.Verbose) {
					stmt.Close()
					conn.Close()
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
					stmt.Close()
					conn.Close()
					continue nextIteration
				}
				warnings, warnErr := r.warningsOnConn(ctx, conn)
				stmt.Close()
				conn.Close()
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
				rows1.Close()
				stmt.Close()
				conn.Close()
				continue nextIteration
			}
			rows1.Close()
			warnings, warnErr := r.warningsOnConn(ctx, conn)
			if warnErr != nil {
				stmt.Close()
				conn.Close()
				continue nextIteration
			}
			if len(warnings) == 0 {
				acceptedFirst = true
				break
			}
		}
		if !acceptedFirst {
			stmt.Close()
			conn.Close()
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
				stmt.Close()
				conn.Close()
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
				stmt.Close()
				conn.Close()
				continue
			}
			warnings, warnErr := r.warningsOnConn(ctx, conn)
			stmt.Close()
			conn.Close()
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
		rows2.Close()
		if sigErr != nil {
			stmt.Close()
			conn.Close()
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
			stmt.Close()
			conn.Close()
			continue
		}
		rowsWarn, err := stmt.QueryContext(ctx, pq.Args...)
		if err == nil {
			_ = drainRows(rowsWarn)
			rowsWarn.Close()
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
			stmt.Close()
			conn.Close()
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
			rowsExplain.Close()
			r.observePlanForConnection(ctx, connID)
		} else if err == nil {
			_ = drainRows(rowsExplain)
			rowsExplain.Close()
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
			stmt.Close()
			conn.Close()
			continue
		}
		stmt.Close()

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
		conn.Close()
	}
	util.Infof("plan_cache_only stats total=%d invalid=%d exec_errors=%d hit_first_unexpected=%d hit_second=%d miss_second=%d", total, invalid, execErrors, hitFirstUnexpected, hitSecond, missSecond)
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
	defer rows.Close()

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
	defer rows.Close()
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

func planCacheSQLSequence(concreteSQL, preparedSQL string, firstArgs []any, baseArgs []any, connID int64) []string {
	seq := []string{concreteSQL, formatPrepareSQL(preparedSQL)}
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
