package runner

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"shiro/internal/config"
	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/oracle"
	"shiro/internal/replayer"
	"shiro/internal/report"
	"shiro/internal/schema"
	"shiro/internal/uploader"
	"shiro/internal/util"
	"shiro/internal/validator"
)

// Runner orchestrates fuzzing, execution, and reporting.
type Runner struct {
	cfg       config.Config
	exec      *db.DB
	gen       *generator.Generator
	state     *schema.State
	baseDB    string
	validator *validator.Validator
	reporter  *report.Reporter
	replayer  *replayer.Replayer
	uploader  uploader.Uploader
	oracles   []oracle.Oracle
	insertLog []string
	statsMu   sync.Mutex
	sqlTotal  int64
	sqlValid  int64
	sqlExists int64
	sqlNotEx  int64
	sqlIn     int64
	sqlNotIn  int64
	qpgState  *qpgState

	actionBandit  *util.Bandit
	oracleBandit  *util.Bandit
	dmlBandit     *util.Bandit
	actionEnabled []bool
	oracleEnabled []bool
	dmlEnabled    []bool

	featureBandit   *featureBandits
	lastFeatureArms featureArms
}

var globalDBSeq atomic.Int64
var notInWrappedPattern = regexp.MustCompile(`(?i)NOT\s*\([^)]*\bIN\s*\(`)

// New constructs a Runner for the given config and DB.
func New(cfg config.Config, exec *db.DB) *Runner {
	state := &schema.State{}
	gen := generator.New(cfg, state, cfg.Seed)
	cloudUploader, err := uploader.NewS3(cfg.Storage.S3)
	if err != nil {
		cloudUploader = nil
	}
	var up uploader.Uploader = uploader.NoopUploader{}
	if cloudUploader != nil && cloudUploader.Enabled() {
		up = cloudUploader
	}
	r := &Runner{
		cfg:       cfg,
		exec:      exec,
		gen:       gen,
		state:     state,
		baseDB:    cfg.Database,
		validator: validator.New(),
		reporter:  report.New(cfg.PlanReplayer.OutputDir, cfg.MaxDataDumpRows),
		replayer:  replayer.New(cfg.PlanReplayer),
		uploader:  up,
		oracles: []oracle.Oracle{
			oracle.NoREC{},
			oracle.TLP{},
			oracle.DQP{HintSets: cfg.DQP.HintSets, Variables: cfg.DQP.Variables},
			oracle.CERT{},
			oracle.CODDTest{},
			oracle.DQE{},
		},
	}
	if cfg.QPG.Enabled {
		r.qpgState = newQPGState(cfg.QPG)
	}
	return r
}

// Run executes the fuzz loop until iterations are exhausted or an error occurs.
func (r *Runner) Run(ctx context.Context) error {
	r.exec.Validate = r.validator.Validate
	r.exec.Observe = r.observeSQL
	stop := r.startStatsLogger()
	defer stop()

	r.initBandits()
	util.Infof("runner start database=%s iterations=%d plan_cache_only=%t", r.cfg.Database, r.cfg.Iterations, r.cfg.PlanCacheOnly)
	if err := r.setupDatabase(ctx); err != nil {
		return err
	}
	if err := r.initState(ctx); err != nil {
		return err
	}
	if r.cfg.PlanCacheOnly {
		return r.runPlanCacheOnly(ctx)
	}

	for i := 0; i < r.cfg.Iterations; i++ {
		action := r.pickAction()
		var reward float64
		switch action {
		case 0:
			r.runDDL(ctx)
		case 1:
			r.runDML(ctx)
		default:
			if r.runQuery(ctx) {
				reward = 1
			}
		}
		r.updateActionBandit(action, reward)
	}
	return nil
}

func (r *Runner) setupDatabase(ctx context.Context) error {
	if _, err := r.exec.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", r.cfg.Database)); err != nil {
		return err
	}
	if _, err := r.exec.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", r.cfg.Database)); err != nil {
		return err
	}
	if _, err := r.exec.ExecContext(ctx, fmt.Sprintf("USE %s", r.cfg.Database)); err != nil {
		return err
	}
	if r.cfg.Features.PlanCache {
		_, _ = r.exec.ExecContext(ctx, "SET SESSION tidb_enable_prepared_plan_cache = 1")
		_, _ = r.exec.ExecContext(ctx, "SET SESSION tidb_enable_plan_cache_for_param = 1")
	}
	return nil
}

func (r *Runner) initState(ctx context.Context) error {
	initialTables := 2
	for i := 0; i < initialTables; i++ {
		tbl := r.gen.GenerateTable()
		sql := r.gen.CreateTableSQL(tbl)
		if err := r.execSQL(ctx, sql); err != nil {
			return err
		}
		r.state.Tables = append(r.state.Tables, tbl)
		tablePtr := &r.state.Tables[len(r.state.Tables)-1]
		insertCount := max(1, r.cfg.MaxRowsPerTable/5)
		for j := 0; j < insertCount; j++ {
			if err := r.execSQL(ctx, r.gen.InsertSQL(tablePtr)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Runner) runDDL(ctx context.Context) {
	actions := make([]string, 0, 5)
	if len(r.state.Tables) < r.cfg.MaxTables {
		actions = append(actions, "create_table")
	}
	if r.cfg.Features.Indexes && len(r.state.Tables) > 0 {
		actions = append(actions, "create_index")
	}
	if r.cfg.Features.Views && len(r.state.Tables) > 0 {
		actions = append(actions, "create_view")
	}
	if r.cfg.Features.ForeignKeys && len(r.state.Tables) > 1 {
		actions = append(actions, "add_fk")
	}
	if r.cfg.Features.CheckConstraints && len(r.state.Tables) > 0 {
		actions = append(actions, "add_check")
	}
	if len(actions) == 0 {
		return
	}

	action := actions[r.gen.Rand.Intn(len(actions))]
	switch action {
	case "create_table":
		tbl := r.gen.GenerateTable()
		sql := r.gen.CreateTableSQL(tbl)
		if err := r.execSQL(ctx, sql); err != nil {
			return
		}
		r.state.Tables = append(r.state.Tables, tbl)
		tablePtr := &r.state.Tables[len(r.state.Tables)-1]
		_ = r.execSQL(ctx, r.gen.InsertSQL(tablePtr))
	case "create_index":
		tableIdx := r.gen.Rand.Intn(len(r.state.Tables))
		tablePtr := &r.state.Tables[tableIdx]
		sql, ok := r.gen.CreateIndexSQL(tablePtr)
		if !ok {
			return
		}
		_ = r.execSQL(ctx, sql)
	case "create_view":
		sql := r.gen.CreateViewSQL()
		if sql == "" {
			return
		}
		_ = r.execSQL(ctx, sql)
	case "add_fk":
		sql := r.gen.AddForeignKeySQL(r.state)
		if sql == "" {
			return
		}
		_ = r.execSQL(ctx, sql)
	case "add_check":
		tbl := r.state.Tables[r.gen.Rand.Intn(len(r.state.Tables))]
		sql := r.gen.AddCheckConstraintSQL(tbl)
		_ = r.execSQL(ctx, sql)
	}
}

func (r *Runner) runDML(ctx context.Context) {
	if len(r.state.Tables) == 0 {
		return
	}
	choice := r.pickDML()
	var reward float64
	tbl := r.state.Tables[r.gen.Rand.Intn(len(r.state.Tables))]
	switch choice {
	case 0:
		tableIdx := r.gen.Rand.Intn(len(r.state.Tables))
		tablePtr := &r.state.Tables[tableIdx]
		_ = r.execSQL(ctx, r.gen.InsertSQL(tablePtr))
	case 1:
		updateSQL, _, _, _ := r.gen.UpdateSQL(tbl)
		if updateSQL != "" {
			_ = r.execSQL(ctx, updateSQL)
		}
	case 2:
		deleteSQL, _ := r.gen.DeleteSQL(tbl)
		if deleteSQL != "" {
			_ = r.execSQL(ctx, deleteSQL)
		}
	}
	r.updateDMLBandit(choice, reward)
}

func (r *Runner) runQuery(ctx context.Context) bool {
	if r.cfg.Features.PlanCache && util.Chance(r.gen.Rand, 20) {
		return r.runPrepared(ctx)
	}
	r.prepareFeatureWeights()
	appliedQPG := r.applyQPGWeights()
	if appliedQPG && r.featureBandit == nil {
		defer r.gen.ClearAdaptiveWeights()
	}
	oracleIdx := r.pickOracle()
	var reward float64
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	result := r.oracles[oracleIdx].Run(qctx, r.exec, r.gen, r.state)
	if result.OK {
		r.maybeObservePlan(ctx, result)
		if isPanicError(result.Err) {
			r.handleResult(ctx, result)
			reward = 1
		}
		r.updateOracleBandit(oracleIdx, reward)
		r.updateFeatureBandits(reward)
		r.tickQPG()
		return reward > 0
	}
	r.handleResult(ctx, result)
	reward = 1
	r.updateOracleBandit(oracleIdx, reward)
	r.updateFeatureBandits(reward)
	r.maybeObservePlan(ctx, result)
	r.tickQPG()
	return true
}

func (r *Runner) runPrepared(ctx context.Context) bool {
	pq := r.gen.GeneratePreparedQuery()
	if pq.SQL == "" {
		return false
	}
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	stmt, err := r.exec.PrepareContext(qctx, pq.SQL)
	if err != nil {
		return false
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(qctx, pq.Args...)
	if err != nil {
		if isPanicError(err) {
			result := oracle.Result{OK: false, Oracle: "PlanCache", SQL: []string{pq.SQL}, Err: err}
			r.handleResult(ctx, result)
			return true
		}
		return false
	}
	rows.Close()
	return false
}

func (r *Runner) runPlanCacheOnly(ctx context.Context) error {
	var total int
	var invalid int
	var execErrors int
	var hitSecond int
	var missSecond int
	var hitFirstUnexpected int
	for i := 0; i < r.cfg.Iterations; i++ {
		total++
		conn, err := r.exec.Conn(ctx)
		if err != nil {
			return err
		}
		if err := r.execOnConn(ctx, conn, fmt.Sprintf("USE %s", r.cfg.Database)); err != nil {
			conn.Close()
			return err
		}
		_ = r.execOnConn(ctx, conn, "SET SESSION tidb_enable_prepared_plan_cache = 1")
		_ = r.execOnConn(ctx, conn, "SET SESSION tidb_enable_plan_cache_for_param = 1")

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

		stmt, err := conn.PrepareContext(ctx, pq.SQL)
		if err != nil {
			conn.Close()
			continue
		}
		_, err = stmt.ExecContext(ctx, pq.Args...)
		if err != nil {
			stmt.Close()
			conn.Close()
			execErrors++
			if isPanicError(err) {
				result := oracle.Result{OK: false, Oracle: "PlanCacheOnly", SQL: []string{pq.SQL}, Err: err}
				r.handleResult(ctx, result)
			}
			continue
		}
		hit1, err := r.lastPlanFromCache(ctx, conn)
		if err == nil && hit1 == 1 {
			hitFirstUnexpected++
			result := oracle.Result{
				OK:       false,
				Oracle:   "PlanCacheOnly",
				SQL:      []string{concreteSQL, formatPrepareSQL(pq.SQL), formatExecuteSQL("stmt", pq.Args)},
				Expected: "last_plan_from_cache=0",
				Actual:   fmt.Sprintf("last_plan_from_cache=%d", hit1),
			}
			r.handleResult(ctx, result)
		}

		args2 := r.gen.GeneratePreparedArgsForQuery(pq.Args, pq.ArgTypes)
		_, err = stmt.ExecContext(ctx, args2...)
		_ = stmt.Close()
		if err != nil {
			conn.Close()
			execErrors++
			if isPanicError(err) {
				result := oracle.Result{OK: false, Oracle: "PlanCacheOnly", SQL: []string{pq.SQL}, Err: err}
				r.handleResult(ctx, result)
			}
			continue
		}

		hit, err := r.lastPlanFromCache(ctx, conn)
		conn.Close()
		if err != nil {
			continue
		}
		if hit != 1 {
			missSecond++
			result := oracle.Result{
				OK:       false,
				Oracle:   "PlanCacheOnly",
				SQL:      []string{concreteSQL, formatPrepareSQL(pq.SQL), formatExecuteSQL("stmt", pq.Args), formatExecuteSQL("stmt", args2)},
				Expected: "last_plan_from_cache=1",
				Actual:   fmt.Sprintf("last_plan_from_cache=%d", hit),
				Details: map[string]any{
					"args_first":  formatArgs(pq.Args),
					"args_second": formatArgs(args2),
				},
			}
			r.handleResult(ctx, result)
		} else {
			hitSecond++
		}
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

func (r *Runner) execOnConn(ctx context.Context, conn *sql.Conn, sql string) error {
	if err := r.validator.Validate(sql); err != nil {
		return err
	}
	_, err := conn.ExecContext(ctx, sql)
	return err
}

func (r *Runner) execSQL(ctx context.Context, sql string) error {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	_, err := r.exec.ExecContext(qctx, sql)
	if err == nil {
		r.recordInsert(sql)
	}
	return err
}

func (r *Runner) maybeObservePlan(ctx context.Context, result oracle.Result) {
	if !r.cfg.QPG.Enabled || result.Err != nil || r.qpgState == nil {
		return
	}
	target := pickExplainTarget(result.SQL)
	if target == "" {
		return
	}
	r.observePlan(ctx, target)
}

func pickExplainTarget(sqls []string) string {
	for _, sqlText := range sqls {
		trimmed := strings.TrimSpace(sqlText)
		if trimmed == "" {
			continue
		}
		upper := strings.ToUpper(trimmed)
		if strings.HasPrefix(upper, "EXPLAIN") || strings.HasPrefix(upper, "ANALYZE") {
			continue
		}
		if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "WITH") {
			return sqlText
		}
	}
	return ""
}

func (r *Runner) observePlan(ctx context.Context, sqlText string) {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	if r.qpgState != nil && r.qpgState.shouldSkipExplain(sqlText) {
		return
	}
	explainSQL := "EXPLAIN " + sqlText
	if format := strings.TrimSpace(r.cfg.QPG.ExplainFormat); format != "" {
		explainSQL = fmt.Sprintf("EXPLAIN FORMAT='%s' %s", format, sqlText)
	}
	rows, err := r.exec.QueryContext(qctx, explainSQL)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "format") {
			rows, err = r.exec.QueryContext(qctx, "EXPLAIN "+sqlText)
		}
		if err != nil {
			return
		}
	}
	defer rows.Close()
	info, err := parsePlan(rows)
	if err != nil || info.signature == "" {
		return
	}
	obs := r.qpgState.observe(info)
	if obs.newPlan && r.cfg.Logging.Verbose && r.qpgState.canLog() {
		util.Infof("qpg new plan ops=%d join=%t agg=%t", len(info.operators), info.hasJoin, info.hasAgg)
	}
	if obs.newJoinType && r.cfg.Logging.Verbose && r.qpgState.canLog() {
		util.Infof("qpg new join type join=%v", info.joins)
	}
	if !obs.newPlan && r.cfg.QPG.MutationProb > 0 && util.Chance(r.gen.Rand, r.cfg.QPG.MutationProb) {
		r.qpgMutate(ctx)
	}
}

func (r *Runner) explainSignature(ctx context.Context, sqlText string) (string, string) {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	explainSQL := "EXPLAIN " + sqlText
	if format := strings.TrimSpace(r.cfg.QPG.ExplainFormat); format != "" {
		explainSQL = fmt.Sprintf("EXPLAIN FORMAT='%s' %s", format, sqlText)
	}
	rows, err := r.exec.QueryContext(qctx, explainSQL)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "format") {
			rows, err = r.exec.QueryContext(qctx, "EXPLAIN "+sqlText)
		}
		if err != nil {
			return "", ""
		}
	}
	defer rows.Close()
	info, err := parsePlan(rows)
	if err != nil {
		return "", ""
	}
	return info.signature, info.version
}

type planInfo struct {
	signature string
	shapeSig  string
	opSig     string
	operators []string
	joins     []string
	joinOrder string
	hasJoin   bool
	hasAgg    bool
	version   string
}

type qpgState struct {
	seenPlans      map[string]struct{}
	seenShapes     map[string]struct{}
	seenOps        map[string]struct{}
	seenJoins      map[string]struct{}
	seenJoinOrder  map[string]struct{}
	seenOpSig      map[string]struct{}
	seenSQL        map[string]int64
	noNewPlan      int
	noNewOp        int
	noJoin         int
	noAgg          int
	noNewJoinType  int
	noNewShape     int
	noNewOpSig     int
	noNewJoinOrder int
	override       *generator.AdaptiveWeights
	overrideTTL    int
	lastOverride   string
	lastLog        time.Time
	seenSQLTTL     int64
	seenSQLMax     int
	seenSQLSweep   int64
}

type qpgObservation struct {
	newPlan     bool
	newOp       bool
	newJoinType bool
}

func newQPGState(cfg config.QPGConfig) *qpgState {
	ttl := cfg.SeenSQLTTLSeconds
	if ttl <= 0 {
		ttl = 3
	}
	maxEntries := cfg.SeenSQLMax
	if maxEntries <= 0 {
		maxEntries = 512
	}
	sweep := cfg.SeenSQLSweepSeconds
	if sweep <= 0 {
		sweep = 10
	}
	return &qpgState{
		seenPlans:     make(map[string]struct{}),
		seenShapes:    make(map[string]struct{}),
		seenOps:       make(map[string]struct{}),
		seenJoins:     make(map[string]struct{}),
		seenJoinOrder: make(map[string]struct{}),
		seenOpSig:     make(map[string]struct{}),
		seenSQL:       make(map[string]int64),
		seenSQLTTL:    int64(ttl),
		seenSQLMax:    maxEntries,
		seenSQLSweep:  int64(sweep),
	}
}

func parsePlan(rows *sql.Rows) (planInfo, error) {
	cols, err := rows.Columns()
	if err != nil {
		return planInfo{}, err
	}
	idIdx := 0
	for i, col := range cols {
		if strings.EqualFold(col, "id") {
			idIdx = i
			break
		}
	}
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var b strings.Builder
	var shape strings.Builder
	var opSig strings.Builder
	var ops []string
	var joins []string
	hasJoin := false
	hasAgg := false
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return planInfo{}, err
		}
		if len(cols) == 1 {
			text := string(values[0])
			if isJSONText(text) {
				return parsePlanJSON(text), nil
			}
		}
		normalizePlanRow(values)
		for i, v := range values {
			if i > 0 {
				b.WriteByte('|')
			}
			b.Write(v)
		}
		b.WriteByte('\n')
		id := ""
		if idIdx >= 0 && idIdx < len(values) {
			id = string(values[idIdx])
		}
		depth, op := parsePlanNode(id)
		if op != "" {
			ops = append(ops, op)
			shape.WriteString(fmt.Sprintf("%d:%s;", depth, op))
			opSig.WriteString(op)
			opSig.WriteByte(';')
			if strings.Contains(strings.ToLower(op), "join") {
				hasJoin = true
				joins = append(joins, fmt.Sprintf("%s@%d", joinTypeFromOp(op), depth))
			}
			if strings.Contains(strings.ToLower(op), "agg") {
				hasAgg = true
			}
		}
	}
	sum := sha1.Sum([]byte(b.String()))
	version := "plain"
	return planInfo{
		signature: hex.EncodeToString(sum[:]),
		shapeSig:  shape.String(),
		opSig:     opSig.String(),
		operators: ops,
		joins:     joins,
		joinOrder: strings.Join(joins, "->"),
		hasJoin:   hasJoin,
		hasAgg:    hasAgg,
		version:   version,
	}, nil
}

func parsePlanNode(id string) (int, string) {
	if id == "" {
		return 0, ""
	}
	prefix, rest := splitPlanPrefix(id)
	if rest == "" {
		return 0, ""
	}
	op := rest
	for i, r := range rest {
		if r == '_' || r == ' ' || r == '(' {
			op = rest[:i]
			break
		}
	}
	spaceCount := 0
	barCount := 0
	for _, r := range prefix {
		if r == ' ' {
			spaceCount++
		} else if r == '│' || r == '|' {
			barCount++
		}
	}
	depth := barCount + spaceCount/2
	return depth, op
}

func splitPlanPrefix(id string) (string, string) {
	for i, r := range id {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return id[:i], id[i:]
		}
	}
	return id, ""
}

func (s *qpgState) observe(info planInfo) qpgObservation {
	obs := qpgObservation{}
	if info.signature == "" {
		return obs
	}
	if _, ok := s.seenPlans[info.signature]; !ok {
		s.seenPlans[info.signature] = struct{}{}
		obs.newPlan = true
		s.noNewPlan = 0
	} else {
		s.noNewPlan++
	}
	if info.shapeSig != "" {
		if _, ok := s.seenShapes[info.shapeSig]; !ok {
			s.seenShapes[info.shapeSig] = struct{}{}
			s.noNewShape = 0
		} else {
			s.noNewShape++
		}
	}
	for _, op := range info.operators {
		if _, ok := s.seenOps[op]; !ok {
			s.seenOps[op] = struct{}{}
			obs.newOp = true
		}
	}
	if info.opSig != "" {
		if _, ok := s.seenOpSig[info.opSig]; !ok {
			s.seenOpSig[info.opSig] = struct{}{}
			s.noNewOpSig = 0
		} else {
			s.noNewOpSig++
		}
	}
	if obs.newOp {
		s.noNewOp = 0
	} else {
		s.noNewOp++
	}
	for _, joinType := range info.joins {
		if joinType == "" {
			continue
		}
		if _, ok := s.seenJoins[joinType]; !ok {
			s.seenJoins[joinType] = struct{}{}
			obs.newJoinType = true
		}
	}
	if obs.newJoinType {
		s.noNewJoinType = 0
	} else if info.hasJoin {
		s.noNewJoinType++
	}
	if info.joinOrder != "" {
		if _, ok := s.seenJoinOrder[info.joinOrder]; !ok {
			s.seenJoinOrder[info.joinOrder] = struct{}{}
			s.noNewJoinOrder = 0
		} else if info.hasJoin {
			s.noNewJoinOrder++
		}
	}
	if info.hasJoin {
		s.noJoin = 0
	} else {
		s.noJoin++
	}
	if info.hasAgg {
		s.noAgg = 0
	} else {
		s.noAgg++
	}
	return obs
}

func (s *qpgState) stats() (int, int, int, int) {
	return len(s.seenPlans), len(s.seenShapes), len(s.seenOps), len(s.seenJoins)
}

func (r *Runner) applyQPGWeights() bool {
	if !r.cfg.QPG.Enabled || r.qpgState == nil {
		return false
	}
	if r.qpgState.overrideTTL <= 0 {
		setOverride := false
		if r.qpgState.noJoin >= 3 {
			joinCount := max(r.cfg.Weights.Features.JoinCount, 3)
			r.qpgState.override = &generator.AdaptiveWeights{JoinCount: min(joinCount, r.cfg.MaxJoinTables)}
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noAgg >= 3 {
			agg := max(r.cfg.Weights.Features.AggProb, 60)
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.AggProb = agg
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noNewPlan >= 5 {
			subq := max(r.cfg.Weights.Features.SubqCount, 3)
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.SubqCount = subq
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noNewOpSig >= 4 {
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.SubqCount = max(r.cfg.Weights.Features.SubqCount, 3)
			override.AggProb = max(r.cfg.Weights.Features.AggProb, 60)
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noNewShape >= 4 {
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.JoinCount = max(r.cfg.Weights.Features.JoinCount, 3)
			override.SubqCount = max(r.cfg.Weights.Features.SubqCount, 3)
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noNewJoinType >= 3 {
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.JoinCount = max(r.cfg.Weights.Features.JoinCount, 3)
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noNewJoinOrder >= 3 {
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.JoinCount = max(r.cfg.Weights.Features.JoinCount, 4)
			override.SubqCount = max(r.cfg.Weights.Features.SubqCount, 3)
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if setOverride && r.cfg.Logging.Verbose && r.qpgState.override != nil && r.qpgState.canLog() {
			sig := fmt.Sprintf("%d/%d/%d/%d", r.qpgState.override.JoinCount, r.qpgState.override.SubqCount, r.qpgState.override.AggProb, r.qpgState.overrideTTL)
			if sig != r.qpgState.lastOverride {
				util.Infof("qpg weight boost join=%d subq=%d agg=%d ttl=%d", r.qpgState.override.JoinCount, r.qpgState.override.SubqCount, r.qpgState.override.AggProb, r.qpgState.overrideTTL)
				r.qpgState.lastOverride = sig
			}
		}
	}
	if r.qpgState.override == nil || r.qpgState.overrideTTL <= 0 {
		return false
	}
	base := generator.AdaptiveWeights{
		JoinCount: r.cfg.Weights.Features.JoinCount,
		SubqCount: r.cfg.Weights.Features.SubqCount,
		AggProb:   r.cfg.Weights.Features.AggProb,
	}
	if r.gen.Adaptive != nil {
		base = *r.gen.Adaptive
	}
	override := r.qpgState.override
	if override.JoinCount > 0 {
		base.JoinCount = override.JoinCount
	}
	if override.SubqCount > 0 {
		base.SubqCount = override.SubqCount
	}
	if override.AggProb > 0 {
		base.AggProb = override.AggProb
	}
	r.gen.SetAdaptiveWeights(base)
	return true
}

func (r *Runner) tickQPG() {
	if r.qpgState == nil || r.qpgState.overrideTTL <= 0 {
		return
	}
	r.qpgState.overrideTTL--
	if r.qpgState.overrideTTL == 0 {
		r.qpgState.override = nil
	}
}

func (r *Runner) qpgMutate(ctx context.Context) {
	if len(r.state.Tables) == 0 {
		return
	}
	if r.cfg.Features.Indexes && util.Chance(r.gen.Rand, 50) {
		tableIdx := r.gen.Rand.Intn(len(r.state.Tables))
		tablePtr := &r.state.Tables[tableIdx]
		sql, ok := r.gen.CreateIndexSQL(tablePtr)
		if ok {
			_ = r.execSQL(ctx, sql)
		}
		return
	}
	tbl := r.state.Tables[r.gen.Rand.Intn(len(r.state.Tables))]
	_ = r.execSQL(ctx, fmt.Sprintf("ANALYZE TABLE %s", tbl.Name))
}

func (s *qpgState) shouldSkipExplain(sqlText string) bool {
	if sqlText == "" {
		return true
	}
	key := sha1.Sum([]byte(sqlText))
	hash := hex.EncodeToString(key[:])
	now := time.Now().Unix()
	if last, ok := s.seenSQL[hash]; ok && now-last < s.seenSQLTTL {
		return true
	}
	s.seenSQL[hash] = now
	if len(s.seenSQL) > s.seenSQLMax {
		for k, v := range s.seenSQL {
			if now-v > s.seenSQLSweep {
				delete(s.seenSQL, k)
			}
		}
	}
	return false
}

func (s *qpgState) canLog() bool {
	if time.Since(s.lastLog) < time.Second {
		return false
	}
	s.lastLog = time.Now()
	return true
}

func joinTypeFromOp(op string) string {
	lower := strings.ToLower(op)
	switch {
	case strings.Contains(lower, "indexhashjoin"):
		return "IndexHashJoin"
	case strings.Contains(lower, "indexjoin"):
		return "IndexJoin"
	case strings.Contains(lower, "mergejoin"):
		return "MergeJoin"
	case strings.Contains(lower, "hashjoin"):
		return "HashJoin"
	case strings.Contains(lower, "join"):
		return "Join"
	default:
		return ""
	}
}

func normalizePlanRow(values []sql.RawBytes) {
	for i, v := range values {
		if len(v) == 0 {
			continue
		}
		normalized := normalizePlanValue(string(v))
		if normalized != string(v) {
			values[i] = []byte(normalized)
		}
	}
}

func normalizePlanValue(value string) string {
	if value == "" {
		return value
	}
	normalized := regexp.MustCompile(`t\\d+`).ReplaceAllString(value, "tN")
	normalized = regexp.MustCompile(`c\\d+`).ReplaceAllString(normalized, "cN")
	normalized = regexp.MustCompile(`idx_\\w+`).ReplaceAllString(normalized, "idx_N")
	normalized = regexp.MustCompile(`\\b\\d+\\b`).ReplaceAllString(normalized, "N")
	return normalized
}

func normalizeOp(op string) string {
	if op == "" {
		return ""
	}
	out := op
	for i, r := range op {
		if r == '_' || r == ' ' || r == '(' {
			out = op[:i]
			break
		}
	}
	return out
}

func isJSONText(text string) bool {
	trimmed := strings.TrimSpace(text)
	return strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[")
}

var jsonIDPattern = regexp.MustCompile(`"id"\s*:\s*"([^"]+)"`)
var jsonOpPattern = regexp.MustCompile(`"operator"\s*:\s*"([^"]+)"`)

func parsePlanJSON(text string) planInfo {
	trimmed := strings.TrimSpace(text)
	ops := make([]string, 0, 16)
	joins := make([]string, 0, 8)
	hasJoin := false
	hasAgg := false
	var shape strings.Builder
	var opSig strings.Builder
	matches := jsonIDPattern.FindAllStringSubmatch(trimmed, -1)
	if len(matches) == 0 {
		matches = jsonOpPattern.FindAllStringSubmatch(trimmed, -1)
	}
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		token := match[1]
		_, op := parsePlanNode(token)
		if op == "" {
			op = token
		}
		op = normalizeOp(op)
		if op == "" {
			continue
		}
		ops = append(ops, op)
		shape.WriteString("0:")
		shape.WriteString(op)
		shape.WriteString(";")
		opSig.WriteString(op)
		opSig.WriteByte(';')
		if strings.Contains(strings.ToLower(op), "join") {
			hasJoin = true
			joins = append(joins, joinTypeFromOp(op))
		}
		if strings.Contains(strings.ToLower(op), "agg") {
			hasAgg = true
		}
	}
	sum := sha1.Sum([]byte(trimmed))
	return planInfo{
		signature: hex.EncodeToString(sum[:]),
		shapeSig:  shape.String(),
		opSig:     opSig.String(),
		operators: ops,
		joins:     joins,
		joinOrder: strings.Join(joins, "->"),
		hasJoin:   hasJoin,
		hasAgg:    hasAgg,
		version:   "json",
	}
}

func (r *Runner) tidbVersion(ctx context.Context) string {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	row := r.exec.QueryRowContext(qctx, "SELECT tidb_version()")
	var version string
	if err := row.Scan(&version); err != nil {
		return ""
	}
	return version
}

func (r *Runner) handleResult(ctx context.Context, result oracle.Result) {
	caseData, err := r.reporter.NewCase()
	if err != nil {
		return
	}
	planPath := ""
	planSignature := ""
	planSigFormat := ""
	if len(result.SQL) > 0 {
		var planErr error
		planPath, planErr = r.replayer.DumpAndDownload(ctx, r.exec, result.SQL[0], caseData.Dir)
		if planErr != nil {
			util.Warnf("plan replayer dump failed dir=%s err=%v", caseData.Dir, planErr)
		}
		if r.cfg.QPG.Enabled && r.qpgState != nil {
			planSignature, planSigFormat = r.explainSignature(ctx, result.SQL[0])
		}
	}

	summary := report.Summary{
		Oracle:        result.Oracle,
		SQL:           result.SQL,
		Expected:      result.Expected,
		Actual:        result.Actual,
		Details:       result.Details,
		Timestamp:     time.Now().Format(time.RFC3339),
		PlanReplay:    planPath,
		TiDBVersion:   r.tidbVersion(ctx),
		PlanSignature: planSignature,
		PlanSigFormat: planSigFormat,
	}
	if result.Err != nil {
		summary.Error = result.Err.Error()
	}
	_ = r.reporter.WriteSummary(caseData, summary)
	_ = r.reporter.WriteSQL(caseData, "case.sql", result.SQL)
	_ = r.reporter.WriteSQL(caseData, "inserts.sql", r.insertLog)
	_ = r.reporter.DumpSchema(ctx, caseData, r.exec, r.state)
	_ = r.reporter.DumpData(ctx, caseData, r.exec, r.state)
	if r.cfg.Minimize.Enabled {
		minimized := r.minimizeCase(ctx, result)
		if minimized.minimized {
			if len(minimized.caseSQL) > 0 {
				_ = r.reporter.WriteSQL(caseData, "case_min.sql", minimized.caseSQL)
			}
			if len(minimized.insertSQL) > 0 {
				_ = r.reporter.WriteSQL(caseData, "inserts_min.sql", minimized.insertSQL)
			}
			if len(minimized.reproSQL) > 0 {
				_ = r.reporter.WriteSQL(caseData, "repro_min.sql", minimized.reproSQL)
			}
		}
	}

	if r.uploader.Enabled() {
		location, err := r.uploader.UploadDir(ctx, caseData.Dir)
		if err == nil {
			summary.UploadLocation = location
			_ = r.reporter.WriteSummary(caseData, summary)
		}
	}

	if result.Err != nil {
		util.Errorf("case captured oracle=%s dir=%s err=%v", result.Oracle, caseData.Dir, result.Err)
	} else if result.Expected != "" || result.Actual != "" {
		util.Warnf("case captured oracle=%s dir=%s expected=%s actual=%s", result.Oracle, caseData.Dir, result.Expected, result.Actual)
	} else {
		util.Warnf("case captured oracle=%s dir=%s", result.Oracle, caseData.Dir)
	}
	if err := r.rotateDatabase(ctx); err != nil {
		util.Errorf("rotate database after bug failed: %v", err)
	}
}

func isPanicError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "panic") || strings.Contains(msg, "assert") || strings.Contains(msg, "internal error")
}

func (r *Runner) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, time.Duration(r.cfg.StatementTimeoutMs)*time.Millisecond)
}

func (r *Runner) recordInsert(sql string) {
	trimmed := strings.TrimSpace(sql)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") {
		return
	}
	if r.cfg.MaxInsertStatements <= 0 {
		return
	}
	if len(r.insertLog) >= r.cfg.MaxInsertStatements {
		r.insertLog = r.insertLog[1:]
	}
	r.insertLog = append(r.insertLog, trimmed)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func formatPrepareSQL(sqlText string) string {
	return fmt.Sprintf("PREPARE stmt FROM '%s'", strings.ReplaceAll(sqlText, "'", "''"))
}

func formatExecuteSQL(name string, args []any) string {
	if len(args) == 0 {
		return fmt.Sprintf("EXECUTE %s", name)
	}
	return fmt.Sprintf("EXECUTE %s USING %s", name, strings.Join(formatArgs(args), ", "))
}

func formatArgs(args []any) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		switch v := arg.(type) {
		case nil:
			out = append(out, "NULL")
		case string:
			out = append(out, fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")))
		default:
			out = append(out, fmt.Sprintf("%v", v))
		}
	}
	return out
}

func materializeSQL(sqlText string, args []any) string {
	if len(args) == 0 {
		return sqlText
	}
	formatted := formatArgs(args)
	var b strings.Builder
	argIdx := 0
	for _, r := range sqlText {
		if r == '?' && argIdx < len(formatted) {
			b.WriteString(formatted[argIdx])
			argIdx++
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func (r *Runner) rotateDatabase(ctx context.Context) error {
	seq := globalDBSeq.Add(1)
	r.cfg.Database = fmt.Sprintf("%s_r%d", r.baseDB, seq)
	r.state = &schema.State{}
	r.gen = generator.New(r.cfg, r.state, r.cfg.Seed+seq)
	r.exec.Validate = r.validator.Validate
	r.exec.Observe = r.observeSQL
	r.insertLog = nil
	if r.cfg.QPG.Enabled {
		r.qpgState = newQPGState(r.cfg.QPG)
	}
	if err := r.setupDatabase(ctx); err != nil {
		return err
	}
	return r.initState(ctx)
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

func (r *Runner) initBandits() {
	if !r.cfg.Adaptive.Enabled {
		return
	}
	if r.cfg.Adaptive.AdaptActions {
		r.actionBandit = util.NewBandit(3, r.cfg.Adaptive.UCBExploration)
		r.actionEnabled = []bool{
			r.cfg.Weights.Actions.DDL > 0,
			r.cfg.Weights.Actions.DML > 0,
			r.cfg.Weights.Actions.Query > 0,
		}
	}
	if r.cfg.Adaptive.AdaptOracles {
		r.oracleBandit = util.NewBandit(len(r.oracles), r.cfg.Adaptive.UCBExploration)
		r.oracleEnabled = []bool{
			r.cfg.Weights.Oracles.NoREC > 0,
			r.cfg.Weights.Oracles.TLP > 0,
			r.cfg.Weights.Oracles.DQP > 0,
			r.cfg.Weights.Oracles.CERT > 0,
			r.cfg.Weights.Oracles.CODDTest > 0,
			r.cfg.Weights.Oracles.DQE > 0,
		}
	}
	if r.cfg.Adaptive.AdaptDML {
		r.dmlBandit = util.NewBandit(3, r.cfg.Adaptive.UCBExploration)
		r.dmlEnabled = []bool{
			r.cfg.Weights.DML.Insert > 0,
			r.cfg.Weights.DML.Update > 0,
			r.cfg.Weights.DML.Delete > 0,
		}
	}
	if r.cfg.Adaptive.AdaptFeatures {
		r.featureBandit = newFeatureBandits(r.cfg)
	}
}

func (r *Runner) pickAction() int {
	if r.actionBandit != nil {
		return r.actionBandit.Pick(r.gen.Rand, r.actionEnabled)
	}
	return util.PickWeighted(r.gen.Rand, []int{r.cfg.Weights.Actions.DDL, r.cfg.Weights.Actions.DML, r.cfg.Weights.Actions.Query})
}

func (r *Runner) updateActionBandit(action int, reward float64) {
	if r.actionBandit != nil {
		r.actionBandit.Update(action, reward)
	}
}

func (r *Runner) pickOracle() int {
	if r.oracleBandit != nil {
		return r.oracleBandit.Pick(r.gen.Rand, r.oracleEnabled)
	}
	return util.PickWeighted(r.gen.Rand, []int{
		r.cfg.Weights.Oracles.NoREC,
		r.cfg.Weights.Oracles.TLP,
		r.cfg.Weights.Oracles.DQP,
		r.cfg.Weights.Oracles.CERT,
		r.cfg.Weights.Oracles.CODDTest,
		r.cfg.Weights.Oracles.DQE,
	})
}

func (r *Runner) updateOracleBandit(oracleIdx int, reward float64) {
	if r.oracleBandit != nil {
		r.oracleBandit.Update(oracleIdx, reward)
	}
}

func (r *Runner) pickDML() int {
	if r.dmlBandit != nil {
		return r.dmlBandit.Pick(r.gen.Rand, r.dmlEnabled)
	}
	return util.PickWeighted(r.gen.Rand, []int{r.cfg.Weights.DML.Insert, r.cfg.Weights.DML.Update, r.cfg.Weights.DML.Delete})
}

func (r *Runner) updateDMLBandit(choice int, reward float64) {
	if r.dmlBandit != nil {
		r.dmlBandit.Update(choice, reward)
	}
}

type featureBandits struct {
	joinBandit *util.Bandit
	subqBandit *util.Bandit
	aggBandit  *util.Bandit
	joinArms   []int
	subqArms   []int
	aggArms    []int
}

type featureArms struct {
	joinArm int
	subqArm int
	aggArm  int
}

func newFeatureBandits(cfg config.Config) *featureBandits {
	joinArms := makeArms(1, cfg.MaxJoinTables)
	subqMax := cfg.Weights.Features.SubqCount
	if subqMax < 1 {
		subqMax = 1
	}
	subqArms := makeArms(0, subqMax)
	aggArms := makeProbArms(cfg.Weights.Features.AggProb)
	return &featureBandits{
		joinBandit: util.NewBandit(len(joinArms), cfg.Adaptive.UCBExploration),
		subqBandit: util.NewBandit(len(subqArms), cfg.Adaptive.UCBExploration),
		aggBandit:  util.NewBandit(len(aggArms), cfg.Adaptive.UCBExploration),
		joinArms:   joinArms,
		subqArms:   subqArms,
		aggArms:    aggArms,
	}
}

func makeArms(minVal, maxVal int) []int {
	if maxVal < minVal {
		maxVal = minVal
	}
	arms := make([]int, 0, maxVal-minVal+1)
	for i := minVal; i <= maxVal; i++ {
		arms = append(arms, i)
	}
	return arms
}

func makeProbArms(base int) []int {
	if base < 0 {
		base = 0
	}
	arms := []int{0, base / 2, base, min(100, base*2)}
	return uniqueInts(arms)
}

func uniqueInts(values []int) []int {
	seen := map[int]struct{}{}
	out := make([]int, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func (r *Runner) prepareFeatureWeights() {
	if r.featureBandit == nil {
		return
	}
	r.lastFeatureArms.joinArm = r.featureBandit.joinBandit.Pick(r.gen.Rand, nil)
	r.lastFeatureArms.subqArm = r.featureBandit.subqBandit.Pick(r.gen.Rand, nil)
	r.lastFeatureArms.aggArm = r.featureBandit.aggBandit.Pick(r.gen.Rand, nil)
	r.gen.SetAdaptiveWeights(generator.AdaptiveWeights{
		JoinCount: r.featureBandit.joinArms[r.lastFeatureArms.joinArm],
		SubqCount: r.featureBandit.subqArms[r.lastFeatureArms.subqArm],
		AggProb:   r.featureBandit.aggArms[r.lastFeatureArms.aggArm],
	})
}

func (r *Runner) updateFeatureBandits(reward float64) {
	if r.featureBandit == nil {
		return
	}
	lastFeatures := r.gen.LastFeatures
	r.gen.ClearAdaptiveWeights()
	r.gen.LastFeatures = nil
	if lastFeatures == nil {
		return
	}
	r.featureBandit.joinBandit.Update(r.lastFeatureArms.joinArm, reward)
	r.featureBandit.subqBandit.Update(r.lastFeatureArms.subqArm, reward)
	r.featureBandit.aggBandit.Update(r.lastFeatureArms.aggArm, reward)
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
						util.Infof("qpg stats plans=%d shapes=%d ops=%d join_types=%d", plans, shapes, ops, joins)
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
