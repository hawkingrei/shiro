package runner

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"shiro/internal/config"
	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/oracle"
	"shiro/internal/replayer"
	"shiro/internal/report"
	"shiro/internal/schema"
	"shiro/internal/tqs"
	"shiro/internal/uploader"
	"shiro/internal/util"
	"shiro/internal/validator"
)

// Runner orchestrates fuzzing, execution, and reporting.
type Runner struct {
	cfg                 config.Config
	exec                *db.DB
	gen                 *generator.Generator
	state               *schema.State
	baseDB              string
	validator           *validator.Validator
	reporter            *report.Reporter
	replayer            *replayer.Replayer
	uploader            uploader.Uploader
	oracles             []oracle.Oracle
	insertLog           []string
	statsMu             sync.Mutex
	genMu               sync.Mutex
	qpgMu               sync.Mutex
	kqeMu               sync.Mutex
	sqlTotal            int64
	sqlValid            int64
	sqlExists           int64
	sqlNotEx            int64
	sqlIn               int64
	sqlNotIn            int64
	impoTotal           int64
	impoSkips           int64
	impoTrunc           int64
	impoSkipReasons     map[string]int64
	impoSkipErrCodes    map[string]int64
	impoLastFailSQL     string
	impoLastFailErr     string
	certLastErrSQL      string
	certLastErr         string
	certLastErrReason   string
	joinCounts          map[int]int64
	joinTypeSeqs        map[string]int64
	joinGraphSigs       map[string]int64
	viewQueries         int64
	viewTableRefs       int64
	predicatePairsTotal int64
	predicatePairsJoin  int64
	truthMismatches     int64
	qpgState            *qpgState
	kqeState            *kqeState
	tqsHistory          *tqs.History
	oracleStats         map[string]*oracleFunnel
	baseActions         config.ActionWeights
	baseDMLWeights      config.DMLWeights
	baseDQEWeight       int
	baseTQSEnabled      bool
	baseDSGEnabled      bool
	dbSeq               int64

	actionBandit  *util.Bandit
	oracleBandit  *util.Bandit
	dmlBandit     *util.Bandit
	actionEnabled []bool
	oracleEnabled []bool
	dmlEnabled    []bool

	featureBandit   *featureBandits
	lastFeatureArms featureArms
}

func (r *Runner) baseTables() []*schema.Table {
	if r == nil || r.state == nil {
		return nil
	}
	out := make([]*schema.Table, 0, len(r.state.Tables))
	for i := range r.state.Tables {
		if r.state.Tables[i].IsView {
			continue
		}
		out = append(out, &r.state.Tables[i])
	}
	return out
}

const viewDDLBoostProb = 70

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
		cfg:              cfg,
		exec:             exec,
		gen:              gen,
		state:            state,
		baseDB:           cfg.Database,
		validator:        validator.New(),
		reporter:         report.New(cfg.PlanReplayer.OutputDir, cfg.MaxDataDumpRows),
		replayer:         replayer.New(cfg.PlanReplayer),
		uploader:         up,
		impoSkipReasons:  make(map[string]int64),
		impoSkipErrCodes: make(map[string]int64),
		joinCounts:       make(map[int]int64),
		joinTypeSeqs:     make(map[string]int64),
		joinGraphSigs:    make(map[string]int64),
		oracleStats:      make(map[string]*oracleFunnel),
		baseActions:      cfg.Weights.Actions,
		baseDMLWeights:   cfg.Weights.DML,
		baseDQEWeight:    cfg.Weights.Oracles.DQE,
		baseTQSEnabled:   cfg.TQS.Enabled,
		baseDSGEnabled:   cfg.Features.DSG,
		dbSeq:            0,
		oracles: []oracle.Oracle{
			oracle.NoREC{},
			oracle.TLP{},
			oracle.DQP{},
			oracle.CERT{MinBaseRows: cfg.Oracles.CertMinBaseRows},
			oracle.CODDTest{},
			oracle.DQE{},
			oracle.Impo{},
			oracle.GroundTruth{},
		},
	}
	if cfg.QPG.Enabled {
		r.qpgState = newQPGState(cfg.QPG)
	}
	if cfg.Features.Joins && cfg.KQE.Enabled {
		r.kqeState = newKQELiteState()
	}
	return r
}

// Run executes the fuzz loop until iterations are exhausted or an error occurs.
func (r *Runner) Run(ctx context.Context) error {
	r.exec.Validate = r.validator.Validate
	r.exec.Observe = r.observeSQL
	stop := r.startStatsLogger()
	defer stop()

	r.applyRuntimeToggles()
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
	return nil
}

func (r *Runner) initState(ctx context.Context) error {
	if r.cfg.TQS.Enabled {
		return r.initStateDSG(ctx)
	}
	r.gen.SetTruth(nil)
	initialTables := 2
	for i := 0; i < initialTables; i++ {
		tbl := r.gen.GenerateTable()
		sql := r.gen.CreateTableSQL(tbl)
		if err := r.execSQL(ctx, sql); err != nil {
			if _, ok := isWhitelistedSQLError(err); ok {
				continue
			}
			return err
		}
		r.state.Tables = append(r.state.Tables, tbl)
		tablePtr := &r.state.Tables[len(r.state.Tables)-1]
		insertCount := max(1, r.cfg.MaxRowsPerTable/5)
		for j := 0; j < insertCount; j++ {
			insertSQL := r.gen.InsertSQL(tablePtr)
			if err := r.execSQL(ctx, insertSQL); err != nil {
				if _, ok := isWhitelistedSQLError(err); ok {
					continue
				}
				return err
			}
		}
	}
	return nil
}

func (r *Runner) initStateDSG(ctx context.Context) error {
	cfg := r.cfg
	adjusted := false
	if cfg.TQS.DimTables > 0 && cfg.TQS.DimTables < 4 {
		cfg.TQS.DimTables = 4
		adjusted = true
	}
	if cfg.TQS.WideRows > 0 && cfg.TQS.WideRows < 80 {
		cfg.TQS.WideRows = 80
		adjusted = true
	}
	if adjusted {
		util.Detailf("tqs config adjusted dim_tables=%d wide_rows=%d", cfg.TQS.DimTables, cfg.TQS.WideRows)
		r.gen.Config = cfg
	}
	result, err := tqs.Build(cfg, r.gen.Rand)
	if err != nil {
		return err
	}
	r.state.Tables = result.State.Tables
	r.gen.State = r.state
	r.gen.SetTruth(result.Truth)
	r.tqsHistory = tqs.NewHistory(r.state, "t0")
	r.gen.SetTQSWalker(r.tqsHistory)
	for _, sql := range result.CreateSQL {
		if err := r.execSQL(ctx, sql); err != nil {
			if _, ok := isWhitelistedSQLError(err); ok {
				continue
			}
			return err
		}
	}
	for _, sql := range result.InsertSQL {
		if err := r.execSQL(ctx, sql); err != nil {
			if _, ok := isWhitelistedSQLError(err); ok {
				continue
			}
			return err
		}
	}
	return nil
}

func (r *Runner) runDDL(ctx context.Context) {
	actions := make([]string, 0, 5)
	baseTables := r.baseTables()
	viewCount := r.viewCount()
	viewMax := r.viewMax()
	if r.cfg.TQS.Enabled {
		if r.cfg.Features.Views && len(r.state.Tables) > 0 && viewCount < viewMax {
			actions = append(actions, "create_view")
		}
	} else {
		if len(baseTables) < r.cfg.MaxTables {
			actions = append(actions, "create_table")
		}
		if r.cfg.Features.Indexes && len(baseTables) > 0 {
			actions = append(actions, "create_index")
		}
		if r.cfg.Features.Views && len(r.state.Tables) > 0 && viewCount < viewMax {
			actions = append(actions, "create_view")
		}
		if r.cfg.Features.ForeignKeys && len(baseTables) > 1 {
			actions = append(actions, "add_fk")
		}
		if r.cfg.Features.CheckConstraints && len(baseTables) > 0 {
			actions = append(actions, "add_check")
		}
	}
	if len(actions) == 0 {
		return
	}

	if r.cfg.Features.Views && hasAction(actions, "create_view") && util.Chance(r.gen.Rand, viewDDLBoostProb) {
		action := "create_view"
		r.runDDLAction(ctx, action, baseTables)
		return
	}
	action := actions[r.gen.Rand.Intn(len(actions))]
	r.runDDLAction(ctx, action, baseTables)
}

func (r *Runner) runDDLAction(ctx context.Context, action string, baseTables []*schema.Table) {
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
		if r.tqsHistory != nil {
			r.tqsHistory.Refresh(r.state)
		}
	case "create_index":
		if len(baseTables) == 0 {
			return
		}
		tableIdx := r.gen.Rand.Intn(len(baseTables))
		tablePtr := baseTables[tableIdx]
		tableCopy := *tablePtr
		sql, ok := r.gen.CreateIndexSQL(&tableCopy)
		if !ok {
			return
		}
		if err := r.execSQL(ctx, sql); err != nil {
			return
		}
		*tablePtr = tableCopy
	case "create_view":
		if r.viewCount() >= r.viewMax() {
			return
		}
		sql, view := r.gen.CreateViewSQL()
		if sql == "" {
			return
		}
		if err := r.execSQL(ctx, sql); err != nil {
			return
		}
		if view != nil {
			r.state.Tables = append(r.state.Tables, *view)
		}
	case "add_fk":
		tableSnapshot := make([]schema.Table, 0, len(baseTables))
		for _, tbl := range baseTables {
			tableSnapshot = append(tableSnapshot, *tbl)
		}
		sql := r.gen.AddForeignKeySQL(&schema.State{Tables: tableSnapshot})
		if sql == "" {
			return
		}
		_ = r.execSQL(ctx, sql)
	case "add_check":
		if len(baseTables) == 0 {
			return
		}
		tbl := baseTables[r.gen.Rand.Intn(len(baseTables))]
		sql := r.gen.AddCheckConstraintSQL(*tbl)
		_ = r.execSQL(ctx, sql)
	}
}

func (r *Runner) viewCount() int {
	if r == nil || r.state == nil {
		return 0
	}
	count := 0
	for _, tbl := range r.state.Tables {
		if tbl.IsView {
			count++
		}
	}
	return count
}

func (r *Runner) viewMax() int {
	if r == nil {
		return 0
	}
	if r.cfg.Features.ViewMax > 0 {
		return r.cfg.Features.ViewMax
	}
	return 5
}

func (r *Runner) oracleModeLabel() string {
	if r == nil {
		return "unknown"
	}
	if r.cfg.TQS.Enabled {
		return "TQS"
	}
	if r.cfg.Weights.Oracles.DQE > 0 {
		return "DQE"
	}
	return "default"
}

func hasAction(actions []string, target string) bool {
	for _, action := range actions {
		if action == target {
			return true
		}
	}
	return false
}

func (r *Runner) runDML(ctx context.Context) {
	baseTables := r.baseTables()
	if len(baseTables) == 0 {
		return
	}
	choice := r.pickDML()
	var reward float64
	tbl := baseTables[r.gen.Rand.Intn(len(baseTables))]
	switch choice {
	case 0:
		_ = r.execSQL(ctx, r.gen.InsertSQL(tbl))
	case 1:
		updateSQL, _, _, _ := r.gen.UpdateSQL(*tbl)
		if updateSQL != "" {
			_ = r.execSQL(ctx, updateSQL)
		}
	case 2:
		deleteSQL, _ := r.gen.DeleteSQL(*tbl)
		if deleteSQL != "" {
			_ = r.execSQL(ctx, deleteSQL)
		}
	}
	r.updateDMLBandit(choice, reward)
}

func (r *Runner) runQuery(ctx context.Context) bool {
	if r.cfg.Features.PlanCache && util.Chance(r.gen.Rand, r.cfg.PlanCacheProb) {
		return r.runPrepared(ctx)
	}
	r.prepareFeatureWeights()
	appliedQPG := r.applyQPGWeights()
	appliedKQE := false
	if !appliedQPG {
		appliedKQE = r.applyKQELiteWeights()
	}
	appliedTemplate := r.applyQPGTemplateWeights()
	if appliedQPG && r.featureBandit == nil {
		defer r.clearAdaptiveWeights()
	}
	if appliedKQE && r.featureBandit == nil {
		defer r.clearAdaptiveWeights()
	}
	if appliedTemplate {
		defer r.clearTemplateWeights()
	}
	oracleIdx := r.pickOracle()
	oracleName := r.oracles[oracleIdx].Name()
	r.observeOracleRun(oracleName)
	restoreOracleBias := r.applyOracleBias(oracleName)
	if restoreOracleBias != nil {
		defer restoreOracleBias()
	}
	restoreOracleOverrides := r.applyOracleOverrides(oracleName)
	defer restoreOracleOverrides()
	var reward float64
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	result := r.oracles[oracleIdx].Run(qctx, r.exec, r.gen, r.state)
	if result.Err != nil {
		if tbl, ok := missingTableName(result.Err); ok && r.removeViewFromState(tbl) {
			if result.Details == nil {
				result.Details = map[string]any{}
			}
			result.Details["skip_reason"] = "missing_view"
			result.OK = true
			result.Err = nil
		}
	}
	if result.Err != nil && isUnknownColumnWhereErr(result.Err) {
		if result.Details == nil {
			result.Details = map[string]any{}
		}
		if _, ok := result.Details["error_reason"]; !ok {
			result.Details["error_reason"] = "unknown_column"
		}
		result.OK = false
	}
	if result.Err != nil && oracle.IsSchemaColumnMissingErr(result.Err) {
		if result.Details == nil {
			result.Details = map[string]any{}
		}
		if _, ok := result.Details["error_reason"]; !ok {
			result.Details["error_reason"] = "missing_column"
		}
		result.OK = false
	}
	if result.Err != nil && oracle.IsPlanRefMissingErr(result.Err) {
		if result.Details == nil {
			result.Details = map[string]any{}
		}
		if _, ok := result.Details["error_reason"]; !ok {
			result.Details["error_reason"] = "planner_ref_missing"
		}
		result.OK = false
	}
	skipReason := oracleSkipReason(result)
	isPanic := isPanicError(result.Err)
	reported := !result.OK || isPanic
	r.observeOracleResult(oracleName, result, skipReason, reported, isPanic)
	if r.gen.LastFeatures != nil {
		r.observeJoinCountValue(r.gen.LastFeatures.JoinCount)
		r.observeJoinSignature(r.gen.LastFeatures)
		r.observeKQELite(r.gen.LastFeatures)
	}
	r.applyResultMetrics(result)
	if result.OK {
		r.maybeObservePlan(ctx, result)
		if isPanicError(result.Err) {
			r.handleResult(ctx, result)
			reward = 1
		}
		r.updateOracleBandit(oracleIdx, reward)
		r.updateFeatureBandits(reward)
		r.tickQPG()
		r.tickKQELite()
		return reward > 0
	}
	r.handleResult(ctx, result)
	reward = 1
	r.updateOracleBandit(oracleIdx, reward)
	r.updateFeatureBandits(reward)
	r.maybeObservePlan(ctx, result)
	r.tickQPG()
	r.tickKQELite()
	return true
}

func missingTableName(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := err.Error()
	const prefix = "Table '"
	const suffix = "' doesn't exist"
	start := strings.Index(msg, prefix)
	if start < 0 {
		return "", false
	}
	start += len(prefix)
	end := strings.Index(msg[start:], suffix)
	if end < 0 {
		return "", false
	}
	name := msg[start : start+end]
	if name == "" {
		return "", false
	}
	if dot := strings.LastIndex(name, "."); dot >= 0 && dot+1 < len(name) {
		name = name[dot+1:]
	}
	return name, true
}

func (r *Runner) removeViewFromState(name string) bool {
	if r == nil || r.state == nil || name == "" {
		return false
	}
	for i, tbl := range r.state.Tables {
		if tbl.Name == name && tbl.IsView {
			r.state.Tables = append(r.state.Tables[:i], r.state.Tables[i+1:]...)
			return true
		}
	}
	return false
}
