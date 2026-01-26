package runner

import (
	"context"
	"fmt"
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
	predicatePairsTotal int64
	predicatePairsJoin  int64
	truthMismatches     int64
	qpgState            *qpgState
	kqeState            *kqeState
	tqsHistory          *tqs.History
	oracleStats         map[string]*oracleFunnel

	actionBandit  *util.Bandit
	oracleBandit  *util.Bandit
	dmlBandit     *util.Bandit
	actionEnabled []bool
	oracleEnabled []bool
	dmlEnabled    []bool

	featureBandit   *featureBandits
	lastFeatureArms featureArms
}

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
		r.cfg = cfg
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
	appliedOracleBias := r.applyOracleBias(oracleName)
	if appliedOracleBias {
		defer r.clearAdaptiveWeights()
	}
	restoreOracleOverrides := r.applyOracleOverrides(oracleName)
	defer restoreOracleOverrides()
	var reward float64
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	result := r.oracles[oracleIdx].Run(qctx, r.exec, r.gen, r.state)
	if result.Err != nil && isUnknownColumnWhereErr(result.Err) {
		if result.Details == nil {
			result.Details = map[string]any{}
		}
		if _, ok := result.Details["error_reason"]; !ok {
			result.Details["error_reason"] = "unknown_column"
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
