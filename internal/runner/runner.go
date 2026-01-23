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
	genMu     sync.Mutex
	qpgMu     sync.Mutex
	sqlTotal  int64
	sqlValid  int64
	sqlExists int64
	sqlNotEx  int64
	sqlIn     int64
	sqlNotIn  int64
	impoTotal int64
	impoSkips int64
	impoTrunc int64
	impoSkipReasons map[string]int64
	impoSkipErrCodes map[string]int64
	impoLastFailSQL  string
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
		impoSkipReasons: make(map[string]int64),
		impoSkipErrCodes: make(map[string]int64),
		oracles: []oracle.Oracle{
			oracle.NoREC{},
			oracle.TLP{},
			oracle.DQP{},
			oracle.CERT{MinBaseRows: cfg.Oracles.CertMinBaseRows},
			oracle.CODDTest{},
			oracle.DQE{},
			oracle.Impo{},
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
	}
	return nil
}

func (r *Runner) initState(ctx context.Context) error {
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
	if r.cfg.Features.PlanCache && util.Chance(r.gen.Rand, r.cfg.PlanCacheProb) {
		if r.cfg.Features.NonPreparedPlanCache && util.Chance(r.gen.Rand, r.cfg.NonPreparedProb) {
			ran, bug := r.runNonPreparedPlanCache(ctx)
			if ran {
				return bug
			}
		}
		return r.runPrepared(ctx)
	}
	r.prepareFeatureWeights()
	appliedQPG := r.applyQPGWeights()
	appliedTemplate := r.applyQPGTemplateWeights()
	if appliedQPG && r.featureBandit == nil {
		defer r.clearAdaptiveWeights()
	}
	if appliedTemplate {
		defer r.clearTemplateWeights()
	}
	oracleIdx := r.pickOracle()
	var reward float64
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	result := r.oracles[oracleIdx].Run(qctx, r.exec, r.gen, r.state)
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
