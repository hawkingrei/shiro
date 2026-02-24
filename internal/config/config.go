package config

import (
	"os"
	"strings"

	"shiro/internal/runinfo"

	"gopkg.in/yaml.v3"
)

// Config captures all runtime options for the fuzz runner.
type Config struct {
	DSN                 string             `yaml:"dsn"`
	Database            string             `yaml:"database"`
	Seed                int64              `yaml:"seed"`
	Iterations          int                `yaml:"iterations"`
	Workers             int                `yaml:"workers"`
	PlanCacheOnly       bool               `yaml:"plan_cache_only"`
	PlanCacheProb       int                `yaml:"plan_cache_prob"`
	NonPreparedProb     int                `yaml:"non_prepared_plan_cache_prob"`
	PlanCacheMeaningful bool               `yaml:"plan_cache_meaningful_predicates"`
	MaxTables           int                `yaml:"max_tables"`
	MaxJoinTables       int                `yaml:"max_join_tables"`
	MaxColumns          int                `yaml:"max_columns"`
	MaxRowsPerTable     int                `yaml:"max_rows_per_table"`
	MaxDataDumpRows     int                `yaml:"max_data_dump_rows"`
	MaxInsertStatements int                `yaml:"max_insert_statements"`
	StatementTimeoutMs  int                `yaml:"statement_timeout_ms"`
	PlanReplayer        PlanReplayer       `yaml:"plan_replayer"`
	Storage             StorageConfig      `yaml:"storage"`
	Features            Features           `yaml:"features"`
	Weights             Weights            `yaml:"weights"`
	Adaptive            Adaptive           `yaml:"adaptive"`
	Logging             Logging            `yaml:"logging"`
	Oracles             OracleConfig       `yaml:"oracles"`
	QPG                 QPGConfig          `yaml:"qpg"`
	KQE                 KQEConfig          `yaml:"kqe"`
	TQS                 TQSConfig          `yaml:"tqs"`
	Signature           SignatureConfig    `yaml:"signature"`
	Minimize            MinimizeConfig     `yaml:"minimize"`
	RunInfo             *runinfo.BasicInfo `yaml:"-"`
}

// PlanReplayer controls plan replayer dumping and download.
type PlanReplayer struct {
	Enabled             bool   `yaml:"enabled"`
	DownloadURLTemplate string `yaml:"download_url_template"`
	OutputDir           string `yaml:"output_dir"`
	TimeoutSeconds      int    `yaml:"timeout_seconds"`
	MaxDownloadBytes    int64  `yaml:"max_download_bytes"`
}

// Features toggles SQL capabilities in generation.
type Features struct {
	Joins                bool `yaml:"joins"`
	NaturalJoins         bool `yaml:"natural_joins"`
	FullJoinEmulation    bool `yaml:"full_join_emulation"`
	CTE                  bool `yaml:"cte"`
	RecursiveCTE         bool `yaml:"recursive_cte"`
	Subqueries           bool `yaml:"subqueries"`
	SetOperations        bool `yaml:"set_operations"`
	DerivedTables        bool `yaml:"derived_tables"`
	QuantifiedSubqueries bool `yaml:"quantified_subqueries"`
	Aggregates           bool `yaml:"aggregates"`
	GroupBy              bool `yaml:"group_by"`
	GroupByRollup        bool `yaml:"group_by_rollup"`
	GroupByCube          bool `yaml:"group_by_cube"`
	GroupByGroupingSets  bool `yaml:"group_by_grouping_sets"`
	Having               bool `yaml:"having"`
	OrderBy              bool `yaml:"order_by"`
	Limit                bool `yaml:"limit"`
	Distinct             bool `yaml:"distinct"`
	PlanCache            bool `yaml:"plan_cache"`
	WindowFuncs          bool `yaml:"window_funcs"`
	WindowFrames         bool `yaml:"window_frames"`
	CorrelatedSubq       bool `yaml:"correlated_subqueries"`
	IntervalArith        bool `yaml:"interval_arith"`
	Views                bool `yaml:"views"`
	ViewMax              int  `yaml:"view_max"`
	Indexes              bool `yaml:"indexes"`
	ForeignKeys          bool `yaml:"foreign_keys"`
	CheckConstraints     bool `yaml:"check_constraints"`
	PartitionTables      bool `yaml:"partition_tables"`
	NotExists            bool `yaml:"not_exists"`
	NotIn                bool `yaml:"not_in"`
	NonPreparedPlanCache bool `yaml:"non_prepared_plan_cache"`
	DSG                  bool `yaml:"dsg"`
}

// Weights controls weighted selections for actions and features.
type Weights struct {
	Actions  ActionWeights  `yaml:"actions"`
	DML      DMLWeights     `yaml:"dml"`
	Oracles  OracleWeights  `yaml:"oracles"`
	Features FeatureWeights `yaml:"features"`
}

// ActionWeights sets probabilities for DDL/DML/Query.
type ActionWeights struct {
	DDL   int `yaml:"ddl"`
	DML   int `yaml:"dml"`
	Query int `yaml:"query"`
}

// DMLWeights sets probabilities for DML operations.
type DMLWeights struct {
	Insert int `yaml:"insert"`
	Update int `yaml:"update"`
	Delete int `yaml:"delete"`
}

// OracleWeights sets probabilities for oracle selection.
type OracleWeights struct {
	NoREC       int `yaml:"norec"`
	TLP         int `yaml:"tlp"`
	EET         int `yaml:"eet"`
	DQP         int `yaml:"dqp"`
	PQS         int `yaml:"pqs"`
	CODDTest    int `yaml:"coddtest"`
	DQE         int `yaml:"dqe"`
	Impo        int `yaml:"impo"`
	GroundTruth int `yaml:"groundtruth"`
}

// FeatureWeights sets feature generation weights.
type FeatureWeights struct {
	JoinCount                int `yaml:"join_count"`
	CTECount                 int `yaml:"cte_count"`
	CTECountMax              int `yaml:"cte_count_max"`
	SubqCount                int `yaml:"subquery_count"`
	AggProb                  int `yaml:"aggregate_prob"`
	DecimalAggProb           int `yaml:"decimal_agg_prob"`
	GroupByProb              int `yaml:"group_by_prob"`
	HavingProb               int `yaml:"having_prob"`
	OrderByProb              int `yaml:"order_by_prob"`
	LimitProb                int `yaml:"limit_prob"`
	DistinctProb             int `yaml:"distinct_prob"`
	WindowProb               int `yaml:"window_prob"`
	PartitionProb            int `yaml:"partition_prob"`
	NotExistsProb            int `yaml:"not_exists_prob"`
	NotInProb                int `yaml:"not_in_prob"`
	IndexPrefixProb          int `yaml:"index_prefix_prob"`
	TemplateJoinOnlyWeight   int `yaml:"template_join_only_weight"`
	TemplateJoinFilterWeight int `yaml:"template_join_filter_weight"`
}

// Logging controls stdout logging behavior.
type Logging struct {
	Verbose               bool              `yaml:"verbose"`
	ReportIntervalSeconds int               `yaml:"report_interval_seconds"`
	LogFile               string            `yaml:"log_file"`
	Metrics               MetricsThresholds `yaml:"metrics"`
}

// TQSConfig configures TQS-style DSG + ground-truth generation.
type TQSConfig struct {
	Enabled     bool    `yaml:"enabled"`
	WideRows    int     `yaml:"wide_rows"`
	DimTables   int     `yaml:"dim_tables"`
	DepColumns  int     `yaml:"dep_columns"`
	PayloadCols int     `yaml:"payload_columns"`
	WalkLength  int     `yaml:"walk_length"`
	WalkMin     int     `yaml:"walk_min"`
	WalkMax     int     `yaml:"walk_max"`
	Gamma       float64 `yaml:"gamma"`
}

// MetricsThresholds defines alert thresholds for periodic stats logging.
type MetricsThresholds struct {
	SQLValidMinRatio           float64 `yaml:"sql_valid_min_ratio"`
	ImpoInvalidColumnsMaxRatio float64 `yaml:"impo_invalid_columns_max_ratio"`
	ImpoBaseExecFailedMaxRatio float64 `yaml:"impo_base_exec_failed_max_ratio"`
}

// OracleConfig holds oracle-specific settings.
type OracleConfig struct {
	StrictPredicates   bool              `yaml:"strict_predicates"`
	PredicateLevel     string            `yaml:"predicate_level"`
	JoinOnPolicy       string            `yaml:"join_on_policy"`
	JoinUsingProb      int               `yaml:"join_using_prob"`
	DQPExternalHints   []string          `yaml:"dqp_external_hints"`
	DQPBaseHintPick    int               `yaml:"dqp_base_hint_pick_limit"`
	DQPSetVarHintPick  int               `yaml:"dqp_set_var_hint_pick_max"`
	CertMinBaseRows    float64           `yaml:"cert_min_base_rows"`
	GroundTruthMaxRows int               `yaml:"groundtruth_max_rows"`
	ImpoMaxRows        int               `yaml:"impo_max_rows"`
	ImpoMaxMutations   int               `yaml:"impo_max_mutations"`
	ImpoTimeoutMs      int               `yaml:"impo_timeout_ms"`
	ImpoDisableStage1  bool              `yaml:"impo_disable_stage1"`
	ImpoKeepLRJoin     bool              `yaml:"impo_keep_lr_join"`
	EETRewrites        EETRewriteWeights `yaml:"eet_rewrites"`
}

// EETRewriteWeights controls rewrite selection inside the EET oracle.
type EETRewriteWeights struct {
	DoubleNot       int `yaml:"double_not"`
	AndTrue         int `yaml:"and_true"`
	OrFalse         int `yaml:"or_false"`
	NumericIdentity int `yaml:"numeric_identity"`
	StringIdentity  int `yaml:"string_identity"`
	DateIdentity    int `yaml:"date_identity"`
}

// QPGConfig configures query plan guidance.
type QPGConfig struct {
	Enabled                 bool                      `yaml:"enabled"`
	ExplainFormat           string                    `yaml:"explain_format"`
	MutationProb            int                       `yaml:"mutation_prob"`
	SeenSQLTTLSeconds       int                       `yaml:"seen_sql_ttl_seconds"`
	SeenSQLMax              int                       `yaml:"seen_sql_max"`
	SeenSQLSweepSeconds     int                       `yaml:"seen_sql_sweep_seconds"`
	NoJoinThreshold         int                       `yaml:"no_join_threshold"`
	NoAggThreshold          int                       `yaml:"no_agg_threshold"`
	NoNewPlanThreshold      int                       `yaml:"no_new_plan_threshold"`
	NoNewOpSigThreshold     int                       `yaml:"no_new_op_sig_threshold"`
	NoNewShapeThreshold     int                       `yaml:"no_new_shape_threshold"`
	NoNewJoinTypeThreshold  int                       `yaml:"no_new_join_type_threshold"`
	NoNewJoinOrderThreshold int                       `yaml:"no_new_join_order_threshold"`
	OverrideTTL             int                       `yaml:"override_ttl"`
	TemplateOverride        QPGTemplateOverrideConfig `yaml:"template_override"`
}

// QPGTemplateOverrideConfig configures template-level QPG adaptive overrides.
type QPGTemplateOverrideConfig struct {
	NoNewJoinOrderThreshold int `yaml:"no_new_join_order_threshold"`
	NoNewShapeThreshold     int `yaml:"no_new_shape_threshold"`
	NoAggThreshold          int `yaml:"no_agg_threshold"`
	NoNewPlanThreshold      int `yaml:"no_new_plan_threshold"`
	JoinWeightBoost         int `yaml:"join_weight_boost"`
	AggWeightBoost          int `yaml:"agg_weight_boost"`
	SemiWeightBoost         int `yaml:"semi_weight_boost"`
	EnabledProb             int `yaml:"enabled_prob"`
	OverrideTTL             int `yaml:"override_ttl"`
}

// KQEConfig controls lightweight join-coverage guidance.
type KQEConfig struct {
	Enabled bool `yaml:"enabled"`
}

// SignatureConfig controls signature rounding for comparisons.
type SignatureConfig struct {
	RoundScale          int `yaml:"round_scale"`
	PlanCacheRoundScale int `yaml:"plan_cache_round_scale"`
}

// MinimizeConfig configures case minimization.
type MinimizeConfig struct {
	Enabled        bool `yaml:"enabled"`
	MaxRounds      int  `yaml:"max_rounds"`
	TimeoutSeconds int  `yaml:"timeout_seconds"`
	MergeInserts   bool `yaml:"merge_inserts"`
}

// Adaptive configures bandit-based adaptation.
type Adaptive struct {
	Enabled        bool    `yaml:"enabled"`
	UCBExploration float64 `yaml:"ucb_exploration"`
	WindowSize     int     `yaml:"window_size"`
	AdaptActions   bool    `yaml:"adapt_actions"`
	AdaptOracles   bool    `yaml:"adapt_oracles"`
	AdaptDML       bool    `yaml:"adapt_dml"`
	AdaptFeatures  bool    `yaml:"adapt_features"`
}

// StorageConfig holds external storage settings.
type StorageConfig struct {
	S3  S3Config  `yaml:"s3"`
	GCS GCSConfig `yaml:"gcs"`
}

// CloudEnabled reports whether any cloud storage backend is enabled.
func (s StorageConfig) CloudEnabled() bool {
	return s.GCS.Enabled || s.S3.Enabled
}

// S3Config configures S3 uploads (legacy and S3-compatible endpoints).
type S3Config struct {
	Enabled         bool   `yaml:"enabled"`
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	Prefix          string `yaml:"prefix"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	SessionToken    string `yaml:"session_token"`
	UsePathStyle    bool   `yaml:"use_path_style"`
}

// GCSConfig configures GCS uploads.
type GCSConfig struct {
	Enabled         bool   `yaml:"enabled"`
	Bucket          string `yaml:"bucket"`
	Prefix          string `yaml:"prefix"`
	CredentialsFile string `yaml:"credentials_file"`
}

// Load reads configuration from a YAML file.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	cfg := defaultConfig()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	normalizeConfig(&cfg)
	cfg.RunInfo = runinfo.FromEnv()
	return cfg, nil
}

const (
	dqpBaseHintPickLimitDefault = 3
	dqpSetVarHintPickMaxDefault = 3

	qpgNoJoinThresholdDefault         = 3
	qpgNoAggThresholdDefault          = 3
	qpgNoNewPlanThresholdDefault      = 5
	qpgNoNewOpSigThresholdDefault     = 4
	qpgNoNewShapeThresholdDefault     = 4
	qpgNoNewJoinTypeThresholdDefault  = 3
	qpgNoNewJoinOrderThresholdDefault = 3
	qpgOverrideTTLDefault             = 5

	qpgTemplateNoNewJoinOrderThresholdDefault = 3
	qpgTemplateNoNewShapeThresholdDefault     = 4
	qpgTemplateNoAggThresholdDefault          = 3
	qpgTemplateNoNewPlanThresholdDefault      = 5
	qpgTemplateJoinWeightBoostDefault         = 6
	qpgTemplateAggWeightBoostDefault          = 6
	qpgTemplateSemiWeightBoostDefault         = 5
	qpgTemplateEnabledProbDefault             = 55
	qpgTemplateOverrideTTLDefault             = 5
)

func normalizeConfig(cfg *Config) {
	if cfg.Adaptive.Enabled && !cfg.Adaptive.AdaptActions && !cfg.Adaptive.AdaptOracles && !cfg.Adaptive.AdaptDML && !cfg.Adaptive.AdaptFeatures {
		cfg.Adaptive.AdaptOracles = true
	}
	if cfg.PlanCacheProb <= 0 {
		cfg.PlanCacheProb = 50
	}
	if cfg.NonPreparedProb <= 0 {
		cfg.NonPreparedProb = 50
	}
	if cfg.MaxJoinTables > 0 && cfg.Weights.Features.JoinCount > cfg.MaxJoinTables {
		cfg.Weights.Features.JoinCount = cfg.MaxJoinTables
	}
	if cfg.Database != "" {
		cfg.DSN = ensureDatabaseInDSN(cfg.DSN, cfg.Database)
	}
	if cfg.Features.ViewMax <= 0 {
		cfg.Features.ViewMax = 3
	}
	if cfg.TQS.Enabled {
		if cfg.Weights.Actions.Query <= 0 {
			cfg.Weights.Actions.Query = 1
		}
	}
	if cfg.Weights.Features.TemplateJoinOnlyWeight < 0 {
		cfg.Weights.Features.TemplateJoinOnlyWeight = 0
	}
	if cfg.Weights.Features.TemplateJoinFilterWeight < 0 {
		cfg.Weights.Features.TemplateJoinFilterWeight = 0
	}
	if cfg.Weights.Features.TemplateJoinOnlyWeight == 0 && cfg.Weights.Features.TemplateJoinFilterWeight == 0 {
		cfg.Weights.Features.TemplateJoinOnlyWeight = 4
		cfg.Weights.Features.TemplateJoinFilterWeight = 6
	}
	if cfg.Oracles.DQPBaseHintPick <= 0 {
		cfg.Oracles.DQPBaseHintPick = dqpBaseHintPickLimitDefault
	}
	if cfg.Oracles.DQPSetVarHintPick <= 0 {
		cfg.Oracles.DQPSetVarHintPick = dqpSetVarHintPickMaxDefault
	}
	if cfg.QPG.NoJoinThreshold <= 0 {
		cfg.QPG.NoJoinThreshold = qpgNoJoinThresholdDefault
	}
	if cfg.QPG.NoAggThreshold <= 0 {
		cfg.QPG.NoAggThreshold = qpgNoAggThresholdDefault
	}
	if cfg.QPG.NoNewPlanThreshold <= 0 {
		cfg.QPG.NoNewPlanThreshold = qpgNoNewPlanThresholdDefault
	}
	if cfg.QPG.NoNewOpSigThreshold <= 0 {
		cfg.QPG.NoNewOpSigThreshold = qpgNoNewOpSigThresholdDefault
	}
	if cfg.QPG.NoNewShapeThreshold <= 0 {
		cfg.QPG.NoNewShapeThreshold = qpgNoNewShapeThresholdDefault
	}
	if cfg.QPG.NoNewJoinTypeThreshold <= 0 {
		cfg.QPG.NoNewJoinTypeThreshold = qpgNoNewJoinTypeThresholdDefault
	}
	if cfg.QPG.NoNewJoinOrderThreshold <= 0 {
		cfg.QPG.NoNewJoinOrderThreshold = qpgNoNewJoinOrderThresholdDefault
	}
	if cfg.QPG.OverrideTTL <= 0 {
		cfg.QPG.OverrideTTL = qpgOverrideTTLDefault
	}
	if cfg.QPG.TemplateOverride.NoNewJoinOrderThreshold <= 0 {
		cfg.QPG.TemplateOverride.NoNewJoinOrderThreshold = qpgTemplateNoNewJoinOrderThresholdDefault
	}
	if cfg.QPG.TemplateOverride.NoNewShapeThreshold <= 0 {
		cfg.QPG.TemplateOverride.NoNewShapeThreshold = qpgTemplateNoNewShapeThresholdDefault
	}
	if cfg.QPG.TemplateOverride.NoAggThreshold <= 0 {
		cfg.QPG.TemplateOverride.NoAggThreshold = qpgTemplateNoAggThresholdDefault
	}
	if cfg.QPG.TemplateOverride.NoNewPlanThreshold <= 0 {
		cfg.QPG.TemplateOverride.NoNewPlanThreshold = qpgTemplateNoNewPlanThresholdDefault
	}
	if cfg.QPG.TemplateOverride.JoinWeightBoost <= 0 {
		cfg.QPG.TemplateOverride.JoinWeightBoost = qpgTemplateJoinWeightBoostDefault
	}
	if cfg.QPG.TemplateOverride.AggWeightBoost <= 0 {
		cfg.QPG.TemplateOverride.AggWeightBoost = qpgTemplateAggWeightBoostDefault
	}
	if cfg.QPG.TemplateOverride.SemiWeightBoost <= 0 {
		cfg.QPG.TemplateOverride.SemiWeightBoost = qpgTemplateSemiWeightBoostDefault
	}
	if cfg.QPG.TemplateOverride.EnabledProb <= 0 {
		cfg.QPG.TemplateOverride.EnabledProb = qpgTemplateEnabledProbDefault
	}
	if cfg.QPG.TemplateOverride.EnabledProb > 100 {
		cfg.QPG.TemplateOverride.EnabledProb = 100
	}
	if cfg.QPG.TemplateOverride.OverrideTTL <= 0 {
		cfg.QPG.TemplateOverride.OverrideTTL = qpgTemplateOverrideTTLDefault
	}
}

func ensureDatabaseInDSN(dsn string, dbName string) string {
	if dsn == "" || dbName == "" {
		return dsn
	}
	slash := strings.Index(dsn, "/")
	if slash < 0 {
		return dsn
	}
	query := strings.Index(dsn[slash+1:], "?")
	if query >= 0 {
		query = slash + 1 + query
	}
	afterSlash := dsn[slash+1:]
	if query >= 0 {
		afterSlash = dsn[slash+1 : query]
	}
	if strings.TrimSpace(afterSlash) != "" {
		return dsn
	}
	if query >= 0 {
		return dsn[:slash+1] + dbName + dsn[query:]
	}
	return dsn + dbName
}

// UpdateDatabaseInDSN replaces the database name in the DSN path with dbName.
// It preserves query parameters, if any.
func UpdateDatabaseInDSN(dsn string, dbName string) string {
	if dsn == "" || dbName == "" {
		return dsn
	}
	slash := strings.Index(dsn, "/")
	if slash < 0 {
		return dsn
	}
	query := strings.Index(dsn[slash+1:], "?")
	if query >= 0 {
		query = slash + 1 + query
		return dsn[:slash+1] + dbName + dsn[query:]
	}
	return dsn[:slash+1] + dbName
}

// AdminDSN strips the database name from a DSN while preserving query parameters.
func AdminDSN(dsn string) string {
	if dsn == "" {
		return dsn
	}
	slash := strings.Index(dsn, "/")
	if slash < 0 {
		return dsn
	}
	query := strings.Index(dsn[slash+1:], "?")
	if query >= 0 {
		query = slash + 1 + query
		return dsn[:slash+1] + dsn[query:]
	}
	return dsn[:slash+1]
}

func defaultConfig() Config {
	return Config{
		DSN:                 "root:@tcp(127.0.0.1:4000)/",
		Database:            "shiro_fuzz",
		Iterations:          1000,
		Workers:             1,
		PlanCacheProb:       50,
		NonPreparedProb:     50,
		PlanCacheMeaningful: true,
		MaxTables:           5,
		MaxJoinTables:       15,
		MaxColumns:          8,
		MaxRowsPerTable:     50,
		MaxDataDumpRows:     50,
		MaxInsertStatements: 200,
		StatementTimeoutMs:  15000,
		Features: Features{
			Views:                true,
			ViewMax:              3,
			PartitionTables:      true,
			NonPreparedPlanCache: true,
			NotExists:            true,
			NotIn:                true,
			CorrelatedSubq:       true,
		},
		TQS: TQSConfig{
			Enabled:     false,
			WideRows:    50,
			DimTables:   3,
			DepColumns:  2,
			PayloadCols: 2,
			WalkLength:  4,
			WalkMin:     0,
			WalkMax:     0,
			Gamma:       0.2,
		},
		PlanReplayer: PlanReplayer{
			OutputDir:           "reports",
			DownloadURLTemplate: "http://127.0.0.1:10080/plan_replayer/dump/%s.zip",
			TimeoutSeconds:      30,
			MaxDownloadBytes:    50 << 20,
		},
		Weights: Weights{
			Actions:  ActionWeights{DDL: 1, DML: 3, Query: 6},
			DML:      DMLWeights{Insert: 3, Update: 1, Delete: 1},
			Oracles:  OracleWeights{NoREC: 4, TLP: 3, EET: 2, DQP: 3, PQS: 2, CODDTest: 2, DQE: 2, Impo: 2, GroundTruth: 5},
			Features: FeatureWeights{JoinCount: 5, CTECount: 4, CTECountMax: 3, SubqCount: 5, AggProb: 50, DecimalAggProb: 70, GroupByProb: 30, HavingProb: 20, OrderByProb: 40, LimitProb: 40, DistinctProb: 20, WindowProb: 20, PartitionProb: 30, NotExistsProb: 40, NotInProb: 40, IndexPrefixProb: 30, TemplateJoinOnlyWeight: 4, TemplateJoinFilterWeight: 6},
		},
		Logging: Logging{
			ReportIntervalSeconds: 30,
			LogFile:               "logs/shiro.log",
			Metrics: MetricsThresholds{
				SQLValidMinRatio:           0.95,
				ImpoInvalidColumnsMaxRatio: 0.05,
				ImpoBaseExecFailedMaxRatio: 0.02,
			},
		},
		Oracles: OracleConfig{
			StrictPredicates:   true,
			PredicateLevel:     "strict",
			JoinOnPolicy:       "simple",
			JoinUsingProb:      -1,
			DQPBaseHintPick:    dqpBaseHintPickLimitDefault,
			DQPSetVarHintPick:  dqpSetVarHintPickMaxDefault,
			CertMinBaseRows:    20,
			GroundTruthMaxRows: 50,
			ImpoMaxRows:        50,
			ImpoMaxMutations:   64,
			ImpoTimeoutMs:      2000,
			EETRewrites:        EETRewriteWeights{DoubleNot: 4, AndTrue: 3, OrFalse: 3, NumericIdentity: 2, StringIdentity: 2, DateIdentity: 2},
		},
		Adaptive: Adaptive{Enabled: true, UCBExploration: 1.5, WindowSize: 50000},
		QPG: QPGConfig{
			Enabled:                 true,
			ExplainFormat:           "brief",
			MutationProb:            30,
			SeenSQLTTLSeconds:       120,
			SeenSQLMax:              8192,
			SeenSQLSweepSeconds:     600,
			NoJoinThreshold:         qpgNoJoinThresholdDefault,
			NoAggThreshold:          qpgNoAggThresholdDefault,
			NoNewPlanThreshold:      qpgNoNewPlanThresholdDefault,
			NoNewOpSigThreshold:     qpgNoNewOpSigThresholdDefault,
			NoNewShapeThreshold:     qpgNoNewShapeThresholdDefault,
			NoNewJoinTypeThreshold:  qpgNoNewJoinTypeThresholdDefault,
			NoNewJoinOrderThreshold: qpgNoNewJoinOrderThresholdDefault,
			OverrideTTL:             qpgOverrideTTLDefault,
			TemplateOverride: QPGTemplateOverrideConfig{
				NoNewJoinOrderThreshold: qpgTemplateNoNewJoinOrderThresholdDefault,
				NoNewShapeThreshold:     qpgTemplateNoNewShapeThresholdDefault,
				NoAggThreshold:          qpgTemplateNoAggThresholdDefault,
				NoNewPlanThreshold:      qpgTemplateNoNewPlanThresholdDefault,
				JoinWeightBoost:         qpgTemplateJoinWeightBoostDefault,
				AggWeightBoost:          qpgTemplateAggWeightBoostDefault,
				SemiWeightBoost:         qpgTemplateSemiWeightBoostDefault,
				EnabledProb:             qpgTemplateEnabledProbDefault,
				OverrideTTL:             qpgTemplateOverrideTTLDefault,
			},
		},
		KQE: KQEConfig{
			Enabled: true,
		},
		Signature: SignatureConfig{
			RoundScale:          6,
			PlanCacheRoundScale: 4,
		},
		Minimize: MinimizeConfig{
			Enabled:        true,
			MaxRounds:      16,
			TimeoutSeconds: 60,
			MergeInserts:   true,
		},
	}
}
