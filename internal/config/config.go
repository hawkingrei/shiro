package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config captures all runtime options for the fuzz runner.
type Config struct {
	DSN                 string          `yaml:"dsn"`
	Database            string          `yaml:"database"`
	Seed                int64           `yaml:"seed"`
	Iterations          int             `yaml:"iterations"`
	Workers             int             `yaml:"workers"`
	PlanCacheOnly       bool            `yaml:"plan_cache_only"`
	MaxTables           int             `yaml:"max_tables"`
	MaxJoinTables       int             `yaml:"max_join_tables"`
	MaxColumns          int             `yaml:"max_columns"`
	MaxRowsPerTable     int             `yaml:"max_rows_per_table"`
	MaxDataDumpRows     int             `yaml:"max_data_dump_rows"`
	MaxInsertStatements int             `yaml:"max_insert_statements"`
	StatementTimeoutMs  int             `yaml:"statement_timeout_ms"`
	PlanReplayer        PlanReplayer    `yaml:"plan_replayer"`
	DQP                 DQPConfig       `yaml:"dqp"`
	Storage             StorageConfig   `yaml:"storage"`
	Features            Features        `yaml:"features"`
	Weights             Weights         `yaml:"weights"`
	Adaptive            Adaptive        `yaml:"adaptive"`
	Logging             Logging         `yaml:"logging"`
	Oracles             OracleConfig    `yaml:"oracles"`
	QPG                 QPGConfig       `yaml:"qpg"`
	Signature           SignatureConfig `yaml:"signature"`
	Minimize            MinimizeConfig  `yaml:"minimize"`
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
	CTE                  bool `yaml:"cte"`
	Subqueries           bool `yaml:"subqueries"`
	Aggregates           bool `yaml:"aggregates"`
	GroupBy              bool `yaml:"group_by"`
	Having               bool `yaml:"having"`
	OrderBy              bool `yaml:"order_by"`
	Limit                bool `yaml:"limit"`
	Distinct             bool `yaml:"distinct"`
	PlanCache            bool `yaml:"plan_cache"`
	WindowFuncs          bool `yaml:"window_funcs"`
	CorrelatedSubq       bool `yaml:"correlated_subqueries"`
	Views                bool `yaml:"views"`
	Indexes              bool `yaml:"indexes"`
	ForeignKeys          bool `yaml:"foreign_keys"`
	CheckConstraints     bool `yaml:"check_constraints"`
	PartitionTables      bool `yaml:"partition_tables"`
	NotExists            bool `yaml:"not_exists"`
	NotIn                bool `yaml:"not_in"`
	NonPreparedPlanCache bool `yaml:"non_prepared_plan_cache"`
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
	NoREC    int `yaml:"norec"`
	TLP      int `yaml:"tlp"`
	DQP      int `yaml:"dqp"`
	CERT     int `yaml:"cert"`
	CODDTest int `yaml:"coddtest"`
	DQE      int `yaml:"dqe"`
}

// FeatureWeights sets feature generation weights.
type FeatureWeights struct {
	JoinCount      int `yaml:"join_count"`
	CTECount       int `yaml:"cte_count"`
	SubqCount      int `yaml:"subquery_count"`
	AggProb        int `yaml:"aggregate_prob"`
	DecimalAggProb int `yaml:"decimal_agg_prob"`
	GroupByProb    int `yaml:"group_by_prob"`
	HavingProb     int `yaml:"having_prob"`
	OrderByProb    int `yaml:"order_by_prob"`
	LimitProb      int `yaml:"limit_prob"`
	DistinctProb   int `yaml:"distinct_prob"`
	WindowProb     int `yaml:"window_prob"`
	PartitionProb  int `yaml:"partition_prob"`
	NotExistsProb  int `yaml:"not_exists_prob"`
	NotInProb      int `yaml:"not_in_prob"`
}

// Logging controls stdout logging behavior.
type Logging struct {
	Verbose               bool `yaml:"verbose"`
	ReportIntervalSeconds int  `yaml:"report_interval_seconds"`
}

// OracleConfig holds oracle-specific settings.
type OracleConfig struct {
	StrictPredicates bool   `yaml:"strict_predicates"`
	PredicateLevel   string `yaml:"predicate_level"`
}

// QPGConfig configures query plan guidance.
type QPGConfig struct {
	Enabled             bool   `yaml:"enabled"`
	ExplainFormat       string `yaml:"explain_format"`
	MutationProb        int    `yaml:"mutation_prob"`
	SeenSQLTTLSeconds   int    `yaml:"seen_sql_ttl_seconds"`
	SeenSQLMax          int    `yaml:"seen_sql_max"`
	SeenSQLSweepSeconds int    `yaml:"seen_sql_sweep_seconds"`
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
	AdaptActions   bool    `yaml:"adapt_actions"`
	AdaptOracles   bool    `yaml:"adapt_oracles"`
	AdaptDML       bool    `yaml:"adapt_dml"`
	AdaptFeatures  bool    `yaml:"adapt_features"`
}

// DQPConfig configures DQP hints and variables.
type DQPConfig struct {
	HintSets  []string `yaml:"hint_sets"`
	Variables []string `yaml:"variables"`
}

// StorageConfig holds external storage settings.
type StorageConfig struct {
	S3 S3Config `yaml:"s3"`
}

// S3Config configures S3 uploads.
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
	return cfg, nil
}

func normalizeConfig(cfg *Config) {
	if cfg.Adaptive.Enabled && !cfg.Adaptive.AdaptActions && !cfg.Adaptive.AdaptOracles && !cfg.Adaptive.AdaptDML && !cfg.Adaptive.AdaptFeatures {
		cfg.Adaptive.AdaptOracles = true
	}
}

func defaultConfig() Config {
	return Config{
		DSN:                 "root@tcp(127.0.0.1:4000)/",
		Database:            "shiro_fuzz",
		Iterations:          1000,
		Workers:             1,
		MaxTables:           5,
		MaxJoinTables:       4,
		MaxColumns:          8,
		MaxRowsPerTable:     50,
		MaxDataDumpRows:     50,
		MaxInsertStatements: 200,
		StatementTimeoutMs:  15000,
		Features: Features{
			PartitionTables:      true,
			NonPreparedPlanCache: true,
			NotExists:            true,
			NotIn:                true,
		},
		PlanReplayer: PlanReplayer{
			OutputDir:           "reports",
			DownloadURLTemplate: "http://127.0.0.1:10080/plan_replayer/dump/%s.zip",
			TimeoutSeconds:      30,
			MaxDownloadBytes:    50 << 20,
		},
		Weights: Weights{
			Actions:  ActionWeights{DDL: 1, DML: 3, Query: 6},
			DML:      DMLWeights{Insert: 3, Update: 2, Delete: 1},
			Oracles:  OracleWeights{NoREC: 4, TLP: 3, DQP: 3, CERT: 2, CODDTest: 2, DQE: 2},
			Features: FeatureWeights{JoinCount: 3, CTECount: 2, SubqCount: 3, AggProb: 40, DecimalAggProb: 70, GroupByProb: 30, HavingProb: 20, OrderByProb: 40, LimitProb: 40, DistinctProb: 20, WindowProb: 10, PartitionProb: 30, NotExistsProb: 40, NotInProb: 40},
		},
		Logging:  Logging{ReportIntervalSeconds: 30},
		Oracles:  OracleConfig{StrictPredicates: true, PredicateLevel: "strict"},
		Adaptive: Adaptive{UCBExploration: 1.5},
		QPG: QPGConfig{
			Enabled:             false,
			ExplainFormat:       "brief",
			MutationProb:        30,
			SeenSQLTTLSeconds:   60,
			SeenSQLMax:          4096,
			SeenSQLSweepSeconds: 300,
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
		DQP: DQPConfig{Variables: []string{
			"tidb_opt_enable_hash_join=ON",
			"tidb_opt_enable_hash_join=OFF",
			"tidb_opt_enable_late_materialization=ON",
			"tidb_opt_enable_late_materialization=OFF",
			"tidb_opt_enable_non_eval_scalar_subquery=ON",
			"tidb_opt_enable_non_eval_scalar_subquery=OFF",
			"tidb_opt_enable_semi_join_rewrite=ON",
			"tidb_opt_enable_semi_join_rewrite=OFF",
			"tidb_opt_enable_no_decorrelate_in_select=ON",
			"tidb_opt_enable_no_decorrelate_in_select=OFF",
			"tidb_opt_force_inline_cte=ON",
			"tidb_opt_force_inline_cte=OFF",
			"tidb_opt_join_reorder_threshold=0",
			"tidb_opt_join_reorder_threshold=8",
		}},
	}
}
