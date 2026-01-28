package config

import (
	"os"
	"strings"

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
	PlanCacheProb       int             `yaml:"plan_cache_prob"`
	NonPreparedProb     int             `yaml:"non_prepared_plan_cache_prob"`
	PlanCacheMeaningful bool            `yaml:"plan_cache_meaningful_predicates"`
	MaxTables           int             `yaml:"max_tables"`
	MaxJoinTables       int             `yaml:"max_join_tables"`
	MaxColumns          int             `yaml:"max_columns"`
	MaxRowsPerTable     int             `yaml:"max_rows_per_table"`
	MaxDataDumpRows     int             `yaml:"max_data_dump_rows"`
	MaxInsertStatements int             `yaml:"max_insert_statements"`
	StatementTimeoutMs  int             `yaml:"statement_timeout_ms"`
	PlanReplayer        PlanReplayer    `yaml:"plan_replayer"`
	Storage             StorageConfig   `yaml:"storage"`
	Features            Features        `yaml:"features"`
	Weights             Weights         `yaml:"weights"`
	Adaptive            Adaptive        `yaml:"adaptive"`
	Logging             Logging         `yaml:"logging"`
	Oracles             OracleConfig    `yaml:"oracles"`
	QPG                 QPGConfig       `yaml:"qpg"`
	KQE                 KQEConfig       `yaml:"kqe"`
	TQS                 TQSConfig       `yaml:"tqs"`
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
	DQP         int `yaml:"dqp"`
	CERT        int `yaml:"cert"`
	CODDTest    int `yaml:"coddtest"`
	DQE         int `yaml:"dqe"`
	Impo        int `yaml:"impo"`
	GroundTruth int `yaml:"groundtruth"`
}

// FeatureWeights sets feature generation weights.
type FeatureWeights struct {
	JoinCount       int `yaml:"join_count"`
	CTECount        int `yaml:"cte_count"`
	SubqCount       int `yaml:"subquery_count"`
	AggProb         int `yaml:"aggregate_prob"`
	DecimalAggProb  int `yaml:"decimal_agg_prob"`
	GroupByProb     int `yaml:"group_by_prob"`
	HavingProb      int `yaml:"having_prob"`
	OrderByProb     int `yaml:"order_by_prob"`
	LimitProb       int `yaml:"limit_prob"`
	DistinctProb    int `yaml:"distinct_prob"`
	WindowProb      int `yaml:"window_prob"`
	PartitionProb   int `yaml:"partition_prob"`
	NotExistsProb   int `yaml:"not_exists_prob"`
	NotInProb       int `yaml:"not_in_prob"`
	IndexPrefixProb int `yaml:"index_prefix_prob"`
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
	StrictPredicates   bool    `yaml:"strict_predicates"`
	PredicateLevel     string  `yaml:"predicate_level"`
	CertMinBaseRows    float64 `yaml:"cert_min_base_rows"`
	GroundTruthMaxRows int     `yaml:"groundtruth_max_rows"`
	ImpoMaxRows        int     `yaml:"impo_max_rows"`
	ImpoMaxMutations   int     `yaml:"impo_max_mutations"`
	ImpoTimeoutMs      int     `yaml:"impo_timeout_ms"`
	ImpoDisableStage1  bool    `yaml:"impo_disable_stage1"`
	ImpoKeepLRJoin     bool    `yaml:"impo_keep_lr_join"`
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
	if cfg.TQS.Enabled {
		cfg.Features.DSG = true
		cfg.Weights.Actions.DML = 0
		if cfg.Weights.Actions.Query <= 0 {
			cfg.Weights.Actions.Query = 1
		}
		if cfg.Features.Views {
			if cfg.Weights.Actions.DDL <= 0 {
				cfg.Weights.Actions.DDL = 1
			}
		} else {
			cfg.Weights.Actions.DDL = 0
		}
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
			Oracles:  OracleWeights{NoREC: 4, TLP: 3, DQP: 3, CERT: 1, CODDTest: 2, DQE: 2, Impo: 2, GroundTruth: 5},
			Features: FeatureWeights{JoinCount: 5, CTECount: 4, SubqCount: 5, AggProb: 50, DecimalAggProb: 70, GroupByProb: 30, HavingProb: 20, OrderByProb: 40, LimitProb: 40, DistinctProb: 20, WindowProb: 20, PartitionProb: 30, NotExistsProb: 40, NotInProb: 40, IndexPrefixProb: 30},
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
		Oracles:  OracleConfig{StrictPredicates: true, PredicateLevel: "strict", CertMinBaseRows: 20, GroundTruthMaxRows: 50, ImpoMaxRows: 50, ImpoMaxMutations: 64, ImpoTimeoutMs: 2000},
		Adaptive: Adaptive{Enabled: true, UCBExploration: 1.5, WindowSize: 50000},
		QPG: QPGConfig{
			Enabled:             false,
			ExplainFormat:       "brief",
			MutationProb:        30,
			SeenSQLTTLSeconds:   120,
			SeenSQLMax:          8192,
			SeenSQLSweepSeconds: 600,
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
