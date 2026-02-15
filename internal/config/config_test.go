package config

import (
	"os"
	"strings"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := tmp.WriteString(""); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}

	cfg, err := Load(tmp.Name())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.DSN == "" {
		t.Fatalf("unexpected DSN: %s", cfg.DSN)
	}
	if cfg.PlanReplayer.DownloadURLTemplate == "" {
		t.Fatalf("expected default plan replayer download url")
	}
	if !strings.Contains(cfg.PlanReplayer.DownloadURLTemplate, "plan_replayer/dump") {
		t.Fatalf("unexpected plan replayer url template: %s", cfg.PlanReplayer.DownloadURLTemplate)
	}
	if cfg.MaxJoinTables != 15 {
		t.Fatalf("unexpected max join tables: %d", cfg.MaxJoinTables)
	}
	if cfg.Logging.ReportIntervalSeconds != 30 {
		t.Fatalf("unexpected report interval: %d", cfg.Logging.ReportIntervalSeconds)
	}
	if cfg.Logging.LogFile != "logs/shiro.log" {
		t.Fatalf("unexpected log file: %s", cfg.Logging.LogFile)
	}
}

func TestLoadOverrides(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	content := `database: test_db
plan_replayer:
  download_url_template: "http://example.com/%s.zip"
`
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}

	cfg, err := Load(tmp.Name())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Database != "test_db" {
		t.Fatalf("unexpected database: %s", cfg.Database)
	}
	if cfg.PlanReplayer.DownloadURLTemplate != "http://example.com/%s.zip" {
		t.Fatalf("unexpected download url template: %s", cfg.PlanReplayer.DownloadURLTemplate)
	}
}

func TestNormalizeTemplateJoinPredicateWeights(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	content := `weights:
  features:
    template_join_only_weight: -1
    template_join_filter_weight: 0
`
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	cfg, err := Load(tmp.Name())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Weights.Features.TemplateJoinOnlyWeight != 4 {
		t.Fatalf("unexpected template_join_only_weight: %d", cfg.Weights.Features.TemplateJoinOnlyWeight)
	}
	if cfg.Weights.Features.TemplateJoinFilterWeight != 6 {
		t.Fatalf("unexpected template_join_filter_weight: %d", cfg.Weights.Features.TemplateJoinFilterWeight)
	}
}

func TestNormalizeQPGTemplateOverride(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	content := `qpg:
  template_override:
    no_new_join_order_threshold: 0
    no_new_shape_threshold: -1
    no_agg_threshold: 0
    no_new_plan_threshold: -2
    join_weight_boost: 0
    agg_weight_boost: -1
    semi_weight_boost: 0
    enabled_prob: 101
    override_ttl: 0
`
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	cfg, err := Load(tmp.Name())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	override := cfg.QPG.TemplateOverride
	if override.NoNewJoinOrderThreshold != qpgTemplateNoNewJoinOrderThresholdDefault {
		t.Fatalf("unexpected no_new_join_order_threshold: %d", override.NoNewJoinOrderThreshold)
	}
	if override.NoNewShapeThreshold != qpgTemplateNoNewShapeThresholdDefault {
		t.Fatalf("unexpected no_new_shape_threshold: %d", override.NoNewShapeThreshold)
	}
	if override.NoAggThreshold != qpgTemplateNoAggThresholdDefault {
		t.Fatalf("unexpected no_agg_threshold: %d", override.NoAggThreshold)
	}
	if override.NoNewPlanThreshold != qpgTemplateNoNewPlanThresholdDefault {
		t.Fatalf("unexpected no_new_plan_threshold: %d", override.NoNewPlanThreshold)
	}
	if override.JoinWeightBoost != qpgTemplateJoinWeightBoostDefault {
		t.Fatalf("unexpected join_weight_boost: %d", override.JoinWeightBoost)
	}
	if override.AggWeightBoost != qpgTemplateAggWeightBoostDefault {
		t.Fatalf("unexpected agg_weight_boost: %d", override.AggWeightBoost)
	}
	if override.SemiWeightBoost != qpgTemplateSemiWeightBoostDefault {
		t.Fatalf("unexpected semi_weight_boost: %d", override.SemiWeightBoost)
	}
	if override.EnabledProb != 100 {
		t.Fatalf("unexpected enabled_prob: %d", override.EnabledProb)
	}
	if override.OverrideTTL != qpgTemplateOverrideTTLDefault {
		t.Fatalf("unexpected override_ttl: %d", override.OverrideTTL)
	}
}

func TestNormalizeQPGThresholds(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	content := `qpg:
  no_join_threshold: 0
  no_agg_threshold: -1
  no_new_plan_threshold: 0
  no_new_op_sig_threshold: -1
  no_new_shape_threshold: 0
  no_new_join_type_threshold: -2
  no_new_join_order_threshold: 0
  override_ttl: -1
`
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	cfg, err := Load(tmp.Name())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.QPG.NoJoinThreshold != qpgNoJoinThresholdDefault {
		t.Fatalf("unexpected no_join_threshold: %d", cfg.QPG.NoJoinThreshold)
	}
	if cfg.QPG.NoAggThreshold != qpgNoAggThresholdDefault {
		t.Fatalf("unexpected no_agg_threshold: %d", cfg.QPG.NoAggThreshold)
	}
	if cfg.QPG.NoNewPlanThreshold != qpgNoNewPlanThresholdDefault {
		t.Fatalf("unexpected no_new_plan_threshold: %d", cfg.QPG.NoNewPlanThreshold)
	}
	if cfg.QPG.NoNewOpSigThreshold != qpgNoNewOpSigThresholdDefault {
		t.Fatalf("unexpected no_new_op_sig_threshold: %d", cfg.QPG.NoNewOpSigThreshold)
	}
	if cfg.QPG.NoNewShapeThreshold != qpgNoNewShapeThresholdDefault {
		t.Fatalf("unexpected no_new_shape_threshold: %d", cfg.QPG.NoNewShapeThreshold)
	}
	if cfg.QPG.NoNewJoinTypeThreshold != qpgNoNewJoinTypeThresholdDefault {
		t.Fatalf("unexpected no_new_join_type_threshold: %d", cfg.QPG.NoNewJoinTypeThreshold)
	}
	if cfg.QPG.NoNewJoinOrderThreshold != qpgNoNewJoinOrderThresholdDefault {
		t.Fatalf("unexpected no_new_join_order_threshold: %d", cfg.QPG.NoNewJoinOrderThreshold)
	}
	if cfg.QPG.OverrideTTL != qpgOverrideTTLDefault {
		t.Fatalf("unexpected override_ttl: %d", cfg.QPG.OverrideTTL)
	}
}

func TestLoadDQPExternalHints(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	content := `oracles:
  dqp_external_hints:
    - "SET_VAR(tidb_opt_partial_ordered_index_for_topn='COST')"
    - "HASH_JOIN"
`
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}

	cfg, err := Load(tmp.Name())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Oracles.DQPExternalHints) != 2 {
		t.Fatalf("unexpected dqp external hints count: %d", len(cfg.Oracles.DQPExternalHints))
	}
	if cfg.Oracles.DQPExternalHints[0] != "SET_VAR(tidb_opt_partial_ordered_index_for_topn='COST')" {
		t.Fatalf("unexpected first dqp external hint: %s", cfg.Oracles.DQPExternalHints[0])
	}
	if cfg.Oracles.DQPExternalHints[1] != "HASH_JOIN" {
		t.Fatalf("unexpected second dqp external hint: %s", cfg.Oracles.DQPExternalHints[1])
	}
}
