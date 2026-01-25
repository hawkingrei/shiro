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
