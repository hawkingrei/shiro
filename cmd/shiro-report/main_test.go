package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"shiro/internal/report"
)

func TestSummaryErrorReason(t *testing.T) {
	tests := []struct {
		name    string
		summary report.Summary
		want    string
	}{
		{
			name: "missing details",
			summary: report.Summary{
				Details: nil,
			},
			want: "",
		},
		{
			name: "non-string reason",
			summary: report.Summary{
				Details: map[string]any{"error_reason": 1},
			},
			want: "",
		},
		{
			name: "string reason",
			summary: report.Summary{
				Details: map[string]any{"error_reason": "unknown_column"},
			},
			want: "unknown_column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := summaryErrorReason(tt.summary)
			if got != tt.want {
				t.Fatalf("summaryErrorReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestObjectKey(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		nameIn string
		want   string
	}{
		{
			name:   "no prefix",
			prefix: "",
			nameIn: "reports.json",
			want:   "reports.json",
		},
		{
			name:   "trim prefix and name",
			prefix: "/a/b/",
			nameIn: "/reports.json",
			want:   "a/b/reports.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := objectKey(tt.prefix, tt.nameIn)
			if got != tt.want {
				t.Fatalf("objectKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDeriveObjectURLs(t *testing.T) {
	reportURL, archiveURL := deriveObjectURLs("s3://bucket/abc/", "case.tar.zst", "")
	if reportURL != "" {
		t.Fatalf("unexpected report url without public base: %q", reportURL)
	}
	if archiveURL != "" {
		t.Fatalf("unexpected archive url without public base: %q", archiveURL)
	}

	reportURL, archiveURL = deriveObjectURLs("s3://bucket/abc/", "case.tar.zst", "https://cdn.example.com")
	if reportURL != "https://cdn.example.com/abc/report.json" {
		t.Fatalf("unexpected report url with public base: %q", reportURL)
	}
	if archiveURL != "https://cdn.example.com/abc/case.tar.zst" {
		t.Fatalf("unexpected archive url with public base: %q", archiveURL)
	}

	reportURL, archiveURL = deriveObjectURLs("gs://bucket/abc/", "case.tar.zst", "https://cdn.example.com")
	if reportURL != "https://cdn.example.com/abc/report.json" {
		t.Fatalf("unexpected report url with gcs public base: %q", reportURL)
	}
	if archiveURL != "https://cdn.example.com/abc/case.tar.zst" {
		t.Fatalf("unexpected archive url with gcs public base: %q", archiveURL)
	}

	reportURL, archiveURL = deriveObjectURLs("GS://bucket/abc/", "case.tar.zst", "https://cdn.example.com")
	if reportURL != "https://cdn.example.com/abc/report.json" {
		t.Fatalf("unexpected report url with gcs uppercase scheme: %q", reportURL)
	}
	if archiveURL != "https://cdn.example.com/abc/case.tar.zst" {
		t.Fatalf("unexpected archive url with gcs uppercase scheme: %q", archiveURL)
	}

	reportURL, archiveURL = deriveObjectURLs("https://cdn.example.com/abc/", "case.tar.zst", "")
	if reportURL != "https://cdn.example.com/abc/report.json" {
		t.Fatalf("unexpected report url from https upload location: %q", reportURL)
	}
	if archiveURL != "https://cdn.example.com/abc/case.tar.zst" {
		t.Fatalf("unexpected archive url from https upload location: %q", archiveURL)
	}
}

func TestCaseIDFromSummary(t *testing.T) {
	s := report.Summary{CaseID: "id-from-case-id", CaseDir: "id-from-case-dir"}
	if got := caseIDFromSummary(s, "fallback"); got != "id-from-case-id" {
		t.Fatalf("unexpected case id: %q", got)
	}
	s = report.Summary{CaseDir: "id-from-case-dir"}
	if got := caseIDFromSummary(s, "fallback"); got != "id-from-case-dir" {
		t.Fatalf("unexpected case id from case_dir: %q", got)
	}
	s = report.Summary{}
	if got := caseIDFromSummary(s, "fallback"); got != "fallback" {
		t.Fatalf("unexpected fallback case id: %q", got)
	}
}

func TestCaseSummaryRelPath(t *testing.T) {
	if got := caseSummaryRelPath("019c5744-b015-7ac5-8cf4-97b2ee3b0fed"); got != "./cases/019c5744-b015-7ac5-8cf4-97b2ee3b0fed/summary.json" {
		t.Fatalf("unexpected summary rel path: %q", got)
	}
	if got := caseSummaryRelPath("dir/with/slash"); got != "./cases/dir_with_slash/summary.json" {
		t.Fatalf("unexpected sanitized summary rel path: %q", got)
	}
	if got := caseSummaryRelPath(".."); got != "" {
		t.Fatalf("unexpected summary rel path for traversal component: %q", got)
	}
	if got := caseSummaryRelPath("abc..def"); got != "" {
		t.Fatalf("unexpected summary rel path for dotted component: %q", got)
	}
}

func TestBuildSearchBlobSkipsNilDetails(t *testing.T) {
	entry := CaseEntry{
		Oracle:                       "norec",
		ErrorReason:                  "result_mismatch",
		Expected:                     "cnt=1",
		Actual:                       "cnt=2",
		PlanSignature:                "abc",
		MinimizeReason:               "delta_debug",
		ReplayKind:                   "base",
		ReplayOutcome:                "failed",
		ReplayFailureStage:           "schema_load",
		ReplayExpectedErrorReason:    "eet:missing_column",
		ReplayExpectedErrorSignature: "eet:missing_column|cant_find_column_in_schema",
		ReplayActualErrorReason:      "replay:no_error",
		ReplayActualErrorSignature:   "replay:no_error|no_error",
		CaptureFreshness:             "infra_unhealthy",
		CaptureStalledReason:         "oracle_timeout",
		Details:                      nil,
	}
	blob := buildSearchBlob(entry)
	if strings.Contains(blob, "{}") {
		t.Fatalf("search blob should not include empty details object: %q", blob)
	}
	for _, want := range []string{
		"delta_debug",
		"base",
		"failed",
		"schema_load",
		"eet:missing_column",
		"replay:no_error",
		"infra_unhealthy",
		"oracle_timeout",
	} {
		if !strings.Contains(blob, want) {
			t.Fatalf("search blob missing observability field %q: %q", want, blob)
		}
	}
}

func TestWriteJSONOutputsIndexAndCaseSummaries(t *testing.T) {
	output := t.TempDir()
	site := SiteData{
		GeneratedAt: "2026-02-13T16:00:00Z",
		Source:      "reports",
		Cases: []CaseEntry{
			{
				ID:                           "legacy-case-id",
				Oracle:                       "norec",
				Timestamp:                    "2026-02-13T15:59:00Z",
				Expected:                     "cnt=1",
				Actual:                       "cnt=2",
				ErrorReason:                  "result_mismatch",
				MinimizeReason:               "delta_debug",
				ReplayKind:                   "base",
				ReplayOutcome:                "failed",
				ReplayFailureStage:           "schema_load",
				ReplayExpectedErrorReason:    "eet:missing_column",
				ReplayExpectedErrorSignature: "eet:missing_column|cant_find_column_in_schema",
				ReplayActualErrorReason:      "replay:no_error",
				ReplayActualErrorSignature:   "replay:no_error|no_error",
				CaptureFreshness:             "infra_unhealthy",
				CaptureStalledReason:         "oracle_timeout",
				ReportURL:                    "https://cdn.example.com/cases/legacy-case-id/report.json",
				UploadLocation:               "gs://bucket/legacy-case-id/",
			},
			{
				ID:               "fallback-id",
				CaseID:           "019c5744-b015-7ac5-8cf4-97b2ee3b0fed",
				CaseDir:          "019c5744-b015-7ac5-8cf4-97b2ee3b0fed",
				Oracle:           "tlp",
				Timestamp:        "2026-02-13T15:58:00Z",
				Expected:         "ok",
				Actual:           "ok",
				CaptureFreshness: "fresh",
				SQL:              []string{"SELECT 1"},
				Details:          map[string]any{"error_reason": "none"},
				UploadLocation:   "gs://bucket/019c5744-b015-7ac5-8cf4-97b2ee3b0fed/",
			},
		},
	}

	if err := writeJSON(output, site); err != nil {
		t.Fatalf("writeJSON() failed: %v", err)
	}

	for _, file := range []string{"report.json", "reports.json", "reports.index.json"} {
		if _, err := os.Stat(filepath.Join(output, file)); err != nil {
			t.Fatalf("missing output file %s: %v", file, err)
		}
	}

	indexData, err := os.ReadFile(filepath.Join(output, "reports.index.json"))
	if err != nil {
		t.Fatalf("read index failed: %v", err)
	}
	var index SiteIndexData
	if err := json.Unmarshal(indexData, &index); err != nil {
		t.Fatalf("unmarshal index failed: %v", err)
	}
	if index.IndexVersion != reportIndexVersion {
		t.Fatalf("unexpected index version: %d", index.IndexVersion)
	}
	if index.CaseCount != 2 || len(index.Cases) != 2 {
		t.Fatalf("unexpected case count in index: case_count=%d len=%d", index.CaseCount, len(index.Cases))
	}
	if index.Cases[0].SummaryURL == "" || index.Cases[1].SummaryURL == "" {
		t.Fatalf("summary urls should not be empty: %+v", index.Cases)
	}
	if index.Cases[0].SummaryURL != "./cases/legacy-case-id/summary.json" {
		t.Fatalf("unexpected summary url for local case summary: %q", index.Cases[0].SummaryURL)
	}
	if index.Cases[1].SummaryURL != "./cases/019c5744-b015-7ac5-8cf4-97b2ee3b0fed/summary.json" {
		t.Fatalf("unexpected local summary url: %q", index.Cases[1].SummaryURL)
	}
	if index.Cases[0].DetailLoaded {
		t.Fatalf("expected detail_loaded=false when summary url is available")
	}
	if index.Cases[1].DetailLoaded {
		t.Fatalf("expected detail_loaded=false when summary url is available")
	}
	if index.Cases[0].MinimizeReason != "delta_debug" ||
		index.Cases[0].ReplayKind != "base" ||
		index.Cases[0].ReplayOutcome != "failed" ||
		index.Cases[0].ReplayFailureStage != "schema_load" ||
		index.Cases[0].ReplayExpectedErrorReason != "eet:missing_column" ||
		index.Cases[0].ReplayExpectedErrorSignature != "eet:missing_column|cant_find_column_in_schema" ||
		index.Cases[0].ReplayActualErrorReason != "replay:no_error" ||
		index.Cases[0].ReplayActualErrorSignature != "replay:no_error|no_error" ||
		index.Cases[0].CaptureFreshness != "infra_unhealthy" ||
		index.Cases[0].CaptureStalledReason != "oracle_timeout" {
		t.Fatalf("index entry missing observability fields: %+v", index.Cases[0])
	}
	if !strings.Contains(index.Cases[0].SearchBlob, "delta_debug") ||
		!strings.Contains(index.Cases[0].SearchBlob, "infra_unhealthy") ||
		!strings.Contains(index.Cases[0].SearchBlob, "replay:no_error|no_error") {
		t.Fatalf("search blob missing observability fields: %q", index.Cases[0].SearchBlob)
	}

	summaryPath := filepath.Join(output, "cases", "019c5744-b015-7ac5-8cf4-97b2ee3b0fed", "summary.json")
	if _, err := os.Stat(summaryPath); err != nil {
		t.Fatalf("missing per-case summary file: %v", err)
	}
	legacySummaryPath := filepath.Join(output, "cases", "legacy-case-id", "summary.json")
	if _, err := os.Stat(legacySummaryPath); err != nil {
		t.Fatalf("missing per-case summary file for legacy id: %v", err)
	}
	legacySummaryData, err := os.ReadFile(legacySummaryPath)
	if err != nil {
		t.Fatalf("read legacy summary failed: %v", err)
	}
	var legacySummary CaseEntry
	if err := json.Unmarshal(legacySummaryData, &legacySummary); err != nil {
		t.Fatalf("unmarshal legacy summary failed: %v", err)
	}
	if legacySummary.MinimizeReason != "delta_debug" ||
		legacySummary.ReplayKind != "base" ||
		legacySummary.ReplayOutcome != "failed" ||
		legacySummary.ReplayFailureStage != "schema_load" ||
		legacySummary.ReplayExpectedErrorReason != "eet:missing_column" ||
		legacySummary.ReplayExpectedErrorSignature != "eet:missing_column|cant_find_column_in_schema" ||
		legacySummary.ReplayActualErrorReason != "replay:no_error" ||
		legacySummary.ReplayActualErrorSignature != "replay:no_error|no_error" ||
		legacySummary.CaptureFreshness != "infra_unhealthy" ||
		legacySummary.CaptureStalledReason != "oracle_timeout" {
		t.Fatalf("per-case summary missing observability fields: %+v", legacySummary)
	}
}

func TestHydrateSummaryObservabilityFieldsFallsBackToDetails(t *testing.T) {
	summary := report.Summary{
		Details: map[string]any{
			"minimize_reason":                               "base_replay_not_reproducible",
			"minimize_base_replay_kind":                     "case_error",
			"minimize_base_replay_outcome":                  "error_mismatch",
			"minimize_base_replay_failure_stage":            "exec-case-sql",
			"minimize_base_replay_expected_error_reason":    "EET:Missing_Column",
			"minimize_base_replay_expected_error_signature": " EET:Missing_Column | cant_find_column_in_schema ",
			"minimize_base_replay_actual_error_reason":      " replay:no_error ",
			"minimize_base_replay_actual_error_signature":   " replay:no_error | no error ",
		},
	}

	hydrateSummaryObservabilityFields(&summary)

	if summary.MinimizeReason != "base_replay_not_reproducible" {
		t.Fatalf("MinimizeReason=%q want=base_replay_not_reproducible", summary.MinimizeReason)
	}
	if summary.ReplayKind != "case_error" || summary.ReplayOutcome != "error_mismatch" {
		t.Fatalf("unexpected replay identity fields: %+v", summary)
	}
	if summary.ReplayFailureStage != "exec_case_sql" {
		t.Fatalf("ReplayFailureStage=%q want=exec_case_sql", summary.ReplayFailureStage)
	}
	if summary.ReplayExpectedErrorReason != "eet:missing_column" {
		t.Fatalf("ReplayExpectedErrorReason=%q want=eet:missing_column", summary.ReplayExpectedErrorReason)
	}
	if summary.ReplayExpectedErrorSignature != "eet:missing_column|cant_find_column_in_schema" {
		t.Fatalf("ReplayExpectedErrorSignature=%q want normalized signature", summary.ReplayExpectedErrorSignature)
	}
	if summary.ReplayActualErrorReason != "replay:no_error" {
		t.Fatalf("ReplayActualErrorReason=%q want=replay:no_error", summary.ReplayActualErrorReason)
	}
	if summary.ReplayActualErrorSignature != "replay:no_error|no_error" {
		t.Fatalf("ReplayActualErrorSignature=%q want normalized signature", summary.ReplayActualErrorSignature)
	}
}

func TestBuildSiteIndexMarksMissingSummaryAsLoaded(t *testing.T) {
	site := SiteData{
		GeneratedAt: "2026-02-13T16:00:00Z",
		Source:      "reports",
		Cases: []CaseEntry{
			{
				ID:        "",
				CaseID:    "",
				Oracle:    "norec",
				Timestamp: "2026-02-13T15:59:00Z",
			},
			{
				ID:        "",
				CaseID:    "",
				Oracle:    "norec",
				Timestamp: "2026-02-13T15:59:01Z",
				ReportURL: "https://cdn.example.com/cases/legacy/report.json",
			},
			{
				ID:        "..",
				CaseID:    "",
				Oracle:    "norec",
				Timestamp: "2026-02-13T15:59:02Z",
				ReportURL: "https://cdn.example.com/cases/danger/report.json",
			},
		},
	}

	index := buildSiteIndex(site)
	if len(index.Cases) != 3 {
		t.Fatalf("unexpected index case count: %d", len(index.Cases))
	}
	if index.Cases[0].SummaryURL != "" {
		t.Fatalf("expected empty summary url for missing case id: %q", index.Cases[0].SummaryURL)
	}
	if !index.Cases[0].DetailLoaded {
		t.Fatalf("expected detail_loaded=true when summary url is unavailable")
	}
	if index.Cases[1].SummaryURL != "https://cdn.example.com/cases/legacy/report.json" {
		t.Fatalf("unexpected summary url from report_url: %q", index.Cases[1].SummaryURL)
	}
	if index.Cases[1].DetailLoaded {
		t.Fatalf("expected detail_loaded=false when report_url is available")
	}
	if index.Cases[2].SummaryURL != "" {
		t.Fatalf("expected empty summary url for sanitized case id: %q", index.Cases[2].SummaryURL)
	}
	if !index.Cases[2].DetailLoaded {
		t.Fatalf("expected detail_loaded=true when sanitized case id cannot resolve summary")
	}
}

func TestCollectPublishFilesIncludesIndexAndCaseSummaries(t *testing.T) {
	output := t.TempDir()
	paths := []string{
		filepath.Join(output, "report.json"),
		filepath.Join(output, "reports.json"),
		filepath.Join(output, "reports.index.json"),
		filepath.Join(output, "cases", "a", "summary.json"),
		filepath.Join(output, "cases", "b", "summary.json"),
		filepath.Join(output, "cases", "b", "notes.txt"),
	}
	for _, path := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
			t.Fatalf("write file failed: %v", err)
		}
	}

	files, err := collectPublishFiles(output)
	if err != nil {
		t.Fatalf("collectPublishFiles() failed: %v", err)
	}
	slices.Sort(files)
	want := []string{
		"cases/a/summary.json",
		"cases/b/summary.json",
		"report.json",
		"reports.index.json",
		"reports.json",
	}
	slices.Sort(want)
	if !slices.Equal(files, want) {
		t.Fatalf("unexpected publish files: got=%v want=%v", files, want)
	}
}
