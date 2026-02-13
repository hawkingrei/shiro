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
}

func TestBuildSearchBlobSkipsNilDetails(t *testing.T) {
	entry := CaseEntry{
		Oracle:        "norec",
		ErrorReason:   "result_mismatch",
		Expected:      "cnt=1",
		Actual:        "cnt=2",
		PlanSignature: "abc",
		Details:       nil,
	}
	blob := buildSearchBlob(entry)
	if strings.Contains(blob, "{}") {
		t.Fatalf("search blob should not include empty details object: %q", blob)
	}
}

func TestWriteJSONOutputsIndexAndCaseSummaries(t *testing.T) {
	output := t.TempDir()
	site := SiteData{
		GeneratedAt: "2026-02-13T16:00:00Z",
		Source:      "reports",
		Cases: []CaseEntry{
			{
				ID:             "legacy-case-id",
				Oracle:         "norec",
				Timestamp:      "2026-02-13T15:59:00Z",
				Expected:       "cnt=1",
				Actual:         "cnt=2",
				ErrorReason:    "result_mismatch",
				ReportURL:      "https://cdn.example.com/cases/legacy-case-id/report.json",
				UploadLocation: "gs://bucket/legacy-case-id/",
			},
			{
				ID:             "fallback-id",
				CaseID:         "019c5744-b015-7ac5-8cf4-97b2ee3b0fed",
				CaseDir:        "019c5744-b015-7ac5-8cf4-97b2ee3b0fed",
				Oracle:         "tlp",
				Timestamp:      "2026-02-13T15:58:00Z",
				Expected:       "ok",
				Actual:         "ok",
				SQL:            []string{"SELECT 1"},
				Details:        map[string]any{"error_reason": "none"},
				UploadLocation: "gs://bucket/019c5744-b015-7ac5-8cf4-97b2ee3b0fed/",
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
	if index.Cases[0].SummaryURL != "https://cdn.example.com/cases/legacy-case-id/report.json" {
		t.Fatalf("unexpected summary url for case with public report url: %q", index.Cases[0].SummaryURL)
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

	summaryPath := filepath.Join(output, "cases", "019c5744-b015-7ac5-8cf4-97b2ee3b0fed", "summary.json")
	if _, err := os.Stat(summaryPath); err != nil {
		t.Fatalf("missing per-case summary file: %v", err)
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
		},
	}

	index := buildSiteIndex(site)
	if len(index.Cases) != 2 {
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
