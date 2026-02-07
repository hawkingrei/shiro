package main

import (
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
	reportURL, archiveURL := deriveObjectURLs("s3://bucket/abc/", "case.tar.zst")
	if reportURL != "s3://bucket/abc/report.json" {
		t.Fatalf("unexpected report url: %q", reportURL)
	}
	if archiveURL != "s3://bucket/abc/case.tar.zst" {
		t.Fatalf("unexpected archive url: %q", archiveURL)
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
