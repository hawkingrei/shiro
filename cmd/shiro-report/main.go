package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"shiro/internal/config"
	"shiro/internal/report"
	"shiro/internal/util"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// FileContent holds inlined report file content.
type FileContent struct {
	Name      string `json:"name"`
	Content   string `json:"content"`
	Truncated bool   `json:"truncated"`
}

// CaseEntry represents a report case entry.
type CaseEntry struct {
	ID                           string                 `json:"id"`
	Dir                          string                 `json:"dir"`
	Oracle                       string                 `json:"oracle"`
	Timestamp                    string                 `json:"timestamp"`
	TiDBVersion                  string                 `json:"tidb_version"`
	TiDBCommit                   string                 `json:"tidb_commit"`
	ErrorReason                  string                 `json:"error_reason"`
	PlanSignature                string                 `json:"plan_signature"`
	PlanSigFormat                string                 `json:"plan_signature_format"`
	Expected                     string                 `json:"expected"`
	Actual                       string                 `json:"actual"`
	Error                        string                 `json:"error"`
	GroundTruthDSGMismatchReason string                 `json:"groundtruth_dsg_mismatch_reason"`
	Flaky                        bool                   `json:"flaky"`
	NoRECOptimizedSQL            string                 `json:"norec_optimized_sql"`
	NoRECUnoptimizedSQL          string                 `json:"norec_unoptimized_sql"`
	NoRECPredicate               string                 `json:"norec_predicate"`
	CaseID                       string                 `json:"case_id"`
	CaseDir                      string                 `json:"case_dir"`
	ArchiveName                  string                 `json:"archive_name"`
	ArchiveCodec                 string                 `json:"archive_codec"`
	ArchiveURL                   string                 `json:"archive_url"`
	ReportURL                    string                 `json:"report_url"`
	SQL                          []string               `json:"sql"`
	PlanReplay                   string                 `json:"plan_replayer"`
	UploadLocation               string                 `json:"upload_location"`
	Details                      map[string]any         `json:"details"`
	Files                        map[string]FileContent `json:"files"`
}

// SiteData is the JSON payload for the static site.
type SiteData struct {
	GeneratedAt string      `json:"generated_at"`
	Source      string      `json:"source"`
	Cases       []CaseEntry `json:"cases"`
}

// SiteIndexData is a lightweight payload for on-demand case loading.
type SiteIndexData struct {
	GeneratedAt  string           `json:"generated_at"`
	Source       string           `json:"source"`
	IndexVersion int              `json:"index_version"`
	CaseCount    int              `json:"case_count"`
	Cases        []CaseIndexEntry `json:"cases"`
}

// CaseIndexEntry contains summary metadata and a detail URL for a case.
type CaseIndexEntry struct {
	ID                           string `json:"id"`
	Dir                          string `json:"dir"`
	Oracle                       string `json:"oracle"`
	Timestamp                    string `json:"timestamp"`
	TiDBVersion                  string `json:"tidb_version"`
	TiDBCommit                   string `json:"tidb_commit"`
	ErrorReason                  string `json:"error_reason"`
	PlanSignature                string `json:"plan_signature"`
	PlanSigFormat                string `json:"plan_signature_format"`
	Expected                     string `json:"expected"`
	Actual                       string `json:"actual"`
	Error                        string `json:"error"`
	GroundTruthDSGMismatchReason string `json:"groundtruth_dsg_mismatch_reason"`
	Flaky                        bool   `json:"flaky"`
	NoRECPredicate               string `json:"norec_predicate"`
	CaseID                       string `json:"case_id"`
	CaseDir                      string `json:"case_dir"`
	ArchiveName                  string `json:"archive_name"`
	ArchiveCodec                 string `json:"archive_codec"`
	ArchiveURL                   string `json:"archive_url"`
	ReportURL                    string `json:"report_url"`
	UploadLocation               string `json:"upload_location"`
	SummaryURL                   string `json:"summary_url"`
	SearchBlob                   string `json:"search_blob"`
	DetailLoaded                 bool   `json:"detail_loaded"`
}

type loadOptions struct {
	MaxBytes              int
	MaxZipBytes           int
	ArtifactPublicBaseURL string
}

type publishOptions struct {
	S3            config.S3Config
	GCS           config.GCSConfig
	PublicBaseURL string
}

type workerSyncOptions struct {
	Endpoint string
	Token    string
}

type workerSyncPayload struct {
	ManifestURL string           `json:"manifest_url"`
	GeneratedAt string           `json:"generated_at"`
	Source      string           `json:"source"`
	Cases       []workerSyncCase `json:"cases"`
}

type workerSyncCase struct {
	CaseID         string `json:"case_id"`
	Oracle         string `json:"oracle"`
	Timestamp      string `json:"timestamp"`
	ErrorReason    string `json:"error_reason"`
	Error          string `json:"error"`
	UploadLocation string `json:"upload_location"`
	ReportURL      string `json:"report_url"`
	ArchiveURL     string `json:"archive_url"`
}

const reportIndexVersion = 1

func main() {
	input := flag.String("input", ".report", "input directory, gs://bucket/prefix, or legacy s3://bucket/prefix")
	output := flag.String("output", "web/public", "output directory for report.json/reports.json")
	configPath := flag.String("config", "config.yaml", "path to config file (for GCS/S3 access)")
	maxBytes := flag.Int("max-bytes", 64*1024, "max bytes to read per case file")
	maxZipBytes := flag.Int("max-zip-bytes", 20*1024*1024, "max bytes to read for plan_replayer.zip")
	publishEndpoint := flag.String("publish-endpoint", "", "S3-compatible endpoint for publishing report.json/reports.json (for example Cloudflare R2)")
	publishRegion := flag.String("publish-region", "auto", "region for publish endpoint")
	publishBucket := flag.String("publish-bucket", "", "target bucket for publishing report manifests")
	publishPrefix := flag.String("publish-prefix", "", "target prefix for publishing report manifests")
	publishAccessKey := flag.String("publish-access-key-id", "", "access key for publishing report manifests")
	publishSecret := flag.String("publish-secret-access-key", "", "secret key for publishing report manifests")
	publishSessionToken := flag.String("publish-session-token", "", "session token for publishing report manifests")
	publishUsePathStyle := flag.Bool("publish-use-path-style", true, "whether to use path-style S3 addressing for publish endpoint")
	publishPublicBaseURL := flag.String("publish-public-base-url", "", "public base URL for published manifests (S3/GCS)")
	publishGCSBucket := flag.String("publish-gcs-bucket", "", "target GCS bucket for publishing report manifests")
	publishGCSPrefix := flag.String("publish-gcs-prefix", "", "target prefix for publishing report manifests")
	publishGCSCredentialsFile := flag.String("publish-gcs-credentials-file", "", "service account JSON for GCS publish (optional, uses ADC when empty)")
	artifactPublicBaseURL := flag.String("artifact-public-base-url", "", "public HTTP(S) base URL used to derive per-case report/archive links from gs:// or s3:// upload locations")
	workerSyncEndpoint := flag.String("worker-sync-endpoint", "", "cloudflare worker sync endpoint for D1 metadata upsert")
	workerSyncToken := flag.String("worker-sync-token", "", "bearer token used for worker sync endpoint")
	flag.Parse()

	opts := loadOptions{
		MaxBytes:              *maxBytes,
		MaxZipBytes:           *maxZipBytes,
		ArtifactPublicBaseURL: strings.TrimSpace(*artifactPublicBaseURL),
	}
	ctx := context.Background()

	var cases []CaseEntry
	var err error
	if strings.HasPrefix(*input, "gs://") {
		cfg, loadErr := config.Load(*configPath)
		if loadErr != nil {
			fail("load config: %v", loadErr)
		}
		bucket, prefix, parseErr := parseGCSURI(*input)
		if parseErr != nil {
			fail("parse gcs input: %v", parseErr)
		}
		if !cfg.Storage.GCS.Enabled {
			fail("gcs input requested but storage.gcs.enabled is false")
		}
		cases, err = loadGCSCases(ctx, cfg.Storage.GCS, bucket, prefix, opts)
	} else if strings.HasPrefix(*input, "s3://") {
		cfg, loadErr := config.Load(*configPath)
		if loadErr != nil {
			fail("load config: %v", loadErr)
		}
		bucket, prefix, parseErr := parseS3URI(*input)
		if parseErr != nil {
			fail("parse s3 input: %v", parseErr)
		}
		if !cfg.Storage.S3.Enabled {
			fail("s3 input requested but storage.s3.enabled is false")
		}
		cases, err = loadS3Cases(ctx, cfg.Storage.S3, bucket, prefix, opts)
	} else {
		cases, err = loadLocalCases(*input, opts)
	}
	if err != nil {
		fail("load cases: %v", err)
	}

	sort.Slice(cases, func(i, j int) bool {
		return cases[i].Timestamp > cases[j].Timestamp
	})

	site := SiteData{
		GeneratedAt: time.Now().Format(time.RFC3339),
		Source:      *input,
		Cases:       cases,
	}
	if err := writeJSON(*output, site); err != nil {
		fail("write json: %v", err)
	}

	publishCfg := publishOptions{
		S3: config.S3Config{
			Enabled:         strings.TrimSpace(*publishBucket) != "",
			Endpoint:        strings.TrimSpace(*publishEndpoint),
			Region:          strings.TrimSpace(*publishRegion),
			Bucket:          strings.TrimSpace(*publishBucket),
			Prefix:          strings.TrimSpace(*publishPrefix),
			AccessKeyID:     strings.TrimSpace(*publishAccessKey),
			SecretAccessKey: strings.TrimSpace(*publishSecret),
			SessionToken:    strings.TrimSpace(*publishSessionToken),
			UsePathStyle:    *publishUsePathStyle,
		},
		GCS: config.GCSConfig{
			Enabled:         strings.TrimSpace(*publishGCSBucket) != "",
			Bucket:          strings.TrimSpace(*publishGCSBucket),
			Prefix:          strings.TrimSpace(*publishGCSPrefix),
			CredentialsFile: strings.TrimSpace(*publishGCSCredentialsFile),
		},
		PublicBaseURL: strings.TrimSpace(*publishPublicBaseURL),
	}
	manifestURL, err := publishReports(ctx, publishCfg, *output)
	if err != nil {
		fail("publish reports: %v", err)
	}
	if manifestURL != "" {
		fmt.Printf("published report manifests to %s\n", manifestURL)
	}

	syncCfg := workerSyncOptions{
		Endpoint: strings.TrimSpace(*workerSyncEndpoint),
		Token:    strings.TrimSpace(*workerSyncToken),
	}
	if err := syncWorkerMetadata(ctx, syncCfg, manifestURL, site); err != nil {
		fail("sync worker metadata: %v", err)
	}
	if syncCfg.Endpoint != "" {
		fmt.Printf("synced %d cases to %s\n", len(site.Cases), syncCfg.Endpoint)
	}

	fmt.Printf(
		"report json written to %s, %s, and %s\n",
		filepath.Join(*output, "report.json"),
		filepath.Join(*output, "reports.json"),
		filepath.Join(*output, "reports.index.json"),
	)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func loadLocalCases(root string, opts loadOptions) ([]CaseEntry, error) {
	dirs, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	cases := make([]CaseEntry, 0, len(dirs))
	for _, dirEntry := range dirs {
		if !dirEntry.IsDir() {
			continue
		}
		dir := filepath.Join(root, dirEntry.Name())
		if _, err := os.Stat(filepath.Join(dir, "summary.json")); err != nil {
			continue
		}
		entry, err := readCaseFromDir(dir, opts)
		if err != nil {
			continue
		}
		entry.Dir = dir
		if strings.TrimSpace(entry.ID) == "" {
			entry.ID = dirEntry.Name()
		}
		cases = append(cases, entry)
	}
	return cases, nil
}

func readCaseFromDir(dir string, opts loadOptions) (CaseEntry, error) {
	summaryPath := filepath.Join(dir, "summary.json")
	data, err := os.ReadFile(summaryPath)
	if err != nil {
		return CaseEntry{}, err
	}
	var summary report.Summary
	if err := json.Unmarshal(data, &summary); err != nil {
		return CaseEntry{}, err
	}
	files := map[string]FileContent{}
	files["case.sql"] = mustReadFile(filepath.Join(dir, "case.sql"), opts.MaxBytes)
	files["schema.sql"] = mustReadFile(filepath.Join(dir, "schema.sql"), opts.MaxBytes)
	files["inserts.sql"] = mustReadFile(filepath.Join(dir, "inserts.sql"), opts.MaxBytes)
	files["data.tsv"] = mustReadFile(filepath.Join(dir, "data.tsv"), opts.MaxBytes)
	files["report.json"] = mustReadFile(filepath.Join(dir, "report.json"), opts.MaxBytes)
	if _, err := os.Stat(filepath.Join(dir, "plan_replayer.zip")); err == nil {
		files["plan_replayer.zip"] = FileContent{Name: "plan_replayer.zip", Content: "(binary)", Truncated: true}
	}
	if _, err := os.Stat(filepath.Join(dir, report.CaseArchiveName)); err == nil {
		files[report.CaseArchiveName] = FileContent{Name: report.CaseArchiveName, Content: "(binary)", Truncated: true}
	}
	commit := extractCommit(summary.TiDBVersion)
	if commit == "" {
		commit = extractCommitFromPlanReplayer(filepath.Join(dir, "plan_replayer.zip"), opts.MaxZipBytes)
	}
	caseID := caseIDFromSummary(summary, filepath.Base(dir))
	caseDir := caseDirFromSummary(summary, caseID)
	reportURL, archiveURL := deriveObjectURLs(summary.UploadLocation, summary.ArchiveName, opts.ArtifactPublicBaseURL)
	return CaseEntry{
		ID:                           caseID,
		Oracle:                       summary.Oracle,
		Timestamp:                    summary.Timestamp,
		TiDBVersion:                  summary.TiDBVersion,
		TiDBCommit:                   commit,
		ErrorReason:                  summaryErrorReason(summary),
		PlanSignature:                summary.PlanSignature,
		PlanSigFormat:                summary.PlanSigFormat,
		Expected:                     summary.Expected,
		Actual:                       summary.Actual,
		Error:                        summary.Error,
		GroundTruthDSGMismatchReason: summary.GroundTruthDSGMismatchReason,
		Flaky:                        summary.Flaky,
		NoRECOptimizedSQL:            summary.NoRECOptimizedSQL,
		NoRECUnoptimizedSQL:          summary.NoRECUnoptimizedSQL,
		NoRECPredicate:               summary.NoRECPredicate,
		CaseID:                       caseID,
		CaseDir:                      caseDir,
		ArchiveName:                  summary.ArchiveName,
		ArchiveCodec:                 summary.ArchiveCodec,
		ArchiveURL:                   archiveURL,
		ReportURL:                    reportURL,
		SQL:                          summary.SQL,
		PlanReplay:                   summary.PlanReplay,
		UploadLocation:               summary.UploadLocation,
		Details:                      summary.Details,
		Files:                        files,
	}, nil
}

func mustReadFile(path string, maxBytes int) FileContent {
	content, truncated, err := readFileLimited(path, maxBytes)
	if err != nil {
		return FileContent{Name: filepath.Base(path)}
	}
	return FileContent{Name: filepath.Base(path), Content: content, Truncated: truncated}
}

func readFileLimited(path string, maxBytes int) (string, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", false, err
	}
	defer util.CloseWithErr(f, "report input")
	limit := int64(maxBytes) + 1
	data, err := io.ReadAll(io.LimitReader(f, limit))
	if err != nil {
		return "", false, err
	}
	truncated := len(data) > maxBytes
	if truncated {
		data = data[:maxBytes]
	}
	return string(data), truncated, nil
}

func writeJSON(output string, site SiteData) error {
	if err := os.MkdirAll(output, 0o755); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(output, "report.json"), site); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(output, "reports.json"), site); err != nil {
		return err
	}
	if err := writeCaseSummaryFiles(output, site.Cases); err != nil {
		return err
	}
	index := buildSiteIndex(site)
	return writeJSONFile(filepath.Join(output, "reports.index.json"), index)
}

func writeJSONFile(path string, payload any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer util.CloseWithErr(f, "report output")
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(payload)
}

func writeCaseSummaryFiles(output string, cases []CaseEntry) error {
	for _, c := range cases {
		caseID := strings.TrimSpace(c.CaseID)
		if caseID == "" {
			caseID = strings.TrimSpace(c.ID)
		}
		relPath := caseSummaryRelPath(caseID)
		if relPath == "" {
			continue
		}
		target := filepath.Join(output, strings.TrimPrefix(relPath, "./"))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := writeJSONFile(target, c); err != nil {
			return err
		}
	}
	return nil
}

func buildSiteIndex(site SiteData) SiteIndexData {
	entries := make([]CaseIndexEntry, 0, len(site.Cases))
	for _, c := range site.Cases {
		caseID := strings.TrimSpace(c.CaseID)
		if caseID == "" {
			caseID = strings.TrimSpace(c.ID)
		}
		var summaryURL string
		if caseID != "" {
			summaryURL = caseSummaryRelPath(caseID)
		} else {
			candidate := strings.TrimSpace(c.ReportURL)
			if isHTTPURL(candidate) {
				summaryURL = candidate
			}
		}
		detailLoaded := strings.TrimSpace(summaryURL) == ""
		entries = append(entries, CaseIndexEntry{
			ID:                           c.ID,
			Dir:                          c.Dir,
			Oracle:                       c.Oracle,
			Timestamp:                    c.Timestamp,
			TiDBVersion:                  c.TiDBVersion,
			TiDBCommit:                   c.TiDBCommit,
			ErrorReason:                  c.ErrorReason,
			PlanSignature:                c.PlanSignature,
			PlanSigFormat:                c.PlanSigFormat,
			Expected:                     c.Expected,
			Actual:                       c.Actual,
			Error:                        c.Error,
			GroundTruthDSGMismatchReason: c.GroundTruthDSGMismatchReason,
			Flaky:                        c.Flaky,
			NoRECPredicate:               c.NoRECPredicate,
			CaseID:                       c.CaseID,
			CaseDir:                      c.CaseDir,
			ArchiveName:                  c.ArchiveName,
			ArchiveCodec:                 c.ArchiveCodec,
			ArchiveURL:                   c.ArchiveURL,
			ReportURL:                    c.ReportURL,
			UploadLocation:               c.UploadLocation,
			SummaryURL:                   summaryURL,
			SearchBlob:                   buildSearchBlob(c),
			DetailLoaded:                 detailLoaded,
		})
	}
	return SiteIndexData{
		GeneratedAt:  site.GeneratedAt,
		Source:       site.Source,
		IndexVersion: reportIndexVersion,
		CaseCount:    len(entries),
		Cases:        entries,
	}
}

func caseSummaryRelPath(caseID string) string {
	component := casePathComponent(caseID)
	if component == "" {
		return ""
	}
	return "./cases/" + component + "/summary.json"
}

func casePathComponent(caseID string) string {
	component := strings.TrimSpace(caseID)
	if component == "" {
		return ""
	}
	component = strings.ReplaceAll(component, "/", "_")
	component = strings.ReplaceAll(component, "\\", "_")
	if component == "." || component == ".." || strings.Contains(component, "..") {
		return ""
	}
	return component
}

func buildSearchBlob(c CaseEntry) string {
	parts := []string{
		c.Oracle,
		c.ErrorReason,
		c.Error,
		c.Expected,
		c.Actual,
		c.GroundTruthDSGMismatchReason,
		c.NoRECOptimizedSQL,
		c.NoRECUnoptimizedSQL,
		c.NoRECPredicate,
		c.TiDBVersion,
		c.TiDBCommit,
		c.PlanSignature,
		c.PlanSigFormat,
		c.CaseID,
		c.CaseDir,
		c.UploadLocation,
	}
	if len(c.SQL) > 0 {
		parts = append(parts, strings.Join(c.SQL, " "))
	}
	if len(c.Details) > 0 {
		if raw, err := json.Marshal(c.Details); err == nil {
			parts = append(parts, string(raw))
		}
	}
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		clean = append(clean, trimmed)
	}
	return strings.ToLower(strings.Join(clean, " "))
}

func parseS3URI(input string) (bucket string, prefix string, err error) {
	trimmed := strings.TrimSpace(input)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "s3://") {
		return "", "", fmt.Errorf("missing s3 scheme")
	}
	trimmed = trimmed[len("s3://"):]
	if trimmed == "" {
		return "", "", fmt.Errorf("missing s3 bucket")
	}
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = parts[0]
	prefix = ""
	if len(parts) == 2 {
		prefix = strings.TrimPrefix(parts[1], "/")
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}
	return bucket, prefix, nil
}

func parseGCSURI(input string) (bucket string, prefix string, err error) {
	trimmed := strings.TrimSpace(input)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "gs://") {
		return "", "", fmt.Errorf("missing gcs scheme")
	}
	trimmed = trimmed[len("gs://"):]
	if trimmed == "" {
		return "", "", fmt.Errorf("missing gcs bucket")
	}
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = parts[0]
	prefix = ""
	if len(parts) == 2 {
		prefix = strings.TrimPrefix(parts[1], "/")
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}
	return bucket, prefix, nil
}

func loadS3Cases(ctx context.Context, cfg config.S3Config, bucket, prefix string, opts loadOptions) ([]CaseEntry, error) {
	client, err := s3ClientFromConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	keys, objectSet, err := listSummaryKeys(ctx, client, bucket, prefix)
	if err != nil {
		return nil, err
	}
	cases := make([]CaseEntry, 0, len(keys))
	for _, key := range keys {
		dir := strings.TrimSuffix(key, "/summary.json")
		entry, err := readCaseFromS3(ctx, client, bucket, dir, opts, objectSet)
		if err != nil {
			continue
		}
		entry.Dir = "s3://" + bucket + "/" + dir
		if strings.TrimSpace(entry.ID) == "" {
			entry.ID = filepath.Base(dir)
		}
		cases = append(cases, entry)
	}
	return cases, nil
}

func listSummaryKeys(ctx context.Context, client *s3.Client, bucket, prefix string) ([]string, map[string]struct{}, error) {
	var keys []string
	objectSet := make(map[string]struct{})
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			objectSet[key] = struct{}{}
			if strings.HasSuffix(key, "/summary.json") {
				keys = append(keys, key)
			}
		}
	}
	return keys, objectSet, nil
}

func readCaseFromS3(ctx context.Context, client *s3.Client, bucket, dir string, opts loadOptions, objectSet map[string]struct{}) (CaseEntry, error) {
	summaryKey := dir + "/summary.json"
	summaryData, _, err := readObjectLimited(ctx, client, bucket, summaryKey, opts.MaxBytes)
	if err != nil {
		return CaseEntry{}, err
	}
	var summary report.Summary
	if err := json.Unmarshal([]byte(summaryData), &summary); err != nil {
		return CaseEntry{}, err
	}
	files := map[string]FileContent{}
	files["case.sql"] = readObjectFile(ctx, client, bucket, dir+"/case.sql", opts.MaxBytes)
	files["schema.sql"] = readObjectFile(ctx, client, bucket, dir+"/schema.sql", opts.MaxBytes)
	files["inserts.sql"] = readObjectFile(ctx, client, bucket, dir+"/inserts.sql", opts.MaxBytes)
	files["data.tsv"] = readObjectFile(ctx, client, bucket, dir+"/data.tsv", opts.MaxBytes)
	files["report.json"] = readObjectFile(ctx, client, bucket, dir+"/report.json", opts.MaxBytes)
	if _, ok := objectSet[dir+"/plan_replayer.zip"]; ok {
		files["plan_replayer.zip"] = FileContent{Name: "plan_replayer.zip", Content: "(binary)", Truncated: true}
	}
	archiveKey := dir + "/" + report.CaseArchiveName
	if _, ok := objectSet[archiveKey]; ok {
		files[report.CaseArchiveName] = FileContent{Name: report.CaseArchiveName, Content: "(binary)", Truncated: true}
	}
	commit := extractCommit(summary.TiDBVersion)
	if commit == "" {
		commit = extractCommitFromPlanReplayerS3(ctx, client, bucket, dir+"/plan_replayer.zip", opts.MaxZipBytes)
	}
	caseID := caseIDFromSummary(summary, filepath.Base(dir))
	caseDir := caseDirFromSummary(summary, caseID)
	reportURL, archiveURL := deriveObjectURLs(summary.UploadLocation, summary.ArchiveName, opts.ArtifactPublicBaseURL)
	return CaseEntry{
		ID:                           caseID,
		Oracle:                       summary.Oracle,
		Timestamp:                    summary.Timestamp,
		TiDBVersion:                  summary.TiDBVersion,
		TiDBCommit:                   commit,
		ErrorReason:                  summaryErrorReason(summary),
		PlanSignature:                summary.PlanSignature,
		PlanSigFormat:                summary.PlanSigFormat,
		Expected:                     summary.Expected,
		Actual:                       summary.Actual,
		Error:                        summary.Error,
		GroundTruthDSGMismatchReason: summary.GroundTruthDSGMismatchReason,
		Flaky:                        summary.Flaky,
		NoRECOptimizedSQL:            summary.NoRECOptimizedSQL,
		NoRECUnoptimizedSQL:          summary.NoRECUnoptimizedSQL,
		NoRECPredicate:               summary.NoRECPredicate,
		CaseID:                       caseID,
		CaseDir:                      caseDir,
		ArchiveName:                  summary.ArchiveName,
		ArchiveCodec:                 summary.ArchiveCodec,
		ArchiveURL:                   archiveURL,
		ReportURL:                    reportURL,
		SQL:                          summary.SQL,
		PlanReplay:                   summary.PlanReplay,
		UploadLocation:               summary.UploadLocation,
		Details:                      summary.Details,
		Files:                        files,
	}, nil
}

func readObjectFile(ctx context.Context, client *s3.Client, bucket, key string, maxBytes int) FileContent {
	content, truncated, err := readObjectLimited(ctx, client, bucket, key, maxBytes)
	if err != nil {
		return FileContent{Name: filepath.Base(key)}
	}
	return FileContent{Name: filepath.Base(key), Content: content, Truncated: truncated}
}

func readObjectBytesLimited(ctx context.Context, client *s3.Client, bucket, key string, maxBytes int) ([]byte, bool, error) {
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, false, err
	}
	defer util.CloseWithErr(resp.Body, "s3 response body")
	limit := int64(maxBytes) + 1
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, false, err
	}
	truncated := len(data) > maxBytes
	if truncated {
		data = data[:maxBytes]
	}
	return data, truncated, nil
}

func readObjectLimited(ctx context.Context, client *s3.Client, bucket, key string, maxBytes int) (string, bool, error) {
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "NoSuchKey") || errors.As(err, &nsk) {
			return "", false, fmt.Errorf("missing object %s", key)
		}
		return "", false, err
	}
	defer util.CloseWithErr(resp.Body, "s3 response body")
	limit := int64(maxBytes) + 1
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return "", false, err
	}
	truncated := len(data) > maxBytes
	if truncated {
		data = data[:maxBytes]
	}
	return string(data), truncated, nil
}

func loadGCSCases(ctx context.Context, cfg config.GCSConfig, bucket, prefix string, opts loadOptions) ([]CaseEntry, error) {
	client, err := gcsClientFromConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := client.Close(); err != nil {
			util.Warnf("gcs client close failed: %v", err)
		}
	}()
	keys, objectSet, err := listGCSKeys(ctx, client, bucket, prefix)
	if err != nil {
		return nil, err
	}
	cases := make([]CaseEntry, 0, len(keys))
	for _, key := range keys {
		dir := strings.TrimSuffix(key, "/summary.json")
		entry, err := readCaseFromGCS(ctx, client, bucket, dir, opts, objectSet)
		if err != nil {
			continue
		}
		entry.Dir = "gs://" + bucket + "/" + dir
		if strings.TrimSpace(entry.ID) == "" {
			entry.ID = filepath.Base(dir)
		}
		cases = append(cases, entry)
	}
	return cases, nil
}

func listGCSKeys(ctx context.Context, client *storage.Client, bucket, prefix string) ([]string, map[string]struct{}, error) {
	var keys []string
	objectSet := make(map[string]struct{})
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		key := attrs.Name
		objectSet[key] = struct{}{}
		if strings.HasSuffix(key, "/summary.json") {
			keys = append(keys, key)
		}
	}
	return keys, objectSet, nil
}

func readCaseFromGCS(ctx context.Context, client *storage.Client, bucket, dir string, opts loadOptions, objectSet map[string]struct{}) (CaseEntry, error) {
	summaryKey := dir + "/summary.json"
	summaryData, _, err := readGCSObjectLimited(ctx, client, bucket, summaryKey, opts.MaxBytes)
	if err != nil {
		return CaseEntry{}, err
	}
	var summary report.Summary
	if err := json.Unmarshal([]byte(summaryData), &summary); err != nil {
		return CaseEntry{}, err
	}
	files := map[string]FileContent{}
	files["case.sql"] = readGCSObjectFile(ctx, client, bucket, dir+"/case.sql", opts.MaxBytes)
	files["schema.sql"] = readGCSObjectFile(ctx, client, bucket, dir+"/schema.sql", opts.MaxBytes)
	files["inserts.sql"] = readGCSObjectFile(ctx, client, bucket, dir+"/inserts.sql", opts.MaxBytes)
	files["data.tsv"] = readGCSObjectFile(ctx, client, bucket, dir+"/data.tsv", opts.MaxBytes)
	files["report.json"] = readGCSObjectFile(ctx, client, bucket, dir+"/report.json", opts.MaxBytes)
	if _, ok := objectSet[dir+"/plan_replayer.zip"]; ok {
		files["plan_replayer.zip"] = FileContent{Name: "plan_replayer.zip", Content: "(binary)", Truncated: true}
	}
	archiveKey := dir + "/" + report.CaseArchiveName
	if _, ok := objectSet[archiveKey]; ok {
		files[report.CaseArchiveName] = FileContent{Name: report.CaseArchiveName, Content: "(binary)", Truncated: true}
	}
	commit := extractCommit(summary.TiDBVersion)
	if commit == "" {
		commit = extractCommitFromPlanReplayerGCS(ctx, client, bucket, dir+"/plan_replayer.zip", opts.MaxZipBytes)
	}
	caseID := caseIDFromSummary(summary, filepath.Base(dir))
	caseDir := caseDirFromSummary(summary, caseID)
	reportURL, archiveURL := deriveObjectURLs(summary.UploadLocation, summary.ArchiveName, opts.ArtifactPublicBaseURL)
	return CaseEntry{
		ID:                           caseID,
		Oracle:                       summary.Oracle,
		Timestamp:                    summary.Timestamp,
		TiDBVersion:                  summary.TiDBVersion,
		TiDBCommit:                   commit,
		ErrorReason:                  summaryErrorReason(summary),
		PlanSignature:                summary.PlanSignature,
		PlanSigFormat:                summary.PlanSigFormat,
		Expected:                     summary.Expected,
		Actual:                       summary.Actual,
		Error:                        summary.Error,
		GroundTruthDSGMismatchReason: summary.GroundTruthDSGMismatchReason,
		Flaky:                        summary.Flaky,
		NoRECOptimizedSQL:            summary.NoRECOptimizedSQL,
		NoRECUnoptimizedSQL:          summary.NoRECUnoptimizedSQL,
		NoRECPredicate:               summary.NoRECPredicate,
		CaseID:                       caseID,
		CaseDir:                      caseDir,
		ArchiveName:                  summary.ArchiveName,
		ArchiveCodec:                 summary.ArchiveCodec,
		ArchiveURL:                   archiveURL,
		ReportURL:                    reportURL,
		SQL:                          summary.SQL,
		PlanReplay:                   summary.PlanReplay,
		UploadLocation:               summary.UploadLocation,
		Details:                      summary.Details,
		Files:                        files,
	}, nil
}

func readGCSObjectFile(ctx context.Context, client *storage.Client, bucket, key string, maxBytes int) FileContent {
	content, truncated, err := readGCSObjectLimited(ctx, client, bucket, key, maxBytes)
	if err != nil {
		return FileContent{Name: filepath.Base(key)}
	}
	return FileContent{Name: filepath.Base(key), Content: content, Truncated: truncated}
}

func readGCSObjectBytesLimited(ctx context.Context, client *storage.Client, bucket, key string, maxBytes int) ([]byte, bool, error) {
	rc, err := client.Bucket(bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, false, err
	}
	defer util.CloseWithErr(rc, "gcs response body")
	limit := int64(maxBytes) + 1
	data, err := io.ReadAll(io.LimitReader(rc, limit))
	if err != nil {
		return nil, false, err
	}
	truncated := len(data) > maxBytes
	if truncated {
		data = data[:maxBytes]
	}
	return data, truncated, nil
}

func readGCSObjectLimited(ctx context.Context, client *storage.Client, bucket, key string, maxBytes int) (string, bool, error) {
	rc, err := client.Bucket(bucket).Object(key).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return "", false, fmt.Errorf("missing object %s", key)
		}
		return "", false, err
	}
	defer util.CloseWithErr(rc, "gcs response body")
	limit := int64(maxBytes) + 1
	data, err := io.ReadAll(io.LimitReader(rc, limit))
	if err != nil {
		return "", false, err
	}
	truncated := len(data) > maxBytes
	if truncated {
		data = data[:maxBytes]
	}
	return string(data), truncated, nil
}

func gcsClientFromConfig(ctx context.Context, cfg config.GCSConfig) (*storage.Client, error) {
	opts := []option.ClientOption{}
	if strings.TrimSpace(cfg.CredentialsFile) != "" {
		opts = append(opts, option.WithCredentialsFile(strings.TrimSpace(cfg.CredentialsFile)))
	}
	return storage.NewClient(ctx, opts...)
}

func s3ClientFromConfig(ctx context.Context, cfg config.S3Config) (*s3.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.Endpoint != "" {
		//nolint:staticcheck // AWS SDK v2 global endpoint resolver is deprecated, but required for custom S3 endpoints.
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, _ string, _ ...any) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				//nolint:staticcheck // AWS SDK v2 global endpoint resolver is deprecated, but required for custom S3 endpoints.
				return aws.Endpoint{URL: cfg.Endpoint, HostnameImmutable: true}, nil
			}
			//nolint:staticcheck // AWS SDK v2 global endpoint resolver is deprecated, but required for custom S3 endpoints.
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		//nolint:staticcheck // AWS SDK v2 global endpoint resolver is deprecated, but required for custom S3 endpoints.
		opts = append(opts, awsconfig.WithEndpointResolverWithOptions(resolver))
	}
	if cfg.AccessKeyID != "" || cfg.SecretAccessKey != "" || cfg.SessionToken != "" {
		creds := credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
		opts = append(opts, awsconfig.WithCredentialsProvider(creds))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.UsePathStyle
	})
	return client, nil
}

func caseIDFromSummary(summary report.Summary, fallback string) string {
	if id := strings.TrimSpace(summary.CaseID); id != "" {
		return id
	}
	if id := strings.TrimSpace(summary.CaseDir); id != "" {
		return id
	}
	return fallback
}

func caseDirFromSummary(summary report.Summary, caseID string) string {
	if v := strings.TrimSpace(summary.CaseDir); v != "" {
		return v
	}
	return caseID
}

func deriveObjectURLs(uploadLocation, archiveName, artifactPublicBaseURL string) (reportURL string, archiveURL string) {
	base := strings.TrimSpace(uploadLocation)
	if base == "" {
		return "", ""
	}
	reportURL = deriveUploadObjectURL(base, "report.json", artifactPublicBaseURL)
	if strings.TrimSpace(archiveName) != "" {
		archiveURL = deriveUploadObjectURL(base, archiveName, artifactPublicBaseURL)
	}
	return reportURL, archiveURL
}

func deriveUploadObjectURL(uploadLocation, name, artifactPublicBaseURL string) string {
	trimmedName := strings.TrimSpace(name)
	if trimmedName == "" {
		return ""
	}
	trimmedUpload := strings.TrimSpace(uploadLocation)
	if trimmedUpload == "" {
		return ""
	}
	if isHTTPURL(trimmedUpload) {
		return objectURL(trimmedUpload, trimmedName)
	}
	if isS3URL(trimmedUpload) {
		return deriveCloudObjectURL(trimmedUpload, trimmedName, artifactPublicBaseURL, parseS3URI)
	}
	if isGCSURL(trimmedUpload) {
		return deriveCloudObjectURL(trimmedUpload, trimmedName, artifactPublicBaseURL, parseGCSURI)
	}
	return ""
}

func isHTTPURL(url string) bool {
	lower := strings.ToLower(strings.TrimSpace(url))
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

func isS3URL(url string) bool {
	lower := strings.ToLower(strings.TrimSpace(url))
	return strings.HasPrefix(lower, "s3://")
}

func isGCSURL(url string) bool {
	lower := strings.ToLower(strings.TrimSpace(url))
	return strings.HasPrefix(lower, "gs://")
}

func objectURL(base, name string) string {
	trimmedBase := strings.TrimRight(strings.TrimSpace(base), "/")
	trimmedName := strings.TrimLeft(strings.TrimSpace(name), "/")
	if trimmedBase == "" || trimmedName == "" {
		return ""
	}
	return trimmedBase + "/" + trimmedName
}

func deriveCloudObjectURL(uploadLocation, name, artifactPublicBaseURL string, parse func(string) (string, string, error)) string {
	publicBase := strings.TrimSpace(artifactPublicBaseURL)
	if publicBase == "" {
		return ""
	}
	_, prefix, err := parse(uploadLocation)
	if err != nil {
		return ""
	}
	key := objectKey(prefix, name)
	if strings.TrimSpace(key) == "" {
		return ""
	}
	return objectURL(publicBase, key)
}

func summaryErrorReason(summary report.Summary) string {
	if reason := strings.TrimSpace(summary.ErrorReason); reason != "" {
		return reason
	}
	if summary.Details == nil {
		return ""
	}
	v, ok := summary.Details["error_reason"]
	if !ok {
		return ""
	}
	reason, ok := v.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(reason)
}

func collectPublishFiles(output string) ([]string, error) {
	files := []string{"report.json", "reports.json", "reports.index.json"}
	seen := map[string]struct{}{
		"report.json":        {},
		"reports.json":       {},
		"reports.index.json": {},
	}
	summaryRoot := filepath.Join(output, "cases")
	if _, err := os.Stat(summaryRoot); err != nil {
		if os.IsNotExist(err) {
			return files, nil
		}
		return nil, err
	}
	if err := filepath.Walk(summaryRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info == nil || info.IsDir() {
			return nil
		}
		if !strings.EqualFold(filepath.Ext(path), ".json") {
			return nil
		}
		rel, err := filepath.Rel(output, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if _, ok := seen[rel]; ok {
			return nil
		}
		seen[rel] = struct{}{}
		files = append(files, rel)
		return nil
	}); err != nil {
		return nil, err
	}
	return files, nil
}

func publishReports(ctx context.Context, opts publishOptions, output string) (string, error) {
	gcsEnabled := opts.GCS.Enabled && strings.TrimSpace(opts.GCS.Bucket) != ""
	s3Enabled := opts.S3.Enabled && strings.TrimSpace(opts.S3.Bucket) != ""
	if !gcsEnabled && !s3Enabled {
		return "", nil
	}
	publishFiles, err := collectPublishFiles(output)
	if err != nil {
		return "", err
	}
	if gcsEnabled {
		if s3Enabled {
			util.Warnf("publish targets include both gcs and s3; using gcs")
		}
		client, err := gcsClientFromConfig(ctx, opts.GCS)
		if err != nil {
			return "", err
		}
		defer func() {
			if err := client.Close(); err != nil {
				util.Warnf("gcs client close failed: %v", err)
			}
		}()
		for _, name := range publishFiles {
			data, err := os.ReadFile(filepath.Join(output, filepath.FromSlash(name)))
			if err != nil {
				return "", err
			}
			key := objectKey(opts.GCS.Prefix, name)
			writer := client.Bucket(opts.GCS.Bucket).Object(key).NewWriter(ctx)
			writer.ContentType = "application/json"
			_, copyErr := io.Copy(writer, bytes.NewReader(data))
			closeErr := writer.Close()
			if copyErr != nil {
				return "", copyErr
			}
			if closeErr != nil {
				return "", closeErr
			}
		}
		reportKey := objectKey(opts.GCS.Prefix, "reports.json")
		if strings.TrimSpace(opts.PublicBaseURL) != "" {
			return objectURL(opts.PublicBaseURL, reportKey), nil
		}
		return fmt.Sprintf("gs://%s/%s", opts.GCS.Bucket, reportKey), nil
	}
	client, err := s3ClientFromConfig(ctx, opts.S3)
	if err != nil {
		return "", err
	}
	for _, name := range publishFiles {
		data, err := os.ReadFile(filepath.Join(output, filepath.FromSlash(name)))
		if err != nil {
			return "", err
		}
		key := objectKey(opts.S3.Prefix, name)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(opts.S3.Bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(data),
			ContentLength: aws.Int64(int64(len(data))),
			ContentType:   aws.String("application/json"),
		})
		if err != nil {
			return "", err
		}
	}
	reportKey := objectKey(opts.S3.Prefix, "reports.json")
	if strings.TrimSpace(opts.PublicBaseURL) != "" {
		return objectURL(opts.PublicBaseURL, reportKey), nil
	}
	return fmt.Sprintf("s3://%s/%s", opts.S3.Bucket, reportKey), nil
}

func objectKey(prefix, name string) string {
	trimmedPrefix := strings.Trim(prefix, "/")
	trimmedName := strings.TrimLeft(strings.TrimSpace(name), "/")
	if trimmedPrefix == "" {
		return trimmedName
	}
	return trimmedPrefix + "/" + trimmedName
}

func syncWorkerMetadata(ctx context.Context, opts workerSyncOptions, manifestURL string, site SiteData) error {
	if strings.TrimSpace(opts.Endpoint) == "" {
		return nil
	}
	const workerSyncTimeout = 20 * time.Second
	payload := workerSyncPayload{
		ManifestURL: manifestURL,
		GeneratedAt: site.GeneratedAt,
		Source:      site.Source,
		Cases:       make([]workerSyncCase, 0, len(site.Cases)),
	}
	for _, c := range site.Cases {
		caseID := strings.TrimSpace(c.CaseID)
		if caseID == "" {
			caseID = strings.TrimSpace(c.ID)
		}
		payload.Cases = append(payload.Cases, workerSyncCase{
			CaseID:         caseID,
			Oracle:         strings.TrimSpace(c.Oracle),
			Timestamp:      strings.TrimSpace(c.Timestamp),
			ErrorReason:    strings.TrimSpace(c.ErrorReason),
			Error:          strings.TrimSpace(c.Error),
			UploadLocation: strings.TrimSpace(c.UploadLocation),
			ReportURL:      strings.TrimSpace(c.ReportURL),
			ArchiveURL:     strings.TrimSpace(c.ArchiveURL),
		})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	requestCtx, cancel := context.WithTimeout(ctx, workerSyncTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, opts.Endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(opts.Token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: workerSyncTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	msg, readErr := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if readErr != nil {
		return fmt.Errorf("worker sync failed status=%d and cannot read body: %w", resp.StatusCode, readErr)
	}
	return fmt.Errorf("worker sync failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
}

var commitPattern = regexp.MustCompile(`(?i)(?:git commit hash|git hash|commit|git commit)\s*:\s*([0-9a-f]{7,40})`)
var hexPattern = regexp.MustCompile(`\b[0-9a-f]{7,40}\b`)

func extractCommit(version string) string {
	if version == "" {
		return ""
	}
	if m := commitPattern.FindStringSubmatch(version); len(m) > 1 {
		return m[1]
	}
	if m := hexPattern.FindStringSubmatch(version); len(m) > 0 {
		return m[0]
	}
	return ""
}

func extractCommitFromPlanReplayer(zipPath string, maxBytes int) string {
	f, err := os.Open(zipPath)
	if err != nil {
		return ""
	}
	defer util.CloseWithErr(f, "report output")
	info, err := f.Stat()
	if err != nil {
		return ""
	}
	if maxBytes > 0 && info.Size() > int64(maxBytes) {
		return ""
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return ""
	}
	return extractCommitFromPlanReplayerData(data)
}

func extractCommitFromPlanReplayerS3(ctx context.Context, client *s3.Client, bucket, key string, maxBytes int) string {
	data, truncated, err := readObjectBytesLimited(ctx, client, bucket, key, maxBytes)
	if err != nil || truncated {
		return ""
	}
	return extractCommitFromPlanReplayerData(data)
}

func extractCommitFromPlanReplayerGCS(ctx context.Context, client *storage.Client, bucket, key string, maxBytes int) string {
	data, truncated, err := readGCSObjectBytesLimited(ctx, client, bucket, key, maxBytes)
	if err != nil || truncated {
		return ""
	}
	return extractCommitFromPlanReplayerData(data)
}

func extractCommitFromPlanReplayerData(data []byte) string {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return ""
	}
	for _, zf := range reader.File {
		name := strings.ToLower(zf.Name)
		if !strings.Contains(name, "meta") {
			continue
		}
		rc, err := zf.Open()
		if err != nil {
			continue
		}
		content, err := io.ReadAll(rc)
		util.CloseWithErr(rc, "zip entry")
		if err != nil {
			continue
		}
		if commit := extractCommit(string(content)); commit != "" {
			return commit
		}
	}
	return ""
}
