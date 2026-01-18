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
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"shiro/internal/config"
	"shiro/internal/report"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type FileContent struct {
	Name      string `json:"name"`
	Content   string `json:"content"`
	Truncated bool   `json:"truncated"`
}

type CaseEntry struct {
	ID             string                 `json:"id"`
	Dir            string                 `json:"dir"`
	Oracle         string                 `json:"oracle"`
	Timestamp      string                 `json:"timestamp"`
	TiDBVersion    string                 `json:"tidb_version"`
	TiDBCommit     string                 `json:"tidb_commit"`
	PlanSignature  string                 `json:"plan_signature"`
	PlanSigFormat  string                 `json:"plan_signature_format"`
	Expected       string                 `json:"expected"`
	Actual         string                 `json:"actual"`
	Error          string                 `json:"error"`
	SQL            []string               `json:"sql"`
	PlanReplay     string                 `json:"plan_replayer"`
	UploadLocation string                 `json:"upload_location"`
	Details        map[string]any         `json:"details"`
	Files          map[string]FileContent `json:"files"`
}

type SiteData struct {
	GeneratedAt string      `json:"generated_at"`
	Source      string      `json:"source"`
	Cases       []CaseEntry `json:"cases"`
}

type loadOptions struct {
	MaxBytes    int
	MaxZipBytes int
}

func main() {
	input := flag.String("input", "reports", "input directory or s3://bucket/prefix")
	output := flag.String("output", "web/public", "output directory for report.json")
	configPath := flag.String("config", "config.yaml", "path to config file (for S3 access)")
	maxBytes := flag.Int("max-bytes", 64*1024, "max bytes to read per case file")
	maxZipBytes := flag.Int("max-zip-bytes", 20*1024*1024, "max bytes to read for plan_replayer.zip")
	flag.Parse()

	opts := loadOptions{MaxBytes: *maxBytes, MaxZipBytes: *maxZipBytes}
	ctx := context.Background()

	var cases []CaseEntry
	var err error
	if strings.HasPrefix(*input, "s3://") {
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
	fmt.Printf("report json written to %s\n", filepath.Join(*output, "report.json"))
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func loadLocalCases(root string, opts loadOptions) ([]CaseEntry, error) {
	pattern := filepath.Join(root, "case_*", "summary.json")
	summaries, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	cases := make([]CaseEntry, 0, len(summaries))
	for _, path := range summaries {
		dir := filepath.Dir(path)
		entry, err := readCaseFromDir(dir, opts)
		if err != nil {
			continue
		}
		entry.Dir = dir
		entry.ID = filepath.Base(dir)
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
	if _, err := os.Stat(filepath.Join(dir, "plan_replayer.zip")); err == nil {
		files["plan_replayer.zip"] = FileContent{Name: "plan_replayer.zip", Content: "(binary)", Truncated: true}
	}
	commit := extractCommit(summary.TiDBVersion)
	if commit == "" {
		commit = extractCommitFromPlanReplayer(filepath.Join(dir, "plan_replayer.zip"), opts.MaxZipBytes)
	}
	return CaseEntry{
		Oracle:         summary.Oracle,
		Timestamp:      summary.Timestamp,
		TiDBVersion:    summary.TiDBVersion,
		TiDBCommit:     commit,
		PlanSignature:  summary.PlanSignature,
		PlanSigFormat:  summary.PlanSigFormat,
		Expected:       summary.Expected,
		Actual:         summary.Actual,
		Error:          summary.Error,
		SQL:            summary.SQL,
		PlanReplay:     summary.PlanReplay,
		UploadLocation: summary.UploadLocation,
		Details:        summary.Details,
		Files:          files,
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
	defer f.Close()
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
	jsonBytes, err := json.MarshalIndent(site, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(output, "report.json"), jsonBytes, 0o644)
}

func parseS3URI(input string) (string, string, error) {
	trimmed := strings.TrimPrefix(input, "s3://")
	if trimmed == "" {
		return "", "", fmt.Errorf("missing s3 bucket")
	}
	parts := strings.SplitN(trimmed, "/", 2)
	bucket := parts[0]
	prefix := ""
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
	keys, err := listSummaryKeys(ctx, client, bucket, prefix)
	if err != nil {
		return nil, err
	}
	cases := make([]CaseEntry, 0, len(keys))
	for _, key := range keys {
		dir := strings.TrimSuffix(key, "/summary.json")
		entry, err := readCaseFromS3(ctx, client, bucket, dir, opts)
		if err != nil {
			continue
		}
		entry.Dir = "s3://" + bucket + "/" + dir
		entry.ID = filepath.Base(dir)
		cases = append(cases, entry)
	}
	return cases, nil
}

func listSummaryKeys(ctx context.Context, client *s3.Client, bucket, prefix string) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if strings.HasSuffix(key, "/summary.json") {
				keys = append(keys, key)
			}
		}
	}
	return keys, nil
}

func readCaseFromS3(ctx context.Context, client *s3.Client, bucket, dir string, opts loadOptions) (CaseEntry, error) {
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
	commit := extractCommit(summary.TiDBVersion)
	if commit == "" {
		commit = extractCommitFromPlanReplayerS3(ctx, client, bucket, dir+"/plan_replayer.zip", opts.MaxZipBytes)
	}
	return CaseEntry{
		Oracle:         summary.Oracle,
		Timestamp:      summary.Timestamp,
		TiDBVersion:    summary.TiDBVersion,
		TiDBCommit:     commit,
		PlanSignature:  summary.PlanSignature,
		PlanSigFormat:  summary.PlanSigFormat,
		Expected:       summary.Expected,
		Actual:         summary.Actual,
		Error:          summary.Error,
		SQL:            summary.SQL,
		PlanReplay:     summary.PlanReplay,
		UploadLocation: summary.UploadLocation,
		Details:        summary.Details,
		Files:          files,
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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

func s3ClientFromConfig(ctx context.Context, cfg config.S3Config) (*s3.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.Endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{URL: cfg.Endpoint, HostnameImmutable: true}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
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
	defer f.Close()
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
		rc.Close()
		if err != nil {
			continue
		}
		if commit := extractCommit(string(content)); commit != "" {
			return commit
		}
	}
	return ""
}
