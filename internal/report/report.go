package report

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"shiro/internal/db"
	"shiro/internal/schema"
	"shiro/internal/util"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

// Reporter writes case artifacts to disk.
type Reporter struct {
	OutputDir       string
	MaxDataDumpRows int
	UseUUIDPath     bool
	caseSeq         int
}

// Case describes a report directory.
type Case struct {
	ID  string
	Dir string
}

// Summary captures the persisted metadata for a case.
type Summary struct {
	Oracle                       string         `json:"oracle"`
	SQL                          []string       `json:"sql"`
	Expected                     string         `json:"expected"`
	Actual                       string         `json:"actual"`
	Error                        string         `json:"error"`
	ErrorReason                  string         `json:"error_reason"`
	BugHint                      string         `json:"bug_hint"`
	GroundTruthDSGMismatchReason string         `json:"groundtruth_dsg_mismatch_reason"`
	ErrorSQL                     string         `json:"error_sql"`
	ReplaySQL                    string         `json:"replay_sql"`
	MinimizeStatus               string         `json:"minimize_status"`
	Flaky                        bool           `json:"flaky"`
	Seed                         int64          `json:"seed"`
	PlanReplay                   string         `json:"plan_replayer"`
	UploadLocation               string         `json:"upload_location"`
	CaseID                       string         `json:"case_id"`
	CaseDir                      string         `json:"case_dir"`
	ArchiveName                  string         `json:"archive_name"`
	ArchiveCodec                 string         `json:"archive_codec"`
	NoRECOptimizedSQL            string         `json:"norec_optimized_sql"`
	NoRECUnoptimizedSQL          string         `json:"norec_unoptimized_sql"`
	NoRECPredicate               string         `json:"norec_predicate"`
	Details                      map[string]any `json:"details"`
	GroundTruth                  *TruthSummary  `json:"groundtruth,omitempty"`
	Timestamp                    string         `json:"timestamp"`
	TiDBVersion                  string         `json:"tidb_version"`
	PlanSignature                string         `json:"plan_signature"`
	PlanSigFormat                string         `json:"plan_signature_format"`
}

// TruthSummary captures optional ground-truth evaluation metadata.
type TruthSummary struct {
	Mismatch bool   `json:"mismatch"`
	JoinSig  string `json:"join_sig"`
	RowCount int    `json:"row_count"`
}

// New creates a reporter that writes to outputDir.
func New(outputDir string, maxRows int) *Reporter {
	return &Reporter{OutputDir: outputDir, MaxDataDumpRows: maxRows}
}

// NewCase allocates a new case directory.
func (r *Reporter) NewCase() (Case, error) {
	r.caseSeq++
	caseID := uuid.New().String()
	if v7, err := uuid.NewV7(); err == nil {
		caseID = v7.String()
	}
	caseDir := fmt.Sprintf("case_%04d_%s", r.caseSeq, caseID)
	if r.UseUUIDPath {
		caseDir = caseID
	}
	dir := filepath.Join(r.OutputDir, caseDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Case{}, err
	}
	_ = os.WriteFile(filepath.Join(dir, "README.md"), []byte("# Reproduce Case\n\n- Apply schema: schema.sql\n- Load data: inserts.sql (preferred) or data.tsv\n- Run query: case.sql\n- Plan replayer: plan_replayer.zip (if present)\n"), 0o644)
	return Case{ID: caseID, Dir: dir}, nil
}

const (
	CaseArchiveName  = "case.tar.zst"
	CaseArchiveCodec = "zstd"
)

// WriteSummary writes summary.json into the case directory.
func (r *Reporter) WriteSummary(c Case, summary Summary) error {
	return r.writeSummaryFile(c, "summary.json", summary)
}

// WriteReport writes report.json into the case directory.
func (r *Reporter) WriteReport(c Case, summary Summary) error {
	return r.writeSummaryFile(c, "report.json", summary)
}

func (r *Reporter) writeSummaryFile(c Case, name string, summary Summary) error {
	f, err := os.Create(filepath.Join(c.Dir, name))
	if err != nil {
		return err
	}
	defer util.CloseWithErr(f, "summary output")
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return encodeSummaryStable(enc, summary)
}

// WriteSQL writes a SQL file from the provided statements.
func (r *Reporter) WriteSQL(c Case, name string, statements []string) error {
	content := strings.Join(statements, ";\n") + ";\n"
	path := filepath.Join(c.Dir, name)
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

// WriteText writes raw text content into the case directory.
func (r *Reporter) WriteText(c Case, name string, content string) error {
	path := filepath.Join(c.Dir, name)
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

// WriteCaseArchive creates a compressed archive for the case directory.
func (r *Reporter) WriteCaseArchive(c Case) (name string, codec string, err error) {
	archivePath := filepath.Join(c.Dir, CaseArchiveName)
	if removeErr := os.Remove(archivePath); removeErr != nil && !os.IsNotExist(removeErr) {
		return "", "", removeErr
	}
	defer func() {
		if err != nil {
			_ = os.Remove(archivePath)
		}
	}()
	file, err := os.Create(archivePath)
	if err != nil {
		return "", "", err
	}
	defer util.CloseWithErr(file, "archive output")

	zw, err := zstd.NewWriter(file)
	if err != nil {
		return "", "", err
	}
	defer func() {
		if closeErr := zw.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	tw := tar.NewWriter(zw)
	defer func() {
		if closeErr := tw.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	walkErr := filepath.WalkDir(c.Dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || path == archivePath {
			return nil
		}
		rel, err := filepath.Rel(c.Dir, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		if _, err := io.Copy(tw, src); err != nil {
			util.CloseWithErr(src, "archive source")
			return err
		}
		util.CloseWithErr(src, "archive source")
		return nil
	})
	if walkErr != nil {
		return "", "", walkErr
	}
	return CaseArchiveName, CaseArchiveCodec, nil
}

// DumpSchema writes schema.sql for the current state.
func (r *Reporter) DumpSchema(ctx context.Context, c Case, exec *db.DB, state *schema.State) error {
	var b strings.Builder
	b.WriteString("SET FOREIGN_KEY_CHECKS=0;\n")
	tables, views := schema.SplitTablesByView(state.Tables)
	for i := len(views) - 1; i >= 0; i-- {
		b.WriteString(fmt.Sprintf("DROP VIEW IF EXISTS %s;\n", views[i].Name))
	}
	for i := len(tables) - 1; i >= 0; i-- {
		b.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS %s;\n", tables[i].Name))
	}
	for _, tbl := range tables {
		row := exec.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", tbl.Name))
		var name, createSQL string
		if err := row.Scan(&name, &createSQL); err != nil {
			continue
		}
		b.WriteString(createSQL)
		b.WriteString(";\n\n")
	}
	for _, view := range views {
		row := exec.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE VIEW %s", view.Name))
		var name, createSQL, charset, collation string
		if err := row.Scan(&name, &createSQL, &charset, &collation); err != nil {
			util.Warnf("dump view failed view=%s err=%v", view.Name, err)
			b.WriteString(fmt.Sprintf("-- failed to dump view %s: %v\n\n", view.Name, err))
			continue
		}
		b.WriteString(normalizeCreateView(createSQL))
		b.WriteString(";\n\n")
	}
	b.WriteString("SET FOREIGN_KEY_CHECKS=1;\n")
	return os.WriteFile(filepath.Join(c.Dir, "schema.sql"), []byte(b.String()), 0o644)
}

func normalizeCreateView(sql string) string {
	definerRe := regexp.MustCompile(`(?i)\s+DEFINER=\S+`)
	out := definerRe.ReplaceAllString(sql, "")
	out = strings.ReplaceAll(out, "SQL SECURITY DEFINER", "SQL SECURITY INVOKER")
	return strings.TrimSpace(out)
}

// DumpData writes data.tsv with capped rows per table.
func (r *Reporter) DumpData(ctx context.Context, c Case, exec *db.DB, state *schema.State) error {
	var b strings.Builder
	for _, tbl := range sortedTables(state.Tables) {
		b.WriteString(fmt.Sprintf("-- %s\n", tbl.Name))
		orderBy := stableOrderBy(tbl)
		sql := fmt.Sprintf("SELECT * FROM %s", tbl.Name)
		if orderBy != "" {
			sql += " ORDER BY " + orderBy
		}
		sql += fmt.Sprintf(" LIMIT %d", r.MaxDataDumpRows)
		rows, err := exec.QueryContext(ctx, sql)
		if err != nil {
			continue
		}
		cols, _ := rows.Columns()
		values := make([][]byte, len(cols))
		scanArgs := make([]any, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		for rows.Next() {
			if err := rows.Scan(scanArgs...); err != nil {
				break
			}
			row := make([]string, 0, len(cols))
			for _, v := range values {
				if v == nil {
					row = append(row, "NULL")
				} else {
					row = append(row, string(v))
				}
			}
			b.WriteString(strings.Join(row, "\t"))
			b.WriteString("\n")
		}
		util.CloseWithErr(rows, "schema rows")
		b.WriteString("\n")
	}
	return os.WriteFile(filepath.Join(c.Dir, "data.tsv"), []byte(b.String()), 0o644)
}

func sortedTables(tables []schema.Table) []schema.Table {
	if len(tables) < 2 {
		return tables
	}
	out := append([]schema.Table(nil), tables...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func stableOrderBy(tbl schema.Table) string {
	if _, ok := tbl.ColumnByName("id"); ok {
		return "`id`"
	}
	if len(tbl.Columns) == 0 {
		return ""
	}
	names := make([]string, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		if col.Name == "" {
			continue
		}
		names = append(names, col.Name)
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return fmt.Sprintf("`%s`", names[0])
}

func encodeSummaryStable(enc *json.Encoder, summary Summary) error {
	type summaryAlias Summary
	alias := summaryAlias(summary)
	rawDetails, err := encodeOrderedValue(alias.Details)
	if err != nil {
		return err
	}
	alias.Details = nil
	payload := struct {
		summaryAlias
		Details json.RawMessage `json:"details"`
	}{
		summaryAlias: alias,
		Details:      rawDetails,
	}
	return enc.Encode(payload)
}

func encodeOrderedValue(v any) (json.RawMessage, error) {
	if v == nil {
		return json.RawMessage("null"), nil
	}
	buf := &strings.Builder{}
	if err := writeOrderedJSON(buf, v); err != nil {
		return nil, err
	}
	return json.RawMessage(buf.String()), nil
}

func writeOrderedJSON(w io.Writer, v any) error {
	if v == nil {
		_, err := io.WriteString(w, "null")
		return err
	}
	if raw, ok := v.(json.RawMessage); ok {
		_, err := w.Write(raw)
		return err
	}
	switch val := v.(type) {
	case map[string]any:
		return writeOrderedMap(w, val)
	case []any:
		return writeOrderedSlice(w, val)
	}
	rv := reflect.ValueOf(v)
	if rv.IsValid() {
		switch rv.Kind() {
		case reflect.Map:
			if rv.Type().Key().Kind() == reflect.String {
				return writeOrderedMapValue(w, rv)
			}
		case reflect.Slice, reflect.Array:
			return writeOrderedSliceValue(w, rv)
		}
	}
	return writeScalarJSON(w, v)
}

func writeOrderedMap(w io.Writer, m map[string]any) error {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if _, err := io.WriteString(w, "{"); err != nil {
		return err
	}
	for i, k := range keys {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		keyJSON, err := json.Marshal(k)
		if err != nil {
			return err
		}
		if _, err := w.Write(keyJSON); err != nil {
			return err
		}
		if _, err := io.WriteString(w, ":"); err != nil {
			return err
		}
		if err := writeOrderedJSON(w, m[k]); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "}")
	return err
}

func writeOrderedMapValue(w io.Writer, rv reflect.Value) error {
	keys := rv.MapKeys()
	strKeys := make([]string, 0, len(keys))
	for _, k := range keys {
		strKeys = append(strKeys, k.String())
	}
	sort.Strings(strKeys)
	if _, err := io.WriteString(w, "{"); err != nil {
		return err
	}
	for i, key := range strKeys {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		if err := writeScalarJSON(w, key); err != nil {
			return err
		}
		if _, err := io.WriteString(w, ":"); err != nil {
			return err
		}
		val := rv.MapIndex(reflect.ValueOf(key))
		if err := writeOrderedJSON(w, val.Interface()); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "}")
	return err
}

func writeOrderedSlice(w io.Writer, vals []any) error {
	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	for i, item := range vals {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		if err := writeOrderedJSON(w, item); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "]")
	return err
}

func writeOrderedSliceValue(w io.Writer, rv reflect.Value) error {
	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	for i := 0; i < rv.Len(); i++ {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		if err := writeOrderedJSON(w, rv.Index(i).Interface()); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "]")
	return err
}

func writeScalarJSON(w io.Writer, v any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return err
	}
	data := bytes.TrimRight(buf.Bytes(), "\n")
	_, err := w.Write(data)
	return err
}
