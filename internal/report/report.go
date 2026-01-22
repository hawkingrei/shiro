package report

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"shiro/internal/db"
	"shiro/internal/schema"
	"shiro/internal/util"

	"github.com/google/uuid"
)

// Reporter writes case artifacts to disk.
type Reporter struct {
	OutputDir       string
	MaxDataDumpRows int
	caseSeq         int
}

// Case describes a report directory.
type Case struct {
	Dir string
}

// Summary captures the persisted metadata for a case.
type Summary struct {
	Oracle              string         `json:"oracle"`
	SQL                 []string       `json:"sql"`
	Expected            string         `json:"expected"`
	Actual              string         `json:"actual"`
	Error               string         `json:"error"`
	Seed                int64          `json:"seed"`
	PlanReplay          string         `json:"plan_replayer"`
	UploadLocation      string         `json:"upload_location"`
	CaseDir             string         `json:"case_dir"`
	NoRECOptimizedSQL   string         `json:"norec_optimized_sql"`
	NoRECUnoptimizedSQL string         `json:"norec_unoptimized_sql"`
	NoRECPredicate      string         `json:"norec_predicate"`
	Details             map[string]any `json:"details"`
	Timestamp           string         `json:"timestamp"`
	TiDBVersion         string         `json:"tidb_version"`
	PlanSignature       string         `json:"plan_signature"`
	PlanSigFormat       string         `json:"plan_signature_format"`
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
	dir := filepath.Join(r.OutputDir, fmt.Sprintf("case_%04d_%s", r.caseSeq, caseID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Case{}, err
	}
	_ = os.WriteFile(filepath.Join(dir, "README.md"), []byte("# Reproduce Case\n\n- Apply schema: schema.sql\n- Load data: inserts.sql (preferred) or data.tsv\n- Run query: case.sql\n- Plan replayer: plan_replayer.zip (if present)\n"), 0o644)
	return Case{Dir: dir}, nil
}

// WriteSummary writes summary.json into the case directory.
func (r *Reporter) WriteSummary(c Case, summary Summary) error {
	f, err := os.Create(filepath.Join(c.Dir, "summary.json"))
	if err != nil {
		return err
	}
	defer util.CloseWithErr(f, "summary output")
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(summary)
}

// WriteSQL writes a SQL file from the provided statements.
func (r *Reporter) WriteSQL(c Case, name string, statements []string) error {
	content := strings.Join(statements, ";\n") + ";\n"
	return os.WriteFile(filepath.Join(c.Dir, name), []byte(content), 0o644)
}

// DumpSchema writes schema.sql for the current state.
func (r *Reporter) DumpSchema(ctx context.Context, c Case, exec *db.DB, state *schema.State) error {
	var b strings.Builder
	b.WriteString("SET FOREIGN_KEY_CHECKS=0;\n")
	for _, tbl := range state.Tables {
		b.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS %s;\n", tbl.Name))
		row := exec.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", tbl.Name))
		var name, createSQL string
		if err := row.Scan(&name, &createSQL); err != nil {
			continue
		}
		b.WriteString(createSQL)
		b.WriteString(";\n\n")
	}
	b.WriteString("SET FOREIGN_KEY_CHECKS=1;\n")
	return os.WriteFile(filepath.Join(c.Dir, "schema.sql"), []byte(b.String()), 0o644)
}

// DumpData writes data.tsv with capped rows per table.
func (r *Reporter) DumpData(ctx context.Context, c Case, exec *db.DB, state *schema.State) error {
	var b strings.Builder
	for _, tbl := range state.Tables {
		b.WriteString(fmt.Sprintf("-- %s\n", tbl.Name))
		rows, err := exec.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT %d", tbl.Name, r.MaxDataDumpRows))
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
