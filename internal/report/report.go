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

	"github.com/google/uuid"
)

type Reporter struct {
	OutputDir       string
	MaxDataDumpRows int
	caseSeq         int
}

type Case struct {
	Dir string
}

type Summary struct {
	Oracle         string         `json:"oracle"`
	SQL            []string       `json:"sql"`
	Expected       string         `json:"expected"`
	Actual         string         `json:"actual"`
	Error          string         `json:"error"`
	PlanReplay     string         `json:"plan_replayer"`
	UploadLocation string         `json:"upload_location"`
	Details        map[string]any `json:"details"`
	Timestamp      string         `json:"timestamp"`
	TiDBVersion    string         `json:"tidb_version"`
}

func New(outputDir string, maxRows int) *Reporter {
	return &Reporter{OutputDir: outputDir, MaxDataDumpRows: maxRows}
}

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

func (r *Reporter) WriteSummary(c Case, summary Summary) error {
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(c.Dir, "summary.json"), data, 0o644)
}

func (r *Reporter) WriteSQL(c Case, name string, statements []string) error {
	content := strings.Join(statements, ";\n") + ";\n"
	return os.WriteFile(filepath.Join(c.Dir, name), []byte(content), 0o644)
}

func (r *Reporter) DumpSchema(ctx context.Context, c Case, exec *db.DB, state *schema.State) error {
	var b strings.Builder
	for _, tbl := range state.Tables {
		row := exec.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", tbl.Name))
		var name, createSQL string
		if err := row.Scan(&name, &createSQL); err != nil {
			continue
		}
		b.WriteString(createSQL)
		b.WriteString(";\n\n")
	}
	return os.WriteFile(filepath.Join(c.Dir, "schema.sql"), []byte(b.String()), 0o644)
}

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
		rows.Close()
		b.WriteString("\n")
	}
	return os.WriteFile(filepath.Join(c.Dir, "data.tsv"), []byte(b.String()), 0o644)
}
