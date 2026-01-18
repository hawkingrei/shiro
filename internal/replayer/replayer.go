package replayer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"shiro/internal/config"
	"shiro/internal/db"
)

// Replayer handles plan replayer dumps and downloads.
type Replayer struct {
	cfg config.PlanReplayer
}

// New constructs a Replayer from config.
func New(cfg config.PlanReplayer) *Replayer {
	return &Replayer{cfg: cfg}
}

// DumpAndDownload triggers PLAN REPLAYER DUMP and downloads the zip to caseDir.
func (r *Replayer) DumpAndDownload(ctx context.Context, exec *db.DB, sql string, caseDir string) (string, error) {
	if !r.cfg.Enabled {
		return "", nil
	}
	dumpSQL := r.buildDumpSQL(sql)
	rows, err := exec.QueryContext(ctx, dumpSQL)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}

	var parts []string
	for rows.Next() {
		values := make([][]byte, len(cols))
		scanArgs := make([]any, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return "", err
		}
		for _, v := range values {
			if len(v) > 0 {
				parts = append(parts, string(v))
			}
		}
	}
	text := strings.Join(parts, " ")
	url := extractURL(text)
	zipName := ""
	if url == "" {
		zipName = extractZipName(text)
		if zipName == "" {
			zipName = r.lastToken(ctx, exec)
		}
		if zipName != "" && r.cfg.DownloadURLTemplate != "" {
			url = formatDownloadURL(r.cfg.DownloadURLTemplate, zipName)
		}
	}
	if url == "" {
		return "", fmt.Errorf("plan replayer dump did not include a downloadable url: %s", text)
	}

	return r.download(ctx, url, caseDir)
}

func (r *Replayer) buildDumpSQL(sql string) string {
	stmt := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	upper := strings.ToUpper(stmt)
	if strings.HasPrefix(upper, "EXPLAIN ANALYZE ") {
		stmt = strings.TrimSpace(stmt[len("EXPLAIN ANALYZE "):])
	} else if strings.HasPrefix(upper, "EXPLAIN ") {
		stmt = strings.TrimSpace(stmt[len("EXPLAIN "):])
	}
	return fmt.Sprintf("PLAN REPLAYER DUMP EXPLAIN %s", stmt)
}

func (r *Replayer) lastToken(ctx context.Context, exec *db.DB) string {
	row := exec.QueryRowContext(ctx, "SELECT @@tidb_last_plan_replayer_token")
	var token string
	if err := row.Scan(&token); err != nil {
		return ""
	}
	return strings.TrimSpace(token)
}

func (r *Replayer) download(ctx context.Context, url string, caseDir string) (string, error) {
	client := &http.Client{Timeout: time.Duration(r.cfg.TimeoutSeconds) * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("download failed with status %s", resp.Status)
	}

	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		return "", err
	}
	filename := filepath.Join(caseDir, "plan_replayer.zip")
	out, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	defer out.Close()

	limited := io.LimitReader(resp.Body, r.cfg.MaxDownloadBytes)
	if _, err := io.Copy(out, limited); err != nil {
		return "", err
	}
	return filename, nil
}

var urlRE = regexp.MustCompile(`https?://\S+`)
var zipRE = regexp.MustCompile(`([A-Za-z0-9_\-=]+\.zip)`)

func extractURL(text string) string {
	match := urlRE.FindString(text)
	return strings.TrimRight(strings.TrimSpace(match), ".,);")
}

func extractZipName(text string) string {
	match := zipRE.FindString(text)
	return strings.TrimSpace(match)
}

func formatDownloadURL(tmpl, name string) string {
	trimmed := name
	if strings.Contains(tmpl, "%s.zip") && strings.HasSuffix(strings.ToLower(trimmed), ".zip") {
		trimmed = strings.TrimSuffix(trimmed, ".zip")
	}
	return fmt.Sprintf(tmpl, trimmed)
}
