package runner

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"shiro/internal/oracle"
	"shiro/internal/report"
	"shiro/internal/util"
)

func (r *Runner) maybeObservePlan(ctx context.Context, result oracle.Result) {
	if !r.cfg.QPG.Enabled || result.Err != nil || r.qpgState == nil {
		return
	}
	target := pickExplainTarget(result.SQL)
	if target == "" {
		return
	}
	r.observePlan(ctx, target)
}

func pickExplainTarget(sqls []string) string {
	if len(sqls) == 0 {
		return ""
	}
	for i := len(sqls) - 1; i >= 0; i-- {
		trimmed := strings.TrimSpace(sqls[i])
		if trimmed == "" {
			continue
		}
		upper := strings.ToUpper(trimmed)
		if strings.HasPrefix(upper, "EXPLAIN") || strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "WITH") {
			return trimmed
		}
	}
	return ""
}

func pickReplaySQL(result oracle.Result) string {
	if result.Details != nil {
		if v, ok := result.Details["replay_sql"]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return s
			}
		}
	}
	for _, sqlText := range result.SQL {
		trimmed := strings.TrimSpace(sqlText)
		if trimmed == "" {
			continue
		}
		upper := strings.ToUpper(trimmed)
		switch {
		case strings.HasPrefix(upper, "SELECT"),
			strings.HasPrefix(upper, "WITH"),
			strings.HasPrefix(upper, "INSERT"),
			strings.HasPrefix(upper, "UPDATE"),
			strings.HasPrefix(upper, "DELETE"):
			return trimmed
		}
	}
	return ""
}

func (r *Runner) tidbVersion(ctx context.Context) string {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	row := r.exec.QueryRowContext(qctx, "SELECT tidb_version()")
	var version string
	if err := row.Scan(&version); err != nil {
		return ""
	}
	return version
}

func (r *Runner) handleResult(ctx context.Context, result oracle.Result) {
	caseData, err := r.reporter.NewCase()
	if err != nil {
		return
	}
	planPath := ""
	planSignature := ""
	planSigFormat := ""
	replaySQL := pickReplaySQL(result)
	if replaySQL != "" {
		var planErr error
		planPath, planErr = r.replayer.DumpAndDownload(ctx, r.exec, replaySQL, caseData.Dir, r.cfg.Database)
		if planErr != nil {
			util.Warnf("plan replayer dump failed dir=%s err=%v", caseData.Dir, planErr)
		}
		if r.cfg.QPG.Enabled && r.qpgState != nil {
			planSignature, planSigFormat = r.explainSignature(ctx, replaySQL)
		}
	}

	details := result.Details
	if details == nil {
		details = map[string]any{}
	}
	flaky := isFlakyExplain(details)
	if result.Err != nil && isUnknownColumnWhereErr(result.Err) {
		if _, ok := details["error_reason"]; !ok {
			details["error_reason"] = "unknown_column"
		}
	}
	if result.Err != nil && isRuntimeError(result.Err) {
		if _, ok := details["error_reason"]; !ok {
			details["error_reason"] = "runtime_error"
		}
	}
	errorReason := ""
	if reason, ok := details["error_reason"].(string); ok {
		errorReason = reason
	}
	bugHint := ""
	if hint, ok := details["bug_hint"].(string); ok {
		bugHint = hint
	}

	summary := report.Summary{
		Oracle:        result.Oracle,
		SQL:           result.SQL,
		Expected:      result.Expected,
		Actual:        result.Actual,
		ErrorReason:   errorReason,
		BugHint:       bugHint,
		ReplaySQL:     replaySQL,
		Flaky:         flaky,
		Details:       details,
		Seed:          r.gen.Seed,
		Timestamp:     time.Now().Format(time.RFC3339),
		PlanReplay:    planPath,
		TiDBVersion:   r.tidbVersion(ctx),
		PlanSignature: planSignature,
		PlanSigFormat: planSigFormat,
	}
	if result.Truth != nil && result.Truth.Enabled {
		summary.GroundTruth = &report.TruthSummary{
			Mismatch: result.Truth.Mismatch,
			JoinSig:  result.Truth.JoinSig,
			RowCount: result.Truth.RowCount,
		}
	}
	summary.CaseID = caseData.ID
	summary.CaseDir = filepath.Base(caseData.Dir)
	if r.cfg.Storage.S3.Enabled {
		summary.CaseDir = caseData.ID
		summary.ArchiveName = report.CaseArchiveName
		summary.ArchiveCodec = report.CaseArchiveCodec
	}
	if result.Oracle == "NoREC" && result.Details != nil {
		if optimized, ok := result.Details["norec_optimized_sql"].(string); ok {
			summary.NoRECOptimizedSQL = optimized
		}
		if unoptimized, ok := result.Details["norec_unoptimized_sql"].(string); ok {
			summary.NoRECUnoptimizedSQL = unoptimized
		}
		if predicate, ok := result.Details["norec_predicate"].(string); ok {
			summary.NoRECPredicate = predicate
		}
	}
	if result.Oracle == "Impo" && result.Details != nil {
		if seedSQL, ok := result.Details["impo_seed_sql"].(string); ok && strings.TrimSpace(seedSQL) != "" {
			_ = r.reporter.WriteSQL(caseData, "impo_seed.sql", []string{seedSQL})
		}
		if initSQL, ok := result.Details["impo_init_sql"].(string); ok && strings.TrimSpace(initSQL) != "" {
			_ = r.reporter.WriteSQL(caseData, "impo_init.sql", []string{initSQL})
		}
		if mutatedSQL, ok := result.Details["impo_mutated_sql"].(string); ok && strings.TrimSpace(mutatedSQL) != "" {
			_ = r.reporter.WriteSQL(caseData, "impo_mutated.sql", []string{mutatedSQL})
		}
	}
	if result.Err != nil {
		summary.Error = result.Err.Error()
		if summary.ErrorSQL == "" {
			if replaySQL != "" {
				summary.ErrorSQL = replaySQL
			} else if len(result.SQL) > 0 {
				summary.ErrorSQL = result.SQL[0]
			}
		}
	}
	if shouldReportRows(result) {
		maxRows := r.cfg.MaxRowsPerTable
		if maxRows <= 0 {
			maxRows = 50
		}
		expectedRows, expectedTrunc, expectedErr := r.queryResultRows(ctx, result.SQL[0], maxRows)
		actualRows, actualTrunc, actualErr := r.queryResultRows(ctx, result.SQL[1], maxRows)
		if expectedErr == nil && expectedRows != "" {
			details["signature_expected"] = result.Expected
			summary.Expected = expectedRows
			details["expected_rows_truncated"] = expectedTrunc
			_ = r.reporter.WriteText(caseData, "expected.tsv", expectedRows)
		}
		if actualErr == nil && actualRows != "" {
			details["signature_actual"] = result.Actual
			summary.Actual = actualRows
			details["actual_rows_truncated"] = actualTrunc
			_ = r.reporter.WriteText(caseData, "actual.tsv", actualRows)
		}
	}
	_ = r.reporter.WriteSQL(caseData, "case.sql", result.SQL)
	_ = r.reporter.WriteSQL(caseData, "inserts.sql", r.insertLog)
	_ = r.reporter.DumpSchema(ctx, caseData, r.exec, r.state)
	_ = r.reporter.DumpData(ctx, caseData, r.exec, r.state)
	if r.cfg.Minimize.Enabled {
		minimized := r.minimizeCase(ctx, result)
		if minimized.minimized {
			if len(minimized.caseSQL) > 0 {
				_ = r.reporter.WriteSQL(caseData, "min/case.sql", minimized.caseSQL)
			}
			if len(minimized.insertSQL) > 0 {
				_ = r.reporter.WriteSQL(caseData, "min/inserts.sql", minimized.insertSQL)
			}
			if len(minimized.reproSQL) > 0 {
				_ = r.reporter.WriteSQL(caseData, "min/repro.sql", minimized.reproSQL)
			}
		}
	}

	_ = r.reporter.WriteSummary(caseData, summary)
	if r.cfg.Storage.S3.Enabled {
		_ = r.reporter.WriteReport(caseData, summary)
		if _, _, archiveErr := r.reporter.WriteCaseArchive(caseData); archiveErr != nil {
			util.Warnf("case archive failed dir=%s err=%v", caseData.Dir, archiveErr)
			summary.ArchiveName = ""
			summary.ArchiveCodec = ""
			_ = r.reporter.WriteSummary(caseData, summary)
			_ = r.reporter.WriteReport(caseData, summary)
		}
	}

	if r.uploader.Enabled() {
		location, err := r.uploader.UploadDir(ctx, caseData.Dir)
		if err == nil {
			summary.UploadLocation = location
			_ = r.reporter.WriteSummary(caseData, summary)
			if r.cfg.Storage.S3.Enabled {
				_ = r.reporter.WriteReport(caseData, summary)
			}
		}
	}

	if result.Err != nil {
		util.Errorf("case captured oracle=%s dir=%s err=%v", result.Oracle, caseData.Dir, result.Err)
	} else if result.Expected != "" || result.Actual != "" {
		if flaky {
			util.Warnf("case captured oracle=%s dir=%s expected=%s actual=%s flaky=true", result.Oracle, caseData.Dir, result.Expected, result.Actual)
		} else {
			util.Warnf("case captured oracle=%s dir=%s expected=%s actual=%s", result.Oracle, caseData.Dir, result.Expected, result.Actual)
		}
	} else {
		util.Warnf("case captured oracle=%s dir=%s", result.Oracle, caseData.Dir)
	}
	if err := r.rotateDatabaseWithRetry(ctx); err != nil {
		util.Errorf("rotate database after bug failed: %v", err)
	}
}

func shouldReportRows(result oracle.Result) bool {
	if strings.TrimSpace(result.Expected) == "" && strings.TrimSpace(result.Actual) == "" {
		return false
	}
	if result.Details == nil {
		return false
	}
	kind, ok := result.Details["replay_kind"].(string)
	if !ok || kind != "signature" {
		return false
	}
	if len(result.SQL) < 2 {
		return false
	}
	return true
}

func (r *Runner) queryResultRows(ctx context.Context, sqlText string, maxRows int) (string, bool, error) {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return "", false, nil
	}
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") {
		return "", false, nil
	}
	trimmed = strings.TrimSuffix(trimmed, ";")
	// NOTE: sqlText is produced by internal query generation and signature replay,
	// not user input. We also restrict to SELECT/WITH and strip trailing semicolons,
	// so wrapping it as a subquery is safe in this context.
	query := fmt.Sprintf("SELECT * FROM (%s) q LIMIT %d", trimmed, maxRows+1)
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	rows, err := r.exec.QueryContext(qctx, query)
	if err != nil {
		return "", false, err
	}
	defer util.CloseWithErr(rows, "report rows")
	cols, err := rows.Columns()
	if err != nil {
		return "", false, err
	}
	values := make([][]byte, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var b strings.Builder
	b.WriteString(strings.Join(cols, "\t"))
	b.WriteString("\n")
	rowCount := 0
	truncated := false
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return "", false, err
		}
		if rowCount >= maxRows {
			truncated = true
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
		rowCount++
	}
	if truncated {
		_ = drainRows(rows)
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	return b.String(), truncated, nil
}

func isFlakyExplain(details map[string]any) bool {
	if details == nil {
		return false
	}
	expected, ok := details["expected_explain"].(string)
	if !ok || strings.TrimSpace(expected) == "" {
		return false
	}
	actual, ok := details["actual_explain"].(string)
	if !ok || strings.TrimSpace(actual) == "" {
		return false
	}
	return normalizeExplain(expected) == normalizeExplain(actual)
}

func normalizeExplain(text string) string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return strings.Join(out, "\n")
}
