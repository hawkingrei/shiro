package runner

import (
	"context"
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

	summary := report.Summary{
		Oracle:        result.Oracle,
		SQL:           result.SQL,
		Expected:      result.Expected,
		Actual:        result.Actual,
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
	summary.CaseDir = filepath.Base(caseData.Dir)
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
	}
	_ = r.reporter.WriteSummary(caseData, summary)
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

	if r.uploader.Enabled() {
		location, err := r.uploader.UploadDir(ctx, caseData.Dir)
		if err == nil {
			summary.UploadLocation = location
			_ = r.reporter.WriteSummary(caseData, summary)
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
