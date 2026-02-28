package runner

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"shiro/internal/oracle"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type replaySpec struct {
	kind        string
	expectedSQL string
	actualSQL   string
	setVar      string
	tolerance   float64
	maxRows     int
	impoIsUpper bool
}

type minimizeOutput struct {
	caseSQL   []string
	insertSQL []string
	reproSQL  []string
	minimized bool
	status    string
	reason    string
	flaky     bool
}

const minimizeReasonBaseReplayNotReproducible = "base_replay_not_reproducible"
const minimizePassLimit = 3
const minimizeDefaultRounds = 8
const minimizeBaseReplayAttempts = 3
const minimizeBaseReplayRequired = 2

// sqlSliceWeightStmtMultiplier keeps statement count dominant in minimization scoring.
const sqlSliceWeightStmtMultiplier = 100000

func (r *Runner) minimizeCase(ctx context.Context, result oracle.Result, spec replaySpec) minimizeOutput {
	if !r.cfg.Minimize.Enabled {
		return minimizeOutput{status: "disabled"}
	}
	if spec.kind == "" {
		return minimizeOutput{status: "not_applicable"}
	}
	tablesUsed := tablesForMinimize(result)
	schemaSQL := r.schemaSQL(ctx, tablesUsed)
	if len(schemaSQL) == 0 {
		return minimizeOutput{status: "skipped", reason: "schema_unavailable"}
	}

	timeout := time.Duration(r.cfg.Minimize.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	minCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	test := func(inserts []string, caseSQL []string) bool {
		return r.replayCase(minCtx, schemaSQL, inserts, caseSQL, result, spec)
	}

	origInserts := append([]string{}, r.insertLog...)
	if len(tablesUsed) > 0 {
		origInserts = filterInsertsByTables(origInserts, tablesUsed)
	}
	origCase := append([]string{}, result.SQL...)

	origInserts = expandInsertStatements(origInserts)
	baseReproducible, baseFlaky := minimizeBaseReplayGate(func() bool {
		return test(origInserts, origCase)
	}, spec.kind)
	if !baseReproducible {
		return minimizeOutput{
			status: "skipped",
			reason: minimizeReasonBaseReplayNotReproducible,
			flaky:  true,
		}
	}
	dedupedInserts := dedupeStatements(origInserts)
	if sqlSliceWeight(dedupedInserts) < sqlSliceWeight(origInserts) && test(dedupedInserts, origCase) {
		origInserts = dedupedInserts
	}
	if spec.kind == "case_error" {
		dedupedCase := dedupeStatements(origCase)
		if sqlSliceWeight(dedupedCase) < sqlSliceWeight(origCase) && test(origInserts, dedupedCase) {
			origCase = dedupedCase
		}
	}

	minInserts := append([]string{}, origInserts...)
	minCase := append([]string{}, origCase...)
	specReduced := spec
	maxRounds := r.cfg.Minimize.MaxRounds

	switch spec.kind {
	case "case_error":
		minInserts, minCase = reduceCaseErrorCandidate(minCtx, maxRounds, minInserts, minCase, test)
		if r.cfg.Minimize.MergeInserts {
			minInserts = validatedMergedInserts(minInserts, func(stmts []string) bool {
				return test(stmts, minCase)
			})
		}
	default:
		if spec.kind != "" {
			minInserts, specReduced = reduceReplaySpecCandidate(minCtx, maxRounds, minInserts, specReduced, r.cfg.Minimize.MergeInserts, func(stmts []string, current replaySpec) bool {
				return r.replayCase(minCtx, schemaSQL, stmts, minimalCaseSQL(current), result, current)
			})
			minCase = minimalCaseSQL(specReduced)
		}
	}

	reproSQL := buildReproSQL(schemaSQL, minInserts, minCase, specReduced)
	return minimizeOutput{
		caseSQL:   minCase,
		insertSQL: minInserts,
		reproSQL:  reproSQL,
		minimized: true,
		status:    "success",
		flaky:     baseFlaky,
	}
}

func minimizeBaseReplayGate(run func() bool, specKind string) (ok bool, flaky bool) {
	if replayConsensus(run, minimizeBaseReplayAttempts, minimizeBaseReplayRequired) {
		return true, false
	}
	if strings.EqualFold(strings.TrimSpace(specKind), "case_error") &&
		replayConsensus(run, minimizeBaseReplayAttempts, 1) {
		return true, true
	}
	return false, true
}

func reduceCaseErrorCandidate(
	ctx context.Context,
	maxRounds int,
	inserts []string,
	caseSQL []string,
	test func([]string, []string) bool,
) (reducedInserts []string, reducedCase []string) {
	currentInserts := append([]string{}, inserts...)
	currentCase := append([]string{}, caseSQL...)
	if len(currentInserts) > 1 {
		shrunk := shrinkStatements(currentInserts, maxRounds, func(stmts []string) bool {
			return test(stmts, currentCase)
		})
		currentInserts = betterSQLSlice(currentInserts, shrunk)
	}
	if len(currentCase) > 1 {
		shrunk := shrinkStatements(currentCase, maxRounds, func(stmts []string) bool {
			return test(currentInserts, stmts)
		})
		currentCase = betterSQLSlice(currentCase, shrunk)
	}

	for pass := 0; pass < minimizePassLimit; pass++ {
		if ctx.Err() != nil {
			break
		}
		changed := false

		if len(currentInserts) > 1 {
			shrunkInserts := shrinkStatements(currentInserts, maxRounds, func(stmts []string) bool {
				return test(stmts, currentCase)
			})
			better := betterSQLSlice(currentInserts, shrunkInserts)
			if sqlSliceWeight(better) < sqlSliceWeight(currentInserts) {
				currentInserts = better
				changed = true
			}
		}

		if len(currentCase) > 1 {
			shrunkCase := shrinkStatements(currentCase, maxRounds, func(stmts []string) bool {
				return test(currentInserts, stmts)
			})
			better := betterSQLSlice(currentCase, shrunkCase)
			if sqlSliceWeight(better) < sqlSliceWeight(currentCase) {
				currentCase = better
				changed = true
			}
		}

		astReduced := astReduceStatements(ctx, currentCase, maxRounds, func(stmts []string) bool {
			return test(currentInserts, stmts)
		})
		betterCase := betterSQLSlice(currentCase, astReduced)
		if sqlSliceWeight(betterCase) < sqlSliceWeight(currentCase) {
			currentCase = betterCase
			changed = true
		}

		if !changed {
			break
		}
	}
	return currentInserts, currentCase
}

func replayConsensus(run func() bool, attempts int, required int) bool {
	if required <= 0 {
		return true
	}
	if run == nil {
		return false
	}
	if attempts <= 0 {
		return false
	}
	if attempts < required {
		return false
	}
	success := 0
	remaining := attempts
	for i := 0; i < attempts; i++ {
		if run() {
			success++
		}
		remaining--
		if success >= required {
			return true
		}
		if success+remaining < required {
			return false
		}
	}
	return false
}

func reduceReplaySpecCandidate(
	ctx context.Context,
	maxRounds int,
	inserts []string,
	spec replaySpec,
	mergeInserts bool,
	test func([]string, replaySpec) bool,
) ([]string, replaySpec) {
	currentInserts := append([]string{}, inserts...)
	currentSpec := spec

	if len(currentInserts) > 1 {
		shrunk := shrinkStatements(currentInserts, maxRounds, func(stmts []string) bool {
			return test(stmts, currentSpec)
		})
		currentInserts = betterReplayCandidateInserts(currentInserts, shrunk, currentSpec)
	}

	for pass := 0; pass < minimizePassLimit; pass++ {
		if ctx.Err() != nil {
			break
		}
		changed := false

		if len(currentInserts) > 1 {
			shrunkInserts := shrinkStatements(currentInserts, maxRounds, func(stmts []string) bool {
				return test(stmts, currentSpec)
			})
			better := betterReplayCandidateInserts(currentInserts, shrunkInserts, currentSpec)
			if replayCandidateWeight(better, currentSpec) < replayCandidateWeight(currentInserts, currentSpec) {
				currentInserts = better
				changed = true
			}
		}

		nextSpec := reduceReplaySpecSQL(ctx, maxRounds, currentSpec, currentInserts, test)
		if replayCandidateWeight(currentInserts, nextSpec) < replayCandidateWeight(currentInserts, currentSpec) {
			currentSpec = nextSpec
			changed = true
		}

		if currentSpec.setVar != "" {
			droppedSetVar := currentSpec
			droppedSetVar.setVar = ""
			if test(currentInserts, droppedSetVar) &&
				replayCandidateWeight(currentInserts, droppedSetVar) < replayCandidateWeight(currentInserts, currentSpec) {
				currentSpec = droppedSetVar
				changed = true
			}
		}

		if mergeInserts {
			merged := validatedMergedInserts(currentInserts, func(stmts []string) bool {
				return test(stmts, currentSpec)
			})
			if replayCandidateWeight(merged, currentSpec) < replayCandidateWeight(currentInserts, currentSpec) {
				currentInserts = merged
				changed = true
			}
		}

		if !changed {
			break
		}
	}

	return currentInserts, currentSpec
}

func reduceReplaySpecSQL(
	ctx context.Context,
	maxRounds int,
	spec replaySpec,
	inserts []string,
	test func([]string, replaySpec) bool,
) replaySpec {
	reduced := spec
	if reduced.expectedSQL != "" {
		candidate := astReduceSQL(ctx, reduced.expectedSQL, maxRounds, func(sqlText string) bool {
			tmp := reduced
			tmp.expectedSQL = sqlText
			return test(inserts, tmp)
		})
		tmp := reduced
		tmp.expectedSQL = candidate
		if replayCandidateWeight(inserts, tmp) < replayCandidateWeight(inserts, reduced) {
			reduced = tmp
		}
	}
	if reduced.actualSQL != "" {
		candidate := astReduceSQL(ctx, reduced.actualSQL, maxRounds, func(sqlText string) bool {
			tmp := reduced
			tmp.actualSQL = sqlText
			return test(inserts, tmp)
		})
		tmp := reduced
		tmp.actualSQL = candidate
		if replayCandidateWeight(inserts, tmp) < replayCandidateWeight(inserts, reduced) {
			reduced = tmp
		}
	}
	return reduced
}

func betterReplayCandidateInserts(current, candidate []string, spec replaySpec) []string {
	if replayCandidateWeight(candidate, spec) < replayCandidateWeight(current, spec) {
		return candidate
	}
	return current
}

func replayCandidateWeight(inserts []string, spec replaySpec) int {
	return sqlSliceWeight(inserts) + sqlSliceWeight(minimalCaseSQL(spec))
}

func betterSQLSlice(current, candidate []string) []string {
	if sqlSliceWeight(candidate) < sqlSliceWeight(current) {
		return candidate
	}
	return current
}

func validatedMergedInserts(inserts []string, test func([]string) bool) []string {
	merged := mergeInsertStatements(inserts)
	if sqlSliceWeight(merged) >= sqlSliceWeight(inserts) {
		return inserts
	}
	if !test(merged) {
		return inserts
	}
	return merged
}

func sqlSliceWeight(stmts []string) int {
	weight := len(stmts) * sqlSliceWeightStmtMultiplier
	for _, stmt := range stmts {
		weight += len(strings.TrimSpace(stmt))
	}
	return weight
}

func (r *Runner) schemaSQL(ctx context.Context, tables map[string]struct{}) []string {
	var out []string
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	for _, tbl := range r.state.Tables {
		if len(tables) > 0 {
			if _, ok := tables[strings.ToLower(tbl.Name)]; !ok {
				continue
			}
		}
		row := r.exec.QueryRowContext(qctx, fmt.Sprintf("SHOW CREATE TABLE %s", tbl.Name))
		var name, createSQL string
		if err := row.Scan(&name, &createSQL); err != nil {
			continue
		}
		out = append(out, normalizeCreateTable(createSQL))
	}
	return out
}

var autoIncPattern = regexp.MustCompile(`(?i)\sAUTO_INCREMENT=\d+`)

func normalizeCreateTable(sql string) string {
	return autoIncPattern.ReplaceAllString(sql, "")
}

func minimalCaseSQL(spec replaySpec) []string {
	var steps []string
	if spec.expectedSQL != "" {
		steps = append(steps, spec.expectedSQL)
	}
	if spec.setVar != "" {
		steps = append(steps, "SET SESSION "+spec.setVar)
	}
	if spec.actualSQL != "" {
		steps = append(steps, spec.actualSQL)
	}
	return steps
}

func tablesForMinimize(result oracle.Result) map[string]struct{} {
	p := parser.New()
	tables := map[string]struct{}{}
	collectTables(p, tables, result.SQL...)
	if result.Details != nil {
		if sqlText, ok := result.Details["replay_expected_sql"].(string); ok {
			collectTables(p, tables, sqlText)
		}
		if sqlText, ok := result.Details["replay_actual_sql"].(string); ok {
			collectTables(p, tables, sqlText)
		}
	}
	if len(tables) == 0 {
		return nil
	}
	return tables
}

func collectTables(p *parser.Parser, tables map[string]struct{}, sqls ...string) {
	for _, sqlText := range sqls {
		stmt, err := p.ParseOneStmt(sqlText, "", "")
		if err != nil {
			continue
		}
		collector := &tableCollector{tables: tables}
		stmt.Accept(collector)
	}
}

type tableCollector struct {
	tables map[string]struct{}
}

// Enter collects table names from nodes during AST traversal.
func (c *tableCollector) Enter(in ast.Node) (ast.Node, bool) {
	if t, ok := in.(*ast.TableName); ok {
		name := strings.ToLower(t.Name.O)
		if name != "" {
			c.tables[name] = struct{}{}
		}
	}
	return in, false
}

// Leave completes the visitor step.
func (c *tableCollector) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func filterInsertsByTables(stmts []string, tables map[string]struct{}) []string {
	p := parser.New()
	out := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		table := insertTargetTable(p, trimmed)
		if table == "" {
			out = append(out, trimmed)
			continue
		}
		if _, ok := tables[strings.ToLower(table)]; ok {
			out = append(out, trimmed)
		}
	}
	return out
}

func insertTargetTable(p *parser.Parser, stmt string) string {
	node, err := p.ParseOneStmt(stmt, "", "")
	if err != nil {
		return ""
	}
	ins, ok := node.(*ast.InsertStmt)
	if !ok || ins.Table == nil {
		return ""
	}
	collector := &tableCollector{tables: map[string]struct{}{}}
	ins.Table.Accept(collector)
	for name := range collector.tables {
		return name
	}
	return ""
}

func expandInsertStatements(stmts []string) []string {
	out := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		if parts := splitInsertValues(trimmed); len(parts) > 1 {
			out = append(out, parts...)
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func splitInsertValues(stmt string) []string {
	upper := strings.ToUpper(stmt)
	idx := strings.Index(upper, "VALUES")
	if idx == -1 {
		return []string{stmt}
	}
	prefix := strings.TrimSpace(stmt[:idx+len("VALUES")])
	values := strings.TrimSpace(stmt[idx+len("VALUES"):])
	if values == "" {
		return []string{stmt}
	}
	tuples := splitValueTuples(values)
	if len(tuples) <= 1 {
		return []string{stmt}
	}
	out := make([]string, 0, len(tuples))
	for _, tup := range tuples {
		out = append(out, prefix+" "+tup)
	}
	return out
}

func splitValueTuples(values string) []string {
	var out []string
	depth := 0
	start := -1
	inString := false
	escape := false
	for i := 0; i < len(values); i++ {
		ch := values[i]
		if inString {
			if escape {
				escape = false
				continue
			}
			if ch == '\\' {
				escape = true
				continue
			}
			if ch == '\'' {
				inString = false
			}
			continue
		}
		switch ch {
		case '\'':
			inString = true
		case '(':
			if depth == 0 {
				start = i
			}
			depth++
		case ')':
			if depth > 0 {
				depth--
				if depth == 0 && start >= 0 {
					out = append(out, strings.TrimSpace(values[start:i+1]))
					start = -1
				}
			}
		}
	}
	return out
}

func buildReproSQL(schemaSQL, inserts, caseSQL []string, spec replaySpec) []string {
	steps := append([]string{}, schemaSQL...)
	steps = append(steps, inserts...)
	switch spec.kind {
	case "signature", "count", "plan_rows":
		if spec.expectedSQL != "" {
			steps = append(steps, spec.expectedSQL)
		}
		if spec.setVar != "" {
			steps = append(steps, "SET SESSION "+spec.setVar)
		}
		if spec.actualSQL != "" {
			steps = append(steps, spec.actualSQL)
		}
	case "rows_affected":
		if spec.expectedSQL != "" {
			steps = append(steps, spec.expectedSQL)
		}
		if spec.actualSQL != "" {
			steps = append(steps, spec.actualSQL)
		}
	case "error_sql":
		if spec.setVar != "" {
			steps = append(steps, "SET SESSION "+spec.setVar)
		}
		if spec.expectedSQL != "" {
			steps = append(steps, spec.expectedSQL)
		}
	case "case_error":
		steps = append(steps, caseSQL...)
	default:
		steps = append(steps, caseSQL...)
	}
	return steps
}
