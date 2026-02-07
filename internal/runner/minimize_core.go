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
	if !test(origInserts, origCase) {
		return minimizeOutput{
			status: "skipped",
			reason: minimizeReasonBaseReplayNotReproducible,
			flaky:  true,
		}
	}
	dedupedInserts := dedupeStatements(origInserts)
	if len(dedupedInserts) > 0 && test(dedupedInserts, origCase) {
		origInserts = dedupedInserts
	}
	dedupedCase := dedupeStatements(origCase)
	if len(dedupedCase) > 0 && test(origInserts, dedupedCase) {
		origCase = dedupedCase
	}

	minInserts := shrinkStatements(origInserts, r.cfg.Minimize.MaxRounds, func(stmts []string) bool {
		return test(stmts, origCase)
	})
	if r.cfg.Minimize.MergeInserts {
		minInserts = mergeInsertStatements(minInserts)
	}

	specReduced := spec
	if spec.kind != "" && spec.kind != "case_error" {
		testSpec := func(s replaySpec) bool {
			return r.replayCase(minCtx, schemaSQL, minInserts, nil, result, s)
		}
		if specReduced.expectedSQL != "" {
			specReduced.expectedSQL = astReduceSQL(minCtx, specReduced.expectedSQL, r.cfg.Minimize.MaxRounds, func(candidate string) bool {
				tmp := specReduced
				tmp.expectedSQL = candidate
				return testSpec(tmp)
			})
		}
		if specReduced.actualSQL != "" {
			specReduced.actualSQL = astReduceSQL(minCtx, specReduced.actualSQL, r.cfg.Minimize.MaxRounds, func(candidate string) bool {
				tmp := specReduced
				tmp.actualSQL = candidate
				return testSpec(tmp)
			})
		}
	}

	minCase := origCase
	if spec.kind == "case_error" {
		minCase = shrinkStatements(origCase, r.cfg.Minimize.MaxRounds, func(stmts []string) bool {
			return test(minInserts, stmts)
		})
		minCase = astReduceStatements(minCtx, minCase, r.cfg.Minimize.MaxRounds, func(stmts []string) bool {
			return test(minInserts, stmts)
		})
	} else if spec.kind != "" {
		minCase = minimalCaseSQL(specReduced)
	}

	reproSQL := buildReproSQL(schemaSQL, minInserts, minCase, specReduced)
	return minimizeOutput{
		caseSQL:   minCase,
		insertSQL: minInserts,
		reproSQL:  reproSQL,
		minimized: true,
		status:    "success",
	}
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
	case "case_error":
		steps = append(steps, caseSQL...)
	default:
		steps = append(steps, caseSQL...)
	}
	return steps
}
