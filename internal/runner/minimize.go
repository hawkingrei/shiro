package runner

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"shiro/internal/db"
	"shiro/internal/oracle"
	"shiro/internal/validator"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

type replaySpec struct {
	kind        string
	expectedSQL string
	actualSQL   string
	setVar      string
	tolerance   float64
}

type minimizeOutput struct {
	caseSQL   []string
	insertSQL []string
	reproSQL  []string
	minimized bool
}

func (r *Runner) minimizeCase(ctx context.Context, result oracle.Result) minimizeOutput {
	if !r.cfg.Minimize.Enabled {
		return minimizeOutput{}
	}
	tablesUsed := tablesForMinimize(result)
	schemaSQL := r.schemaSQL(ctx, tablesUsed)
	if len(schemaSQL) == 0 {
		return minimizeOutput{}
	}
	spec := buildReplaySpec(result)
	if spec.kind == "" {
		return minimizeOutput{}
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

func (r *Runner) replayCase(ctx context.Context, schemaSQL, inserts, caseSQL []string, result oracle.Result, spec replaySpec) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	conn, err := r.exec.Conn(ctx)
	if err != nil {
		return false
	}
	defer conn.Close()
	minDB := r.baseDB + "_min"
	if err := r.resetDatabaseOnConn(ctx, conn, minDB); err != nil {
		return false
	}
	if err := r.execOnConn(ctx, conn, "USE "+minDB); err != nil {
		return false
	}
	if err := execStatements(ctx, conn, schemaSQL, r.validator); err != nil {
		return false
	}
	if err := execStatements(ctx, conn, inserts, r.validator); err != nil {
		return false
	}

	switch spec.kind {
	case "signature":
		base, err := querySignatureConn(ctx, conn, spec.expectedSQL, r.validator)
		if err != nil {
			return false
		}
		if spec.setVar != "" {
			if err := r.execOnConn(ctx, conn, "SET SESSION "+spec.setVar); err != nil {
				return false
			}
		}
		other, err := querySignatureConn(ctx, conn, spec.actualSQL, r.validator)
		if spec.setVar != "" {
			resetVarOnConn(conn, ctx, spec.setVar)
		}
		if err != nil {
			return false
		}
		return base != other
	case "count":
		base, err := queryCountConn(ctx, conn, spec.expectedSQL, r.validator)
		if err != nil {
			return false
		}
		other, err := queryCountConn(ctx, conn, spec.actualSQL, r.validator)
		if err != nil {
			return false
		}
		return base != other
	case "plan_rows":
		base, err := queryPlanRowsConn(ctx, conn, spec.expectedSQL, r.validator)
		if err != nil {
			return false
		}
		other, err := queryPlanRowsConn(ctx, conn, spec.actualSQL, r.validator)
		if err != nil {
			return false
		}
		return other > base*(1.0+spec.tolerance)
	case "rows_affected":
		base, err := queryCountConn(ctx, conn, spec.expectedSQL, r.validator)
		if err != nil {
			return false
		}
		affected, err := execRowsAffected(ctx, conn, spec.actualSQL, r.validator)
		if err != nil {
			return false
		}
		return affected != base
	case "case_error":
		err := execStatements(ctx, conn, caseSQL, r.validator)
		return errorMatches(err, result.Err)
	default:
		err := execStatements(ctx, conn, caseSQL, r.validator)
		return errorMatches(err, result.Err)
	}
}

func (r *Runner) resetDatabaseOnConn(ctx context.Context, conn *sql.Conn, name string) error {
	if err := r.execOnConn(ctx, conn, fmt.Sprintf("DROP DATABASE IF EXISTS %s", name)); err != nil {
		return err
	}
	if err := r.execOnConn(ctx, conn, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", name)); err != nil {
		return err
	}
	return nil
}

func buildReplaySpec(result oracle.Result) replaySpec {
	if result.Err != nil {
		return replaySpec{kind: "case_error"}
	}
	if result.Details == nil {
		return replaySpec{}
	}
	kind, _ := result.Details["replay_kind"].(string)
	expected, _ := result.Details["replay_expected_sql"].(string)
	actual, _ := result.Details["replay_actual_sql"].(string)
	setVar, _ := result.Details["replay_set_var"].(string)
	tol, _ := result.Details["replay_tolerance"].(float64)
	if tol == 0 {
		tol = 0.1
	}
	return replaySpec{
		kind:        kind,
		expectedSQL: expected,
		actualSQL:   actual,
		setVar:      setVar,
		tolerance:   tol,
	}
}

func dedupeStatements(stmts []string) []string {
	seen := make(map[string]struct{}, len(stmts))
	out := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func shrinkStatements(stmts []string, maxRounds int, test func([]string) bool) []string {
	if len(stmts) < 2 {
		return stmts
	}
	if maxRounds <= 0 {
		maxRounds = 8
	}
	n := 2
	rounds := 0
	for len(stmts) >= 2 && rounds < maxRounds {
		rounds++
		chunk := len(stmts) / n
		if chunk == 0 {
			break
		}
		removed := false
		for i := 0; i < n; i++ {
			start := i * chunk
			end := start + chunk
			if i == n-1 {
				end = len(stmts)
			}
			candidate := append([]string{}, stmts[:start]...)
			candidate = append(candidate, stmts[end:]...)
			if test(candidate) {
				stmts = candidate
				n = max(n-1, 2)
				removed = true
				break
			}
		}
		if !removed {
			if n >= len(stmts) {
				break
			}
			n = min(n*2, len(stmts))
		}
	}
	return stmts
}

func execStatements(ctx context.Context, conn *sql.Conn, stmts []string, v *validator.Validator) error {
	for _, stmt := range stmts {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if v != nil {
			if err := v.Validate(stmt); err != nil {
				return err
			}
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func querySignatureConn(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator) (db.Signature, error) {
	if v != nil {
		if err := v.Validate(query); err != nil {
			return db.Signature{}, err
		}
	}
	row := conn.QueryRowContext(ctx, query)
	var sig db.Signature
	if err := row.Scan(&sig.Count, &sig.Checksum); err != nil {
		return db.Signature{}, err
	}
	return sig, nil
}

func queryCountConn(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator) (int64, error) {
	if v != nil {
		if err := v.Validate(query); err != nil {
			return 0, err
		}
	}
	row := conn.QueryRowContext(ctx, query)
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func execRowsAffected(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator) (int64, error) {
	if v != nil {
		if err := v.Validate(query); err != nil {
			return 0, err
		}
	}
	res, err := conn.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return affected, nil
}

func queryPlanRowsConn(ctx context.Context, conn *sql.Conn, query string, v *validator.Validator) (float64, error) {
	if v != nil {
		if err := v.Validate(query); err != nil {
			return 0, err
		}
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	if len(cols) == 0 {
		return 0, fmt.Errorf("no columns in explain result")
	}

	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return 0, err
		}
		for i, name := range cols {
			if name == "estRows" || name == "rows" || name == "est_rows" {
				if len(values[i]) == 0 {
					continue
				}
				var v float64
				if _, err := fmt.Sscanf(string(values[i]), "%f", &v); err == nil {
					return v, nil
				}
			}
		}
	}
	return 0, fmt.Errorf("no estRows field")
}

func resetVarOnConn(conn *sql.Conn, ctx context.Context, assignment string) {
	name := strings.SplitN(assignment, "=", 2)[0]
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	_, _ = conn.ExecContext(ctx, "SET SESSION "+name+"=DEFAULT")
}

func errorMatches(err error, expected error) bool {
	if expected == nil {
		return err == nil
	}
	if err == nil {
		return false
	}
	if isPanicError(expected) {
		return isPanicError(err)
	}
	exp := strings.ToLower(expected.Error())
	got := strings.ToLower(err.Error())
	return strings.Contains(got, exp) || strings.Contains(exp, got)
}

func astReduceStatements(ctx context.Context, stmts []string, maxRounds int, test func([]string) bool) []string {
	if len(stmts) == 0 {
		return stmts
	}
	if maxRounds <= 0 {
		maxRounds = 8
	}
	p := parser.New()
	reduced := append([]string{}, stmts...)
	for round := 0; round < maxRounds; round++ {
		if ctx.Err() != nil {
			break
		}
		changed := false
		for i, stmt := range reduced {
			if ctx.Err() != nil {
				break
			}
			candidates := astCandidates(p, stmt)
			for _, cand := range candidates {
				if cand == stmt || cand == "" {
					continue
				}
				next := append([]string{}, reduced...)
				next[i] = cand
				if test(next) {
					reduced = next
					changed = true
					break
				}
			}
		}
		if !changed {
			break
		}
	}
	return reduced
}

func astReduceSQL(ctx context.Context, stmt string, maxRounds int, test func(string) bool) string {
	if strings.TrimSpace(stmt) == "" {
		return stmt
	}
	if maxRounds <= 0 {
		maxRounds = 8
	}
	trimmed := strings.TrimSpace(stmt)
	explain := false
	if strings.HasPrefix(strings.ToUpper(trimmed), "EXPLAIN ") {
		explain = true
		trimmed = strings.TrimSpace(trimmed[len("EXPLAIN "):])
	}
	p := parser.New()
	reduced := trimmed
	for round := 0; round < maxRounds; round++ {
		if ctx.Err() != nil {
			break
		}
		changed := false
		candidates := astCandidates(p, reduced)
		for _, cand := range candidates {
			if cand == reduced || cand == "" {
				continue
			}
			if test(cand) {
				reduced = cand
				changed = true
				break
			}
		}
		if !changed {
			break
		}
	}
	if explain {
		return "EXPLAIN " + reduced
	}
	return reduced
}

func astCandidates(p *parser.Parser, stmt string) []string {
	node, err := p.ParseOneStmt(stmt, "", "")
	if err != nil {
		return nil
	}
	switch n := node.(type) {
	case *ast.SelectStmt:
		return selectCandidates(p, n)
	case *ast.SetOprStmt:
		return setOprCandidates(p, n)
	default:
		return nil
	}
}

func selectCandidates(p *parser.Parser, n *ast.SelectStmt) []string {
	base := restoreSQL(n)
	var candidates []string
	if n.With != nil {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.With = nil
		}))
	}
	if n.OrderBy != nil {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.OrderBy = nil
		}))
		if n.OrderBy != nil && len(n.OrderBy.Items) > 1 {
			for idx := range n.OrderBy.Items {
				i := idx
				candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
					if s.OrderBy == nil || len(s.OrderBy.Items) <= 1 {
						return
					}
					s.OrderBy.Items = append([]*ast.ByItem{}, append(s.OrderBy.Items[:i], s.OrderBy.Items[i+1:]...)...)
				}))
			}
		}
	}
	if n.Limit != nil {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.Limit = nil
		}))
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.Limit = &ast.Limit{
				Count: ast.NewValueExpr(1, "", ""),
			}
		}))
		if n.Limit.Offset != nil {
			candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
				if s.Limit != nil {
					s.Limit.Offset = nil
				}
			}))
		}
	}
	if n.Distinct {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.Distinct = false
		}))
	}
	if n.Having != nil {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.Having = nil
		}))
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			if s.Having != nil {
				s.Having.Expr = simplifyExpr(s.Having.Expr)
			}
		}))
	}
	if n.GroupBy != nil && len(n.GroupBy.Items) > 0 {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.GroupBy = nil
		}))
		if len(n.GroupBy.Items) > 1 {
			for idx := range n.GroupBy.Items {
				i := idx
				candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
					if s.GroupBy == nil || len(s.GroupBy.Items) <= 1 {
						return
					}
					s.GroupBy.Items = append([]*ast.ByItem{}, append(s.GroupBy.Items[:i], s.GroupBy.Items[i+1:]...)...)
				}))
			}
		}
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			if s.GroupBy == nil || len(s.GroupBy.Items) == 0 {
				return
			}
			s.GroupBy.Items = s.GroupBy.Items[:1]
			s.GroupBy.Rollup = false
		}))
	}
	if n.Where != nil {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.Where = nil
		}))
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.Where = ast.NewValueExpr(1, "", "")
		}))
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.Where = ast.NewValueExpr(0, "", "")
		}))
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.Where = simplifyExpr(s.Where)
		}))
	}
	candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
		applyDeepSimplify(s)
	}))
	if len(n.WindowSpecs) > 0 {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.WindowSpecs = nil
		}))
	}
	if len(n.TableHints) > 0 {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.TableHints = nil
		}))
	}
	if n.LockInfo != nil {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.LockInfo = nil
		}))
	}
	if n.Fields != nil && len(n.Fields.Fields) > 1 {
		for idx := range n.Fields.Fields {
			i := idx
			candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
				if s.Fields == nil || len(s.Fields.Fields) <= 1 {
					return
				}
				s.Fields.Fields = append([]*ast.SelectField{}, append(s.Fields.Fields[:i], s.Fields.Fields[i+1:]...)...)
			}))
		}
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			if s.Fields == nil || len(s.Fields.Fields) <= 1 {
				return
			}
			s.Fields.Fields = s.Fields.Fields[:1]
		}))
	}
	if n.Fields != nil && len(n.Fields.Fields) > 0 {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			s.Fields = &ast.FieldList{Fields: []*ast.SelectField{{Expr: ast.NewValueExpr(1, "", "")}}}
		}))
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			if s.Fields == nil {
				return
			}
			for _, f := range s.Fields.Fields {
				if f == nil || f.Expr == nil {
					continue
				}
				f.Expr = ast.NewValueExpr(1, "", "")
			}
		}))
	}
	if n.From != nil && n.From.TableRefs != nil {
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			if s.From != nil {
				simplifyJoinToLeft(s.From)
			}
		}))
		candidates = append(candidates, mutateSelect(p, base, func(s *ast.SelectStmt) {
			if s.From != nil {
				dropJoinConditions(s.From)
			}
		}))
	}
	return uniqueStrings(candidates)
}

func setOprCandidates(p *parser.Parser, n *ast.SetOprStmt) []string {
	base := restoreSQL(n)
	var candidates []string
	if n.With != nil {
		candidates = append(candidates, mutateSetOpr(p, base, func(u *ast.SetOprStmt) {
			u.With = nil
		}))
	}
	if n.OrderBy != nil {
		candidates = append(candidates, mutateSetOpr(p, base, func(u *ast.SetOprStmt) {
			u.OrderBy = nil
		}))
	}
	if n.Limit != nil {
		candidates = append(candidates, mutateSetOpr(p, base, func(u *ast.SetOprStmt) {
			u.Limit = nil
		}))
	}
	if n.SelectList != nil && len(n.SelectList.Selects) > 1 {
		candidates = append(candidates, mutateSetOpr(p, base, func(u *ast.SetOprStmt) {
			if u.SelectList == nil || len(u.SelectList.Selects) == 0 {
				return
			}
			u.SelectList.Selects = u.SelectList.Selects[:1]
		}))
	}
	candidates = append(candidates, mutateSetOpr(p, base, func(u *ast.SetOprStmt) {
		applyDeepSimplifySet(u)
	}))
	return uniqueStrings(candidates)
}

func mutateSelect(p *parser.Parser, sql string, fn func(*ast.SelectStmt)) string {
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return ""
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		return ""
	}
	fn(sel)
	return restoreSQL(sel)
}

func mutateSetOpr(p *parser.Parser, sql string, fn func(*ast.SetOprStmt)) string {
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return ""
	}
	union, ok := node.(*ast.SetOprStmt)
	if !ok {
		return ""
	}
	fn(union)
	return restoreSQL(union)
}

func restoreSQL(node ast.Node) string {
	var b strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &b)
	if err := node.Restore(ctx); err != nil {
		return ""
	}
	return b.String()
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func simplifyExpr(expr ast.ExprNode) ast.ExprNode {
	switch v := expr.(type) {
	case *ast.BinaryOperationExpr:
		switch v.Op {
		case opcode.LogicAnd, opcode.LogicOr:
			if v.L != nil {
				return v.L
			}
			return v.R
		}
		return ast.NewValueExpr(1, "", "")
	case *ast.PatternInExpr:
		if len(v.List) > 1 {
			v.List = v.List[:1]
		}
		return v
	case *ast.BetweenExpr:
		if v.Expr != nil {
			return v.Expr
		}
		return ast.NewValueExpr(1, "", "")
	case *ast.PatternLikeOrIlikeExpr:
		if v.Expr != nil {
			return v.Expr
		}
		return ast.NewValueExpr(1, "", "")
	case *ast.FuncCallExpr:
		return ast.NewValueExpr(1, "", "")
	default:
		return ast.NewValueExpr(1, "", "")
	}
}

func applyDeepSimplify(stmt *ast.SelectStmt) {
	if stmt == nil {
		return
	}
	stmt.OrderBy = nil
	stmt.Limit = nil
	stmt.Distinct = false
	stmt.Having = nil
	stmt.GroupBy = nil
	stmt.WindowSpecs = nil
	stmt.TableHints = nil
	stmt.LockInfo = nil
	if stmt.Where != nil {
		stmt.Where = simplifyExpr(stmt.Where)
	}
	if stmt.Fields != nil {
		stmt.Fields = &ast.FieldList{Fields: []*ast.SelectField{{Expr: ast.NewValueExpr(1, "", "")}}}
	}
	if stmt.From != nil {
		stmt.From.Accept(&subquerySimplifier{})
	}
}

func applyDeepSimplifySet(stmt *ast.SetOprStmt) {
	if stmt == nil {
		return
	}
	stmt.OrderBy = nil
	stmt.Limit = nil
	stmt.With = nil
	if stmt.SelectList != nil && len(stmt.SelectList.Selects) > 0 {
		stmt.SelectList.Selects = stmt.SelectList.Selects[:1]
		for _, sel := range stmt.SelectList.Selects {
			if s, ok := sel.(*ast.SelectStmt); ok {
				applyDeepSimplify(s)
			}
		}
	}
}

type subquerySimplifier struct{}

// Enter tracks the table source path for subquery simplification.
func (s *subquerySimplifier) Enter(in ast.Node) (ast.Node, bool) {
	if sub, ok := in.(*ast.SubqueryExpr); ok {
		if sel, ok := sub.Query.(*ast.SelectStmt); ok {
			applyDeepSimplify(sel)
		}
	}
	if ts, ok := in.(*ast.TableSource); ok {
		switch src := ts.Source.(type) {
		case *ast.SelectStmt:
			applyDeepSimplify(src)
		case *ast.SetOprStmt:
			applyDeepSimplifySet(src)
		}
	}
	return in, false
}

// Leave unwinds subquery simplification state.
func (s *subquerySimplifier) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func mergeInsertStatements(stmts []string) []string {
	grouped := map[string][]string{}
	others := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		prefix, values, ok := splitInsertPrefixValues(trimmed)
		if !ok {
			others = append(others, trimmed)
			continue
		}
		grouped[prefix] = append(grouped[prefix], values)
	}
	out := make([]string, 0, len(others)+len(grouped))
	out = append(out, others...)
	for prefix, vals := range grouped {
		out = append(out, prefix+" "+strings.Join(vals, ", "))
	}
	return out
}

func simplifyJoinToLeft(from *ast.TableRefsClause) {
	if from == nil || from.TableRefs == nil {
		return
	}
	left := from.TableRefs.Left
	if left == nil {
		return
	}
	from.TableRefs = &ast.Join{Left: left}
}

func dropJoinConditions(from *ast.TableRefsClause) {
	if from == nil || from.TableRefs == nil {
		return
	}
	join := from.TableRefs
	join.On = nil
	join.Using = nil
	join.NaturalJoin = false
	join.Tp = ast.CrossJoin
}

func splitInsertPrefixValues(stmt string) (string, string, bool) {
	upper := strings.ToUpper(stmt)
	idx := strings.Index(upper, "VALUES")
	if idx == -1 {
		return "", "", false
	}
	prefix := strings.TrimSpace(stmt[:idx+len("VALUES")])
	values := strings.TrimSpace(stmt[idx+len("VALUES"):])
	if values == "" {
		return "", "", false
	}
	return prefix, values, true
}
