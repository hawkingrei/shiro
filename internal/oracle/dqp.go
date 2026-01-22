package oracle

import (
	"context"
	"fmt"
	"math/rand"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

// DQP implements differential query plan testing.
//
// It runs the same query under different plan choices (hints or SET_VAR hints)
// and compares result signatures (COUNT + checksum). Any mismatch suggests a plan-
// dependent correctness bug in the optimizer or execution engine.
type DQP struct {
}

// Name returns the oracle identifier.
func (o DQP) Name() string { return "DQP" }

// Run generates a join query, executes the base signature, then tries variants:
// - join hints (HASH_JOIN/MERGE_JOIN/INL_*)
// - join order hint
// - SET_VAR hints toggling optimizer paths
// Differences in signature are reported with the hint/variable that triggered it.
//
// Example:
//
//	Base:  SELECT * FROM t1 JOIN t2 ON t1.id = t2.id
//	Hint:  SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t1 JOIN t2 ON t1.id = t2.id
//
// If the signatures differ, the plan choice affected correctness.
func (o DQP) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	query := gen.GenerateSelectQuery()
	if query == nil {
		return Result{OK: true, Oracle: o.Name()}
	}
	if query.Limit != nil || queryHasWindow(query) {
		return Result{OK: true, Oracle: o.Name()}
	}
	if !queryDeterministic(query) {
		return Result{OK: true, Oracle: o.Name()}
	}
	if query.Where != nil {
		policy := predicatePolicyFor(gen)
		if !predicateMatches(query.Where, policy) {
			return Result{OK: true, Oracle: o.Name()}
		}
	}
	hasSubquery := queryHasSubquery(query)
	hasSemi := queryHasSemiJoinSubquery(query)
	hasCorr := queryHasCorrelatedSubquery(query)
	hasAgg := queryHasAggregate(query) || len(query.GroupBy) > 0 || query.Having != nil
	hasIndexCandidate := queryHasIndexCandidate(query, state)
	if len(query.From.Joins) == 0 && !hasAgg && !hasIndexCandidate && !hasSubquery {
		return Result{OK: true, Oracle: o.Name()}
	}

	baseSQL := query.SQLString()
	baseSig, err := exec.QuerySignature(ctx, query.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Err: err}
	}

	hasCTE := len(query.With) > 0
	hasPartition := queryHasPartitionedTable(query, state)
	variants := buildDQPVariants(query, state, hasSemi, hasCorr, hasAgg, hasSubquery, hasCTE, hasPartition)
	for _, variant := range variants {
	variantSig, err := exec.QuerySignature(ctx, variant.signatureSQL)
		if err != nil {
			continue
		}
		if variantSig != baseSig {
			details := map[string]any{
				"hint":                variant.hint,
				"replay_kind":         "signature",
				"replay_expected_sql": query.SignatureSQL(),
				"replay_actual_sql":   variant.signatureSQL,
			}
			return Result{
				OK:       false,
				Oracle:   o.Name(),
				SQL:      []string{baseSQL, variant.sql},
				Expected: fmt.Sprintf("cnt=%d checksum=%d", baseSig.Count, baseSig.Checksum),
				Actual:   fmt.Sprintf("cnt=%d checksum=%d", variantSig.Count, variantSig.Checksum),
				Details:  details,
			}
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}}
}

type dqpVariant struct {
	sql          string
	signatureSQL string
	hint         string
}

const maxLeadingHintVariants = 10
const maxCombinedHintVariants = 12

func buildDQPVariants(query *generator.SelectQuery, state *schema.State, hasSemi bool, hasCorr bool, hasAgg bool, hasSubquery bool, hasCTE bool, hasPartition bool) []dqpVariant {
	tables := []string{query.From.BaseTable}
	for _, join := range query.From.Joins {
		tables = append(tables, join.Table)
	}
	hasJoin := len(query.From.Joins) > 0

	noArgHints := map[string]struct{}{
		HintStraightJoin:    {},
		HintSemiJoinRewrite: {},
		HintNoDecorrelate:   {},
		HintHashAgg:         {},
		HintStreamAgg:       {},
		HintAggToCop:        {},
	}
	baseHints := dqpHintsForQuery(tables, hasJoin, hasSemi, hasCorr, hasAgg, noArgHints)
	variants := make([]dqpVariant, 0, len(baseHints)+1)

	for _, hintSQL := range baseHints {
		variantSQL := injectHint(query, hintSQL)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: hintSQL})
	}

	setVarHints := dqpSetVarHints(len(tables), hasJoin, hasSemi, hasCorr, hasSubquery, hasCTE, hasPartition)
	for _, hintSQL := range setVarHints {
		variantSQL := injectHint(query, hintSQL)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: hintSQL})
	}
	for _, hintSQL := range buildCombinedHints(setVarHints, baseHints, maxCombinedHintVariants) {
		variantSQL := injectHint(query, hintSQL)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: hintSQL})
	}
	if state != nil {
		for _, tbl := range tables {
			table, ok := state.TableByName(tbl)
			if !ok {
				continue
			}
			if !tableHasIndex(table) {
				continue
			}
			hints := []string{
				fmt.Sprintf(HintUseIndexFmt, table.Name),
				fmt.Sprintf(HintUseIndexMergeFmt, table.Name),
			}
			for _, hint := range hints {
				variantSQL := injectHint(query, hint)
				variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
				variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: hint})
			}
		}
	}

	return variants
}

func dqpHintsForQuery(tables []string, hasJoin bool, hasSemi bool, hasCorr bool, hasAgg bool, noArgHints map[string]struct{}) []string {
	var candidates []string
	if hasJoin {
		joinHints := []string{
			HintHashJoin,
			HintNoHashJoin,
			HintMergeJoin,
			HintInlJoin,
			HintInlHashJoin,
			HintInlMergeJoin,
			HintHashJoinBuild,
			HintHashJoinProbe,
			HintLeading,
			HintStraightJoin,
		}
		candidates = append(candidates, buildHintSQL(chooseOne(joinHints), tables, noArgHints))
	}
	if hasAgg {
		aggHints := []string{HintHashAgg, HintStreamAgg, HintAggToCop}
		candidates = append(candidates, buildHintSQL(chooseOne(aggHints), tables, noArgHints))
	}
	if hasSemi {
		candidates = append(candidates, buildHintSQL(HintSemiJoinRewrite, tables, noArgHints))
	}
	if hasCorr {
		candidates = append(candidates, buildHintSQL(HintNoDecorrelate, tables, noArgHints))
	}
	return pickHints(candidates, 2)
}

func dqpSetVarHints(tableCount int, hasJoin bool, hasSemi bool, hasCorr bool, hasSubquery bool, hasCTE bool, hasPartition bool) []string {
	var candidates []string
	if hasJoin {
		candidates = append(candidates, chooseToggleHint(SetVarEnableHashJoinOn, SetVarEnableHashJoinOff))
	}
	if hasSubquery {
		candidates = append(candidates, chooseToggleHint(SetVarEnableNonEvalScalarSubqueryOn, SetVarEnableNonEvalScalarSubqueryOff))
	}
	if hasSemi {
		candidates = append(candidates, chooseToggleHint(SetVarEnableSemiJoinRewriteOn, SetVarEnableSemiJoinRewriteOff))
	}
	if hasCorr {
		candidates = append(candidates, chooseToggleHint(SetVarEnableNoDecorrelateOn, SetVarEnableNoDecorrelateOff))
	}
	if hasCTE {
		candidates = append(candidates, chooseToggleHint(SetVarForceInlineCTEOn, SetVarForceInlineCTEOff))
	}
	candidates = append(candidates, joinReorderThresholdHints(tableCount)...)
	if hasPartition {
		candidates = append(candidates, chooseToggleHint(SetVarPartitionPruneDynamic, SetVarPartitionPruneStatic))
	}
	candidates = append(candidates,
		chooseToggleHint(SetVarFixControl33031On, SetVarFixControl33031Off),
		chooseToggleHint(SetVarFixControl44830On, SetVarFixControl44830Off),
		chooseToggleHint(SetVarFixControl44855On, SetVarFixControl44855Off),
		SetVarFixControl45132Zero,
	)
	if len(candidates) == 0 {
		return nil
	}
	return pickHints(candidates, rand.Intn(3))
}

func chooseToggleHint(on, off string) string {
	if rand.Intn(2) == 0 {
		return on
	}
	return off
}

func joinReorderThresholdHints(tableCount int) []string {
	if tableCount <= 1 {
		return nil
	}
	lower := tableCount - 2
	upper := tableCount + 2
	if lower < 0 {
		lower = 0
	}
	if upper > 15 {
		upper = 15
	}
	if lower > upper {
		return nil
	}
	value := lower
	if upper > lower {
		value = lower + rand.Intn(upper-lower+1)
	}
	return []string{fmt.Sprintf(SetVarJoinReorderThresholdFmt, value)}
}

func queryHasIndexCandidate(query *generator.SelectQuery, state *schema.State) bool {
	if query == nil || state == nil {
		return false
	}
	tables := []string{query.From.BaseTable}
	for _, join := range query.From.Joins {
		tables = append(tables, join.Table)
	}
	for _, tbl := range tables {
		table, ok := state.TableByName(tbl)
		if !ok {
			continue
		}
		if tableHasIndex(table) {
			return true
		}
	}
	return false
}

func queryHasPartitionedTable(query *generator.SelectQuery, state *schema.State) bool {
	if query == nil || state == nil {
		return false
	}
	tables := []string{query.From.BaseTable}
	for _, join := range query.From.Joins {
		tables = append(tables, join.Table)
	}
	for _, tbl := range tables {
		table, ok := state.TableByName(tbl)
		if !ok {
			continue
		}
		if table.Partitioned {
			return true
		}
	}
	return false
}

func tableHasIndex(table schema.Table) bool {
	if table.HasPK {
		return true
	}
	for _, col := range table.Columns {
		if col.HasIndex {
			return true
		}
	}
	return false
}

func buildHintSQL(hint string, tables []string, noArgHints map[string]struct{}) string {
	trimmed := strings.TrimSpace(hint)
	upper := strings.ToUpper(trimmed)
	if strings.Contains(upper, "(") {
		return trimmed
	}
	if _, ok := noArgHints[upper]; ok {
		return upper + "()"
	}
	return fmt.Sprintf("%s(%s)", upper, strings.Join(tables, ", "))
}

func buildLeadingHints(tables []string) []string {
	if len(tables) == 0 {
		return nil
	}
	hints := make([]string, 0, maxLeadingHintVariants)
	seen := make(map[string]struct{}, maxLeadingHintVariants)
	add := func(seq []string) {
		if len(hints) >= maxLeadingHintVariants {
			return
		}
		key := strings.Join(seq, ", ")
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		hints = append(hints, fmt.Sprintf(HintLeadingFmt, key))
	}
	add(tables)
	if len(tables) > 1 {
		reversed := append([]string{}, tables...)
		for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
			reversed[i], reversed[j] = reversed[j], reversed[i]
		}
		add(reversed)
	}
	if len(tables) > 2 {
		rotLeft := append([]string{}, tables[1:]...)
		rotLeft = append(rotLeft, tables[0])
		add(rotLeft)
		rotRight := append([]string{tables[len(tables)-1]}, tables[:len(tables)-1]...)
		add(rotRight)
	}
	if len(tables) > 3 {
		swapped := append([]string{}, tables...)
		swapped[1], swapped[2] = swapped[2], swapped[1]
		add(swapped)
		swapped2 := append([]string{}, tables...)
		swapped2[0], swapped2[1] = swapped2[1], swapped2[0]
		add(swapped2)
		swapped3 := append([]string{}, tables...)
		swapped3[0], swapped3[len(swapped3)-1] = swapped3[len(swapped3)-1], swapped3[0]
		add(swapped3)
		if len(tables) > 4 {
			swapped4 := append([]string{}, tables...)
			swapped4[2], swapped4[3] = swapped4[3], swapped4[2]
			add(swapped4)
		}
	}
	for i := 0; i < 3; i++ {
		if len(hints) >= maxLeadingHintVariants {
			break
		}
		shuffled := append([]string{}, tables...)
		seed := int64(len(tables)*7 + i*13)
		r := rand.New(rand.NewSource(seed))
		r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		add(shuffled)
	}
	return hints
}

func buildCombinedHints(setVarHints []string, baseHints []string, limit int) []string {
	if limit <= 0 || len(setVarHints) == 0 || len(baseHints) == 0 {
		return nil
	}
	parts := make([][]string, 0, limit)
	for i := 0; i < len(setVarHints) && len(parts) < limit; i++ {
		base := baseHints[i%len(baseHints)]
		parts = append(parts, []string{setVarHints[i], base})
	}
	out := make([]string, 0, len(parts))
	for _, combo := range parts {
		out = append(out, strings.Join(combo, ", "))
	}
	return out
}

func pickHints(candidates []string, limit int) []string {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}
	if limit > len(candidates) {
		limit = len(candidates)
	}
	shuffled := append([]string{}, candidates...)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	return shuffled[:limit]
}

func chooseOne(candidates []string) string {
	if len(candidates) == 0 {
		return ""
	}
	return candidates[rand.Intn(len(candidates))]
}

func injectHint(query *generator.SelectQuery, hint string) string {
	base := query.SQLString()
	idx := findTopLevelSelectIndex(base)
	if idx == -1 {
		return base
	}
	return base[:idx+6] + " /*+ " + hint + " */" + base[idx+6:]
}

func findTopLevelSelectIndex(sql string) int {
	depth := 0
	inString := false
	for i := 0; i+6 <= len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			if !inString {
				inString = true
			} else if i+1 < len(sql) && sql[i+1] == '\'' {
				i++
			} else {
				inString = false
			}
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}
		if !isSelectToken(sql[i:]) {
			continue
		}
		if i > 0 && isIdentChar(sql[i-1]) {
			continue
		}
		return i
	}
	return -1
}

func isSelectToken(s string) bool {
	if len(s) < 6 {
		return false
	}
	if !strings.EqualFold(s[:6], "SELECT") {
		return false
	}
	if len(s) == 6 {
		return true
	}
	return s[6] == ' ' || s[6] == '\t' || s[6] == '\n' || s[6] == '\r'
}

func isIdentChar(b byte) bool {
	return b == '_' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func signatureSelectList(query *generator.SelectQuery) string {
	aliases := query.ColumnAliases()
	cols := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		cols = append(cols, fmt.Sprintf("q.%s", alias))
	}
	if len(cols) == 0 {
		return "0"
	}
	return strings.Join(cols, ", ")
}

func queryHasSemiJoinSubquery(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	for _, item := range query.Items {
		if exprHasSemiJoinSubquery(item.Expr) {
			return true
		}
	}
	if query.Where != nil && exprHasSemiJoinSubquery(query.Where) {
		return true
	}
	if query.Having != nil && exprHasSemiJoinSubquery(query.Having) {
		return true
	}
	for _, expr := range query.GroupBy {
		if exprHasSemiJoinSubquery(expr) {
			return true
		}
	}
	for _, ob := range query.OrderBy {
		if exprHasSemiJoinSubquery(ob.Expr) {
			return true
		}
	}
	return false
}

func exprHasSemiJoinSubquery(expr generator.Expr) bool {
	switch e := expr.(type) {
	case generator.ExistsExpr:
		return true
	case generator.InExpr:
		if exprHasSemiJoinSubquery(e.Left) {
			return true
		}
		for _, item := range e.List {
			if _, ok := item.(generator.SubqueryExpr); ok {
				return true
			}
			if exprHasSemiJoinSubquery(item) {
				return true
			}
		}
		return false
	case generator.SubqueryExpr:
		return false
	case generator.UnaryExpr:
		return exprHasSemiJoinSubquery(e.Expr)
	case generator.BinaryExpr:
		return exprHasSemiJoinSubquery(e.Left) || exprHasSemiJoinSubquery(e.Right)
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if exprHasSemiJoinSubquery(w.When) || exprHasSemiJoinSubquery(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return exprHasSemiJoinSubquery(e.Else)
		}
		return false
	case generator.FuncExpr:
		for _, arg := range e.Args {
			if exprHasSemiJoinSubquery(arg) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func queryHasCorrelatedSubquery(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	outerTables := make(map[string]struct{})
	outerTables[query.From.BaseTable] = struct{}{}
	for _, join := range query.From.Joins {
		outerTables[join.Table] = struct{}{}
	}
	for _, item := range query.Items {
		if exprHasCorrelatedSubquery(item.Expr, outerTables) {
			return true
		}
	}
	if query.Where != nil && exprHasCorrelatedSubquery(query.Where, outerTables) {
		return true
	}
	if query.Having != nil && exprHasCorrelatedSubquery(query.Having, outerTables) {
		return true
	}
	for _, expr := range query.GroupBy {
		if exprHasCorrelatedSubquery(expr, outerTables) {
			return true
		}
	}
	for _, ob := range query.OrderBy {
		if exprHasCorrelatedSubquery(ob.Expr, outerTables) {
			return true
		}
	}
	return false
}

func exprHasCorrelatedSubquery(expr generator.Expr, outerTables map[string]struct{}) bool {
	switch e := expr.(type) {
	case generator.SubqueryExpr:
		return subqueryIsCorrelated(e.Query, outerTables)
	case generator.ExistsExpr:
		return subqueryIsCorrelated(e.Query, outerTables)
	case generator.InExpr:
		if exprHasCorrelatedSubquery(e.Left, outerTables) {
			return true
		}
		for _, item := range e.List {
			if exprHasCorrelatedSubquery(item, outerTables) {
				return true
			}
		}
		return false
	case generator.UnaryExpr:
		return exprHasCorrelatedSubquery(e.Expr, outerTables)
	case generator.BinaryExpr:
		return exprHasCorrelatedSubquery(e.Left, outerTables) || exprHasCorrelatedSubquery(e.Right, outerTables)
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if exprHasCorrelatedSubquery(w.When, outerTables) || exprHasCorrelatedSubquery(w.Then, outerTables) {
				return true
			}
		}
		if e.Else != nil {
			return exprHasCorrelatedSubquery(e.Else, outerTables)
		}
		return false
	case generator.FuncExpr:
		for _, arg := range e.Args {
			if exprHasCorrelatedSubquery(arg, outerTables) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func subqueryIsCorrelated(query *generator.SelectQuery, outerTables map[string]struct{}) bool {
	if query == nil {
		return false
	}
	innerTables := make(map[string]struct{})
	if query.From.BaseTable != "" {
		innerTables[query.From.BaseTable] = struct{}{}
	}
	for _, join := range query.From.Joins {
		innerTables[join.Table] = struct{}{}
	}
	for _, cte := range query.With {
		innerTables[cte.Name] = struct{}{}
	}
	for _, item := range query.Items {
		if refsUseOuterTable(item.Expr, innerTables, outerTables) {
			return true
		}
	}
	if query.Where != nil && refsUseOuterTable(query.Where, innerTables, outerTables) {
		return true
	}
	if query.Having != nil && refsUseOuterTable(query.Having, innerTables, outerTables) {
		return true
	}
	for _, expr := range query.GroupBy {
		if refsUseOuterTable(expr, innerTables, outerTables) {
			return true
		}
	}
	for _, ob := range query.OrderBy {
		if refsUseOuterTable(ob.Expr, innerTables, outerTables) {
			return true
		}
	}
	for _, join := range query.From.Joins {
		if join.On != nil && refsUseOuterTable(join.On, innerTables, outerTables) {
			return true
		}
	}
	return false
}

func refsUseOuterTable(expr generator.Expr, innerTables map[string]struct{}, outerTables map[string]struct{}) bool {
	for _, ref := range expr.Columns() {
		if ref.Table == "" {
			continue
		}
		if _, ok := innerTables[ref.Table]; ok {
			continue
		}
		if _, ok := outerTables[ref.Table]; ok {
			return true
		}
	}
	return false
}
