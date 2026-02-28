package oracle

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
	"shiro/internal/util"
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

const dqpBuildMaxTries = 10
const dqpComplexityJoinCountThreshold = 4
const dqpComplexityJoinTableThreshold = 4
const dqpComplexitySetOpsThresholdDefault = 2
const dqpComplexityDerivedThresholdDefault = 4
const dqpComplexityFalseJoinThreshold = 3
const dqpBaseHintPickLimitDefault = 4
const dqpSetVarHintPickMaxDefault = 4
const dqpWarningLogMaxItems = 5
const dqpMinHintGroupCount = 2

const (
	dqpVariantGroupBaseHint = "base_hint"
	dqpVariantGroupSetVar   = "set_var_hint"
	dqpVariantGroupMPP      = "mpp_hint"
	dqpVariantGroupCombined = "combined_hint"
	dqpVariantGroupIndex    = "index_hint"
)

const (
	dqpComplexityConstraintJoinTables     = "constraint:dqp_complexity_join_tables"
	dqpComplexityConstraintSetOpsDerived  = "constraint:dqp_complexity_set_ops_derived"
	dqpComplexityConstraintFalseJoinChain = "constraint:dqp_complexity_false_join_chain"
	dqpComplexityConstraintFalseJoinRatio = "constraint:dqp_complexity_false_join_ratio"
)

var (
	replaySetVarNamePattern      = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	replaySetVarUnquotedPattern  = regexp.MustCompile(`^[a-zA-Z0-9_.+-]+$`)
	replaySetVarSingleQuotedExpr = regexp.MustCompile(`^'[a-zA-Z0-9_.+-]+'$`)
)

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
	policy := predicatePolicyFor(gen)
	policy.allowNot = true
	policy.allowIsNull = true
	setOpsThreshold := dqpComplexitySetOpsThreshold(gen)
	derivedThreshold := dqpComplexityDerivedThreshold(gen)
	spec := QuerySpec{
		Oracle:          "dqp",
		Profile:         ProfileByName("DQP"),
		PredicatePolicy: policy,
		PredicateGuard:  true,
		MaxTries:        dqpBuildMaxTries,
		Constraints: generator.SelectQueryConstraints{
			RequireDeterministic: true,
			PredicateMode:        generator.PredicateModeSimpleColumns,
			DisallowLimit:        true,
			DisallowWindow:       true,
			DisallowSetOps:       true,
			MaxJoinCount:         3,
			MaxJoinCountSet:      true,
			QueryGuardReason:     dqpComplexityQueryGuardReason(setOpsThreshold, derivedThreshold),
		},
		SkipReasonOverrides: map[string]string{
			"constraint:limit":                    "dqp:limit",
			"constraint:window":                   "dqp:window",
			"constraint:set_ops":                  "dqp:set_ops",
			"constraint:nondeterministic":         "dqp:nondeterministic",
			"constraint:predicate_guard":          "dqp:predicate_guard",
			dqpComplexityConstraintJoinTables:     "dqp:complexity_guard",
			dqpComplexityConstraintSetOpsDerived:  "dqp:complexity_guard",
			dqpComplexityConstraintFalseJoinChain: "dqp:complexity_guard",
			dqpComplexityConstraintFalseJoinRatio: "dqp:complexity_guard",
		},
	}
	query, details := buildQueryWithSpec(gen, spec)
	if query == nil {
		return Result{OK: true, Oracle: o.Name(), Details: details}
	}
	if gen != nil && !gen.ValidateQueryScope(query) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "dqp:scope_invalid"}}
	}
	if cteHasUnstableLimit(query) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "dqp:cte_limit"}}
	}
	hasSubquery := queryHasSubquery(query)
	hasSemi := queryHasSemiJoinSubquery(query)
	hasCorr := queryHasCorrelatedSubquery(query)
	hasAgg := queryHasAggregate(query) || len(query.GroupBy) > 0 || query.Having != nil
	hasIndexCandidate := queryHasIndexCandidate(query, state)
	if len(query.From.Joins) == 0 && !hasAgg && !hasIndexCandidate && !hasSubquery {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "dqp:insufficient_features"}}
	}

	baseSQL := query.SQLString()
	baseSignatureSQL := query.SignatureSQL()
	baseSig, baseWarnings, err := exec.QuerySignatureWithWarnings(ctx, baseSignatureSQL)
	if err != nil {
		reason, code := sqlErrorReason("dqp", err)
		details := map[string]any{
			"error_reason":        reason,
			"replay_kind":         "error_sql",
			"replay_expected_sql": baseSignatureSQL,
		}
		if code != 0 {
			details["error_code"] = int(code)
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Err: err, Details: details}
	}
	dqpLogWarnings("base", "", baseSQL, baseWarnings)

	hasCTE := len(query.With) > 0
	hasPartition := queryHasPartitionedTable(query, state)
	variants, variantMetrics := buildDQPVariants(query, state, hasSemi, hasCorr, hasAgg, hasSubquery, hasCTE, hasPartition, gen)
	executedHintGroups := make(map[string]struct{}, 5)
	for _, variant := range variants {
		variantSig, warnings, err := exec.QuerySignatureWithWarnings(ctx, variant.signatureSQL)
		if err != nil {
			continue
		}
		dqpLogWarnings("variant", variant.hint, variant.sql, warnings)
		mismatch := variantSig != baseSig
		updateHintBandit(variant.hint, dqpHintReward(mismatch), gen.Config.Adaptive.WindowSize, gen.Config.Adaptive.UCBExploration)
		if mismatch {
			expectedExplain, expectedExplainErr := explainSQL(ctx, exec, query.SignatureSQL())
			actualExplain, actualExplainErr := explainSQL(ctx, exec, variant.signatureSQL)
			details := map[string]any{
				"hint":                 variant.hint,
				"replay_kind":          "signature",
				"replay_expected_sql":  query.SignatureSQL(),
				"replay_actual_sql":    variant.signatureSQL,
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
			}
			if setVarAssignment, ok := dqpReplaySetVarAssignment(variant.hint); ok {
				details["replay_set_var"] = setVarAssignment
			}
			return Result{
				OK:       false,
				Oracle:   o.Name(),
				SQL:      []string{baseSQL, variant.sql},
				Expected: fmt.Sprintf("cnt=%d checksum=%d", baseSig.Count, baseSig.Checksum),
				Actual:   fmt.Sprintf("cnt=%d checksum=%d", variantSig.Count, variantSig.Checksum),
				Details:  details,
				Metrics:  variantMetrics.resultMetrics(),
			}
		}
		if strings.TrimSpace(variant.group) != "" {
			executedHintGroups[variant.group] = struct{}{}
		}
	}
	if len(executedHintGroups) < dqpMinHintGroupCount {
		return Result{
			OK:     true,
			Oracle: o.Name(),
			SQL:    []string{baseSQL},
			Details: map[string]any{
				"skip_reason":      "dqp:insufficient_hint_groups",
				"hint_groups":      dqpFormatHintGroups(executedHintGroups),
				"hint_group_count": len(executedHintGroups),
			},
			Metrics: variantMetrics.resultMetrics(),
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Metrics: variantMetrics.resultMetrics()}
}

type dqpComplexityStats struct {
	joinCount        int
	setOps           int
	derivedTables    int
	alwaysFalseJoins int
}

func dqpComplexitySetOpsThreshold(gen *generator.Generator) int {
	if gen == nil || gen.Config.Oracles.DQPComplexitySetOpsThreshold <= 0 {
		return dqpComplexitySetOpsThresholdDefault
	}
	return gen.Config.Oracles.DQPComplexitySetOpsThreshold
}

func dqpComplexityDerivedThreshold(gen *generator.Generator) int {
	if gen == nil || gen.Config.Oracles.DQPComplexityDerivedThreshold <= 0 {
		return dqpComplexityDerivedThresholdDefault
	}
	return gen.Config.Oracles.DQPComplexityDerivedThreshold
}

func dqpComplexityQueryGuardReason(setOpsThreshold int, derivedThreshold int) func(*generator.SelectQuery) (bool, string) {
	return func(query *generator.SelectQuery) (bool, string) {
		reason := dqpComplexityGuardReason(query, setOpsThreshold, derivedThreshold)
		return reason == "", reason
	}
}

func dqpComplexityGuardReason(query *generator.SelectQuery, setOpsThreshold int, derivedThreshold int) string {
	stats := dqpCollectComplexityStats(query)
	if stats.setOps >= setOpsThreshold && stats.derivedTables >= derivedThreshold {
		return dqpComplexityConstraintSetOpsDerived
	}
	if dqpJoinTableCountWithCTE(query) > dqpComplexityJoinTableThreshold {
		return dqpComplexityConstraintJoinTables
	}
	if stats.alwaysFalseJoins >= dqpComplexityFalseJoinThreshold {
		return dqpComplexityConstraintFalseJoinChain
	}
	if stats.joinCount >= dqpComplexityJoinCountThreshold && stats.alwaysFalseJoins*2 >= stats.joinCount {
		return dqpComplexityConstraintFalseJoinRatio
	}
	return ""
}

func dqpCollectComplexityStats(query *generator.SelectQuery) dqpComplexityStats {
	var stats dqpComplexityStats
	var walk func(*generator.SelectQuery)
	walk = func(q *generator.SelectQuery) {
		if q == nil {
			return
		}
		stats.joinCount += len(q.From.Joins)
		stats.setOps += len(q.SetOps)
		if q.From.BaseQuery != nil {
			stats.derivedTables++
			walk(q.From.BaseQuery)
		}
		for _, join := range q.From.Joins {
			if join.TableQuery != nil {
				stats.derivedTables++
				walk(join.TableQuery)
			}
			if dqpExprAlwaysFalse(join.On) {
				stats.alwaysFalseJoins++
			}
		}
		for _, cte := range q.With {
			walk(cte.Query)
		}
		for _, op := range q.SetOps {
			walk(op.Query)
		}
	}
	walk(query)
	return stats
}

func dqpJoinTableCountWithCTE(query *generator.SelectQuery) int {
	return queryTableFactorCountWithCTE(query)
}

func dqpExprAlwaysFalse(expr generator.Expr) bool {
	v, ok := dqpExprConstBool(expr)
	return ok && !v
}

func dqpExprConstBool(expr generator.Expr) (value bool, ok bool) {
	switch e := expr.(type) {
	case nil:
		return false, false
	case generator.LiteralExpr:
		return dqpLiteralAsBool(e.Value)
	case *generator.LiteralExpr:
		if e == nil {
			return false, false
		}
		return dqpLiteralAsBool(e.Value)
	case generator.UnaryExpr:
		if strings.EqualFold(strings.TrimSpace(e.Op), "NOT") {
			if v, ok := dqpExprConstBool(e.Expr); ok {
				return !v, true
			}
		}
		return false, false
	case generator.BinaryExpr:
		op := strings.ToUpper(strings.TrimSpace(e.Op))
		switch op {
		case "AND":
			left, lok := dqpExprConstBool(e.Left)
			right, rok := dqpExprConstBool(e.Right)
			if lok && !left {
				return false, true
			}
			if rok && !right {
				return false, true
			}
			if lok && rok {
				return left && right, true
			}
			return false, false
		case "OR":
			left, lok := dqpExprConstBool(e.Left)
			right, rok := dqpExprConstBool(e.Right)
			if lok && left {
				return true, true
			}
			if rok && right {
				return true, true
			}
			if lok && rok {
				return left || right, true
			}
			return false, false
		case "=", "<=>", "!=", "<>":
			left, lok := dqpLiteralValue(e.Left)
			right, rok := dqpLiteralValue(e.Right)
			if !lok || !rok {
				return false, false
			}
			eq := reflect.DeepEqual(left, right)
			if op == "=" || op == "<=>" {
				return eq, true
			}
			return !eq, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}

func dqpLiteralValue(expr generator.Expr) (any, bool) {
	switch e := expr.(type) {
	case generator.LiteralExpr:
		return e.Value, true
	case *generator.LiteralExpr:
		if e == nil {
			return nil, false
		}
		return e.Value, true
	default:
		return nil, false
	}
}

func dqpLiteralAsBool(v any) (value bool, ok bool) {
	switch x := v.(type) {
	case nil:
		return false, true
	case bool:
		return x, true
	case int:
		return x != 0, true
	case int8:
		return x != 0, true
	case int16:
		return x != 0, true
	case int32:
		return x != 0, true
	case int64:
		return x != 0, true
	case uint:
		return x != 0, true
	case uint8:
		return x != 0, true
	case uint16:
		return x != 0, true
	case uint32:
		return x != 0, true
	case uint64:
		return x != 0, true
	case string:
		s := strings.TrimSpace(strings.ToLower(x))
		switch s {
		case "0", "false":
			return false, true
		case "1", "true":
			return true, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}

type dqpVariant struct {
	sql          string
	signatureSQL string
	hint         string
	group        string
}

type dqpVariantMetrics struct {
	hintInjectedTotal  int64
	hintFallbackTotal  int64
	setVarVariantTotal int64
	hintLengthMin      int64
	hintLengthMax      int64
	hintLengthSum      int64
	hintLengthCount    int64
}

func (m *dqpVariantMetrics) observeVariant(baseSQL string, variantSQL string, hint string) {
	if m == nil {
		return
	}
	if variantSQL != baseSQL {
		m.hintInjectedTotal++
	} else {
		m.hintFallbackTotal++
	}
	if dqpHintContainsSetVar(hint) {
		m.setVarVariantTotal++
	}
	hintLen := int64(len(strings.TrimSpace(hint)))
	if hintLen <= 0 {
		return
	}
	if m.hintLengthCount == 0 || hintLen < m.hintLengthMin {
		m.hintLengthMin = hintLen
	}
	if hintLen > m.hintLengthMax {
		m.hintLengthMax = hintLen
	}
	m.hintLengthSum += hintLen
	m.hintLengthCount++
}

func (m dqpVariantMetrics) resultMetrics() map[string]int64 {
	if m.hintInjectedTotal == 0 &&
		m.hintFallbackTotal == 0 &&
		m.setVarVariantTotal == 0 &&
		m.hintLengthCount == 0 {
		return nil
	}
	metrics := map[string]int64{
		"dqp_hint_injected_total":   m.hintInjectedTotal,
		"dqp_hint_fallback_total":   m.hintFallbackTotal,
		"dqp_set_var_variant_total": m.setVarVariantTotal,
	}
	if m.hintLengthCount > 0 {
		metrics["dqp_hint_length_min"] = m.hintLengthMin
		metrics["dqp_hint_length_max"] = m.hintLengthMax
		metrics["dqp_hint_length_sum"] = m.hintLengthSum
		metrics["dqp_hint_length_count"] = m.hintLengthCount
	}
	return metrics
}

func dqpHintContainsSetVar(hint string) bool {
	for _, token := range splitTopLevelHintList(hint) {
		trimmed := strings.TrimSpace(token)
		if strings.HasPrefix(strings.ToUpper(trimmed), "SET_VAR(") {
			return true
		}
	}
	return false
}

func dqpLogWarnings(stage string, hint string, sqlText string, warnings []string) {
	if len(warnings) == 0 {
		return
	}
	sample, omitted := dqpWarningSample(warnings, dqpWarningLogMaxItems)
	warnText := strings.Join(sample, " | ")
	if omitted > 0 {
		warnText = fmt.Sprintf("%s | ...(+%d more)", warnText, omitted)
	}
	if strings.TrimSpace(hint) == "" {
		util.Detailf("dqp warnings stage=%s count=%d sql=%s warnings=%s", stage, len(warnings), dqpCompactSQL(sqlText, 600), warnText)
		return
	}
	util.Detailf("dqp warnings stage=%s count=%d hint=%s sql=%s warnings=%s", stage, len(warnings), hint, dqpCompactSQL(sqlText, 600), warnText)
}

func dqpWarningSample(warnings []string, limit int) ([]string, int) {
	if limit <= 0 || len(warnings) <= limit {
		out := make([]string, 0, len(warnings))
		out = append(out, warnings...)
		return out, 0
	}
	out := make([]string, 0, limit)
	out = append(out, warnings[:limit]...)
	return out, len(warnings) - limit
}

func dqpCompactSQL(sqlText string, maxLen int) string {
	compact := strings.Join(strings.Fields(strings.TrimSpace(sqlText)), " ")
	if maxLen <= 0 || len(compact) <= maxLen {
		return compact
	}
	return compact[:maxLen] + "..."
}

func dqpFormatHintGroups(groups map[string]struct{}) string {
	if len(groups) == 0 {
		return ""
	}
	parts := make([]string, 0, len(groups))
	for group := range groups {
		if strings.TrimSpace(group) == "" {
			continue
		}
		parts = append(parts, group)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

type dqpHintTableFactor struct {
	hintName   string
	tableName  string
	derived    bool
	hasIndex   bool
	indexCount int
}

func dqpHintTableFactors(query *generator.SelectQuery, state *schema.State) []dqpHintTableFactor {
	if query == nil {
		return nil
	}
	factors := make([]dqpHintTableFactor, 0, 1+len(query.From.Joins))
	appendFactor := func(hintName string, tableName string, derived bool) {
		name := strings.TrimSpace(hintName)
		if name == "" {
			return
		}
		factor := dqpHintTableFactor{
			hintName:  name,
			tableName: strings.TrimSpace(tableName),
			derived:   derived,
		}
		if state != nil && !derived && factor.tableName != "" {
			if tbl, ok := state.TableByName(factor.tableName); ok {
				factor.hasIndex = tableHasIndex(tbl)
				factor.indexCount = dqpTableIndexCount(tbl)
			}
		}
		factors = append(factors, factor)
	}

	baseHintName := query.From.BaseTable
	if strings.TrimSpace(query.From.BaseAlias) != "" {
		baseHintName = query.From.BaseAlias
	}
	appendFactor(baseHintName, query.From.BaseTable, query.From.BaseQuery != nil)
	for _, join := range query.From.Joins {
		hintName := join.Table
		if strings.TrimSpace(join.TableAlias) != "" {
			hintName = join.TableAlias
		}
		appendFactor(hintName, join.Table, join.TableQuery != nil)
	}
	return dqpDedupHintTableFactors(factors)
}

func dqpDedupHintTableFactors(in []dqpHintTableFactor) []dqpHintTableFactor {
	if len(in) == 0 {
		return nil
	}
	out := make([]dqpHintTableFactor, 0, len(in))
	seen := make(map[string]int, len(in))
	for _, factor := range in {
		key := strings.ToLower(strings.TrimSpace(factor.hintName))
		if key == "" {
			continue
		}
		if idx, ok := seen[key]; ok {
			merged := out[idx]
			merged.hasIndex = merged.hasIndex || factor.hasIndex
			if factor.indexCount > merged.indexCount {
				merged.indexCount = factor.indexCount
			}
			if merged.tableName == "" {
				merged.tableName = factor.tableName
			}
			merged.derived = merged.derived && factor.derived
			out[idx] = merged
			continue
		}
		seen[key] = len(out)
		out = append(out, factor)
	}
	return out
}

func dqpHintTableNames(query *generator.SelectQuery, state *schema.State) []string {
	factors := dqpHintTableFactors(query, state)
	if len(factors) == 0 {
		return nil
	}
	names := make([]string, 0, len(factors))
	for _, factor := range factors {
		if strings.TrimSpace(factor.hintName) == "" {
			continue
		}
		names = append(names, factor.hintName)
	}
	return names
}

func dqpTableIndexCount(table schema.Table) int {
	count := len(table.Indexes)
	if table.HasPK {
		count++
	}
	for _, col := range table.Columns {
		if col.HasIndex {
			count++
		}
	}
	return count
}

func dqpJoinHintCandidates(query *generator.SelectQuery, state *schema.State, noArgHints map[string]struct{}) []string {
	if query == nil || len(query.From.Joins) == 0 {
		return nil
	}
	factors := dqpHintTableFactors(query, state)
	if len(factors) < 2 {
		return nil
	}
	tables := make([]string, 0, len(factors))
	for _, factor := range factors {
		tables = append(tables, factor.hintName)
	}

	candidates := []string{
		buildHintSQL(HintHashJoin, tables, noArgHints),
		buildHintSQL(HintNoHashJoin, tables, noArgHints),
		buildHintSQL(HintStraightJoin, tables, noArgHints),
	}
	if !dqpShouldUseStrictJoinHints(query, factors) {
		return dqpDedupHints(candidates)
	}

	candidates = append(candidates, buildHintSQL(HintMergeJoin, tables, noArgHints))
	candidates = append(candidates, fmt.Sprintf(HintLeadingFmt, strings.Join(tables, ", ")))
	for _, target := range dqpINLJoinHintTargets(factors) {
		candidates = append(candidates,
			fmt.Sprintf("%s(%s)", HintInlJoin, target),
			fmt.Sprintf("%s(%s)", HintInlHashJoin, target),
		)
	}
	if len(tables) == 2 {
		target := tables[1]
		candidates = append(candidates,
			fmt.Sprintf("%s(%s)", HintHashJoinBuild, target),
			fmt.Sprintf("%s(%s)", HintHashJoinProbe, target),
		)
	}
	return dqpDedupHints(candidates)
}

func dqpShouldUseStrictJoinHints(query *generator.SelectQuery, factors []dqpHintTableFactor) bool {
	if query == nil || len(query.From.Joins) == 0 {
		return false
	}
	for _, factor := range factors {
		if factor.derived {
			return false
		}
	}
	for _, join := range query.From.Joins {
		if join.Natural || join.Type != generator.JoinInner {
			return false
		}
	}
	return true
}

func dqpINLJoinHintTargets(factors []dqpHintTableFactor) []string {
	if len(factors) <= 1 {
		return nil
	}
	targets := make([]string, 0, len(factors)-1)
	seen := make(map[string]struct{}, len(factors)-1)
	for i := 1; i < len(factors); i++ {
		factor := factors[i]
		if factor.derived || !factor.hasIndex {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(factor.hintName))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		targets = append(targets, factor.hintName)
	}
	return targets
}

func dqpIndexHintCandidates(query *generator.SelectQuery, state *schema.State) []string {
	if query == nil || state == nil {
		return nil
	}
	factors := dqpHintTableFactors(query, state)
	candidates := make([]string, 0, len(factors)*2)
	for _, factor := range factors {
		if factor.derived || !factor.hasIndex {
			continue
		}
		candidates = append(candidates, fmt.Sprintf(HintUseIndexFmt, factor.hintName))
		if dqpShouldUseIndexMergeHint(query, factor) {
			candidates = append(candidates, fmt.Sprintf(HintUseIndexMergeFmt, factor.hintName))
		}
	}
	return dqpDedupHints(candidates)
}

func dqpShouldUseIndexMergeHint(query *generator.SelectQuery, factor dqpHintTableFactor) bool {
	if query == nil || query.Where == nil {
		return false
	}
	if factor.derived || factor.indexCount < 2 {
		return false
	}
	if !dqpExprHasDisjunction(query.Where) {
		return false
	}
	return dqpExprReferencesTable(query.Where, factor.hintName)
}

func dqpExprHasDisjunction(expr generator.Expr) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case generator.BinaryExpr:
		if strings.EqualFold(strings.TrimSpace(e.Op), "OR") {
			return true
		}
		return dqpExprHasDisjunction(e.Left) || dqpExprHasDisjunction(e.Right)
	case generator.UnaryExpr:
		return dqpExprHasDisjunction(e.Expr)
	case generator.InExpr:
		if dqpExprHasDisjunction(e.Left) {
			return true
		}
		for _, item := range e.List {
			if dqpExprHasDisjunction(item) {
				return true
			}
		}
		return false
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if dqpExprHasDisjunction(w.When) || dqpExprHasDisjunction(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return dqpExprHasDisjunction(e.Else)
		}
		return false
	case generator.FuncExpr:
		for _, arg := range e.Args {
			if dqpExprHasDisjunction(arg) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func dqpExprReferencesTable(expr generator.Expr, table string) bool {
	name := strings.TrimSpace(table)
	if expr == nil || name == "" {
		return false
	}
	for _, col := range expr.Columns() {
		if strings.EqualFold(strings.TrimSpace(col.Table), name) {
			return true
		}
	}
	return false
}

func dqpDedupHints(candidates []string) []string {
	if len(candidates) == 0 {
		return nil
	}
	out := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, hint := range candidates {
		trimmed := strings.TrimSpace(hint)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func buildDQPVariants(query *generator.SelectQuery, state *schema.State, hasSemi bool, hasCorr bool, hasAgg bool, hasSubquery bool, hasCTE bool, hasPartition bool, gen *generator.Generator) ([]dqpVariant, dqpVariantMetrics) {
	tables := dqpHintTableNames(query, state)
	if len(tables) == 0 {
		tables = make([]string, 0, 1+len(query.From.Joins))
		tables = append(tables, query.From.BaseTable)
		for _, join := range query.From.Joins {
			tables = append(tables, join.Table)
		}
	}
	hasJoin := len(query.From.Joins) > 0
	baseSQL := query.SQLString()
	metrics := dqpVariantMetrics{}

	noArgHints := map[string]struct{}{
		HintStraightJoin:    {},
		HintSemiJoinRewrite: {},
		HintNoDecorrelate:   {},
		HintHashAgg:         {},
		HintStreamAgg:       {},
		HintAggToCop:        {},
	}
	externalBaseHints, externalSetVarHints := dqpExternalHintCandidates(gen, tables, noArgHints)
	baseHints := dqpHintsForBuiltQuery(gen, query, state, hasSemi, hasCorr, hasAgg, noArgHints, externalBaseHints)
	variants := make([]dqpVariant, 0, len(baseHints)+1)

	for _, hintSQL := range baseHints {
		variantSQL := injectHint(query, hintSQL)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		metrics.observeVariant(baseSQL, variantSQL, hintSQL)
		variants = append(variants, dqpVariant{
			sql:          variantSQL,
			signatureSQL: variantSig,
			hint:         hintSQL,
			group:        dqpVariantGroupBaseHint,
		})
	}

	setVarHints := dqpSetVarHints(gen, dqpJoinTableCountWithCTE(query), hasJoin, hasSemi, hasCorr, hasSubquery, hasCTE, hasPartition, externalSetVarHints)
	nonMPPSetVarHints := dqpFilterSetVarHints(setVarHints, false)
	mppSetVarHints := dqpFilterSetVarHints(setVarHints, true)
	for _, hintSQL := range nonMPPSetVarHints {
		variantSQL := injectHint(query, hintSQL)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		metrics.observeVariant(baseSQL, variantSQL, hintSQL)
		variants = append(variants, dqpVariant{
			sql:          variantSQL,
			signatureSQL: variantSig,
			hint:         hintSQL,
			group:        dqpVariantGroupSetVar,
		})
	}
	for _, hintSQL := range mppSetVarHints {
		variantSQL := injectHint(query, hintSQL)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		metrics.observeVariant(baseSQL, variantSQL, hintSQL)
		variants = append(variants, dqpVariant{
			sql:          variantSQL,
			signatureSQL: variantSig,
			hint:         hintSQL,
			group:        dqpVariantGroupMPP,
		})
	}
	combinedSetVarHints := dqpCombinedSetVarHints(nonMPPSetVarHints, mppSetVarHints)
	for _, hintSQL := range buildCombinedHints(combinedSetVarHints, baseHints, MaxCombinedHintVariants) {
		variantSQL := injectHint(query, hintSQL)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		metrics.observeVariant(baseSQL, variantSQL, hintSQL)
		variants = append(variants, dqpVariant{
			sql:          variantSQL,
			signatureSQL: variantSig,
			hint:         hintSQL,
			group:        dqpVariantGroupCombined,
		})
	}
	for _, hint := range dqpIndexHintCandidates(query, state) {
		variantSQL := injectHint(query, hint)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		metrics.observeVariant(baseSQL, variantSQL, hint)
		variants = append(variants, dqpVariant{
			sql:          variantSQL,
			signatureSQL: variantSig,
			hint:         hint,
			group:        dqpVariantGroupIndex,
		})
	}

	return variants, metrics
}

func dqpHintsForBuiltQuery(gen *generator.Generator, query *generator.SelectQuery, state *schema.State, hasSemi bool, hasCorr bool, hasAgg bool, noArgHints map[string]struct{}, externalBaseHints []string) []string {
	var candidates []string
	candidates = append(candidates, dqpJoinHintCandidates(query, state, noArgHints)...)
	if hasAgg {
		aggHints := []string{HintHashAgg, HintStreamAgg, HintAggToCop}
		tables := dqpHintTableNames(query, state)
		for _, hint := range aggHints {
			candidates = append(candidates, buildHintSQL(hint, tables, noArgHints))
		}
	}
	if hasSemi {
		tables := dqpHintTableNames(query, state)
		candidates = append(candidates, buildHintSQL(HintSemiJoinRewrite, tables, noArgHints))
	}
	if hasCorr {
		tables := dqpHintTableNames(query, state)
		candidates = append(candidates, buildHintSQL(HintNoDecorrelate, tables, noArgHints))
	}
	candidates = append(candidates, externalBaseHints...)
	return pickHintsWithBandit(gen, dqpDedupHints(candidates), dqpBaseHintPickLimit(gen))
}

func dqpHintsForQuery(gen *generator.Generator, tables []string, hasJoin bool, hasSemi bool, hasCorr bool, hasAgg bool, noArgHints map[string]struct{}, externalBaseHints []string) []string {
	var candidates []string
	if hasJoin {
		joinHints := []string{
			HintHashJoin,
			HintNoHashJoin,
			HintMergeJoin,
			HintInlJoin,
			HintInlHashJoin,
			HintHashJoinBuild,
			HintHashJoinProbe,
			HintLeading,
			HintStraightJoin,
		}
		for _, hint := range joinHints {
			candidates = append(candidates, buildHintSQL(hint, tables, noArgHints))
		}
	}
	if hasAgg {
		aggHints := []string{HintHashAgg, HintStreamAgg, HintAggToCop}
		for _, hint := range aggHints {
			candidates = append(candidates, buildHintSQL(hint, tables, noArgHints))
		}
	}
	if hasSemi {
		candidates = append(candidates, buildHintSQL(HintSemiJoinRewrite, tables, noArgHints))
	}
	if hasCorr {
		candidates = append(candidates, buildHintSQL(HintNoDecorrelate, tables, noArgHints))
	}
	candidates = append(candidates, externalBaseHints...)
	return pickHintsWithBandit(gen, candidates, dqpBaseHintPickLimit(gen))
}

func dqpSetVarHints(gen *generator.Generator, tableCount int, hasJoin bool, hasSemi bool, hasCorr bool, hasSubquery bool, hasCTE bool, hasPartition bool, externalSetVarHints []string) []string {
	candidates := dqpSetVarHintCandidates(gen, tableCount, hasJoin, hasSemi, hasCorr, hasSubquery, hasCTE, hasPartition, externalSetVarHints)
	if len(candidates) == 0 {
		return nil
	}
	maxPick := dqpSetVarHintPickMax(gen)
	if maxPick > len(candidates) {
		maxPick = len(candidates)
	}
	if maxPick <= 0 {
		return nil
	}
	requireMPP := dqpShouldRequireMPPSetVar(gen, hasJoin)
	pool := candidates
	if requireMPP {
		if mppPool := dqpFilterSetVarHints(candidates, true); len(mppPool) > 0 {
			pool = mppPool
		}
	}
	selected := pickHintsWithBandit(gen, pool, 1)
	if len(selected) == 0 {
		return nil
	}
	// Keep each DQP run focused: mutate one SET_VAR dimension at a time.
	if maxPick > 2 {
		maxPick = 2
	}
	return dqpEnsureSetVarTogglePairs(selected, candidates, maxPick)
}

func dqpBaseHintPickLimit(gen *generator.Generator) int {
	if gen == nil || gen.Config.Oracles.DQPBaseHintPick <= 0 {
		return dqpBaseHintPickLimitDefault
	}
	return gen.Config.Oracles.DQPBaseHintPick
}

func dqpSetVarHintPickMax(gen *generator.Generator) int {
	if gen == nil || gen.Config.Oracles.DQPSetVarHintPick <= 0 {
		return dqpSetVarHintPickMaxDefault
	}
	return gen.Config.Oracles.DQPSetVarHintPick
}

func dqpShouldRequireMPPSetVar(gen *generator.Generator, hasJoin bool) bool {
	if gen == nil || !hasJoin {
		return false
	}
	return !gen.Config.Oracles.DisableMPP && gen.Config.Oracles.MPPTiFlashReplica > 0
}

func dqpEnsureSetVarTogglePairs(selected []string, candidates []string, limit int) []string {
	if len(selected) == 0 || len(candidates) == 0 || limit <= 0 {
		return selected
	}
	if limit < len(selected) {
		limit = len(selected)
	}
	pool := dqpBuildSetVarTogglePool(candidates)
	if len(pool) == 0 {
		return selected
	}
	out := append([]string(nil), selected...)
	seen := make(map[string]struct{}, len(out))
	for _, hint := range out {
		seen[strings.TrimSpace(hint)] = struct{}{}
	}
	for _, hint := range selected {
		name, value, ok := dqpParseSingleSetVarHint(hint)
		if !ok {
			continue
		}
		targets := dqpOppositeSetVarTargets(name, value)
		if len(targets) == 0 {
			continue
		}
		for _, target := range targets {
			options, ok := pool[target.name]
			if !ok {
				continue
			}
			oppositeHint, ok := options[target.value]
			if !ok {
				continue
			}
			trimmed := strings.TrimSpace(oppositeHint)
			if trimmed == "" {
				continue
			}
			if _, exists := seen[trimmed]; exists {
				continue
			}
			if len(out) >= limit {
				break
			}
			out = append(out, trimmed)
			seen[trimmed] = struct{}{}
			break
		}
		if len(out) >= limit {
			break
		}
	}
	return out
}

type dqpSetVarToggleTarget struct {
	name  string
	value string
}

func dqpOppositeSetVarTargets(name string, value string) []dqpSetVarToggleTarget {
	normalizedName := strings.ToLower(strings.TrimSpace(name))
	toggle := dqpNormalizeToggleValue(value)
	if normalizedName == "" || toggle == "" {
		return nil
	}
	switch normalizedName {
	case "tidb_enforce_mpp":
		if toggle == "ON" {
			return []dqpSetVarToggleTarget{
				{name: "tidb_allow_mpp", value: "OFF"},
				{name: "tidb_enforce_mpp", value: "OFF"},
			}
		}
		return []dqpSetVarToggleTarget{
			{name: "tidb_allow_mpp", value: "ON"},
			{name: "tidb_enforce_mpp", value: "ON"},
		}
	case "tidb_allow_mpp":
		if toggle == "OFF" {
			return []dqpSetVarToggleTarget{
				{name: "tidb_enforce_mpp", value: "ON"},
				{name: "tidb_allow_mpp", value: "ON"},
			}
		}
	}
	pair := dqpOppositeToggleValue(toggle)
	if pair == "" {
		return nil
	}
	return []dqpSetVarToggleTarget{{name: normalizedName, value: pair}}
}

func dqpBuildSetVarTogglePool(candidates []string) map[string]map[string]string {
	pool := make(map[string]map[string]string, len(candidates))
	for _, hint := range candidates {
		name, value, ok := dqpParseSingleSetVarHint(hint)
		if !ok {
			continue
		}
		toggle := dqpNormalizeToggleValue(value)
		if toggle == "" {
			continue
		}
		byValue, ok := pool[name]
		if !ok {
			byValue = make(map[string]string, 2)
			pool[name] = byValue
		}
		trimmed := strings.TrimSpace(hint)
		if trimmed == "" {
			continue
		}
		if _, exists := byValue[toggle]; !exists {
			byValue[toggle] = trimmed
		}
	}
	return pool
}

func dqpParseSingleSetVarHint(hint string) (name string, value string, ok bool) {
	trimmed := strings.TrimSpace(hint)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SET_VAR(") || !strings.HasSuffix(trimmed, ")") {
		return "", "", false
	}
	body := strings.TrimSpace(trimmed[len("SET_VAR(") : len(trimmed)-1])
	if body == "" || strings.Count(body, "=") != 1 {
		return "", "", false
	}
	parts := strings.SplitN(body, "=", 2)
	name = strings.ToLower(strings.TrimSpace(parts[0]))
	value = strings.TrimSpace(parts[1])
	if name == "" || value == "" {
		return "", "", false
	}
	return name, value, true
}

func dqpNormalizeToggleValue(value string) string {
	v := strings.TrimSpace(value)
	v = strings.Trim(v, "'")
	switch strings.ToUpper(v) {
	case "ON", "TRUE", "1":
		return "ON"
	case "OFF", "FALSE", "0":
		return "OFF"
	default:
		return ""
	}
}

func dqpOppositeToggleValue(value string) string {
	switch dqpNormalizeToggleValue(value) {
	case "ON":
		return "OFF"
	case "OFF":
		return "ON"
	default:
		return ""
	}
}

func dqpHasSetVarCategory(hints []string, mpp bool) bool {
	for _, hint := range hints {
		if isMPPSetVarHint(hint) == mpp {
			return true
		}
	}
	return false
}

func dqpFilterSetVarHints(candidates []string, mpp bool) []string {
	out := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, hint := range candidates {
		trimmed := strings.TrimSpace(hint)
		if trimmed == "" || isMPPSetVarHint(trimmed) != mpp {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func dqpCombinedSetVarHints(nonMPPSetVarHints []string, mppSetVarHints []string) []string {
	if len(nonMPPSetVarHints) == 0 && len(mppSetVarHints) == 0 {
		return nil
	}
	combined := make([]string, 0, len(nonMPPSetVarHints)+len(mppSetVarHints))
	// Keep MPP hints ahead so combined variants prioritize optimizer+MPP overlays.
	combined = append(combined, mppSetVarHints...)
	combined = append(combined, nonMPPSetVarHints...)
	return dqpDedupHints(combined)
}

func dqpSetVarHintCandidates(gen *generator.Generator, tableCount int, hasJoin bool, hasSemi bool, hasCorr bool, hasSubquery bool, hasCTE bool, hasPartition bool, externalSetVarHints []string) []string {
	var candidates []string
	disableMPP := dqpDisableMPP(gen)
	if hasJoin {
		candidates = append(candidates, toggleHints(SetVarEnableHashJoinOn, SetVarEnableHashJoinOff)...)
		candidates = append(candidates, toggleHints(SetVarEnableOuterJoinReorderOn, SetVarEnableOuterJoinReorderOff)...)
		candidates = append(candidates, toggleHints(SetVarEnableInlJoinInnerMultiOn, SetVarEnableInlJoinInnerMultiOff)...)
		if !disableMPP {
			candidates = append(candidates, toggleHints(SetVarAllowMPPOn, SetVarAllowMPPOff)...)
			candidates = append(candidates, SetVarEnforceMPPOn)
		}
	}
	candidates = append(candidates, toggleHints(SetVarPartialOrderedTopNCost, SetVarPartialOrderedTopNDisable)...)
	if hasSubquery {
		candidates = append(candidates, toggleHints(SetVarEnableNonEvalScalarSubqueryOn, SetVarEnableNonEvalScalarSubqueryOff)...)
	}
	if hasSemi {
		candidates = append(candidates, toggleHints(SetVarEnableSemiJoinRewriteOn, SetVarEnableSemiJoinRewriteOff)...)
	}
	if hasCorr {
		candidates = append(candidates, toggleHints(SetVarEnableNoDecorrelateOn, SetVarEnableNoDecorrelateOff)...)
	}
	if hasJoin {
		candidates = append(candidates, toggleHints(SetVarEnableTojaOn, SetVarEnableTojaOff)...)
	}
	if hasCTE {
		candidates = append(candidates, toggleHints(SetVarForceInlineCTEOn, SetVarForceInlineCTEOff)...)
	}
	candidates = append(candidates, joinReorderThresholdHints(gen, tableCount)...)
	if hasPartition {
		candidates = append(candidates, toggleHints(SetVarPartitionPruneDynamic, SetVarPartitionPruneStatic)...)
	}
	candidates = append(candidates,
		toggleHints(SetVarFixControl33031On, SetVarFixControl33031Off)...,
	)
	candidates = append(candidates,
		toggleHints(SetVarFixControl44830On, SetVarFixControl44830Off)...,
	)
	candidates = append(candidates, toggleHints(SetVarFixControl44855On, SetVarFixControl44855Off)...)
	candidates = append(candidates, SetVarFixControl45132Zero)
	candidates = append(candidates, externalSetVarHints...)
	return candidates
}

func dqpExternalHintCandidates(gen *generator.Generator, tables []string, noArgHints map[string]struct{}) (baseHints []string, setVarHints []string) {
	if gen == nil {
		return nil, nil
	}
	rawHints := gen.Config.Oracles.DQPExternalHints
	if len(rawHints) == 0 {
		return nil, nil
	}
	disableMPP := dqpDisableMPP(gen)
	baseHints = make([]string, 0, len(rawHints))
	setVarHints = make([]string, 0, len(rawHints))
	for _, raw := range rawHints {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, "*/") {
			continue
		}
		setVarHint, isSetVar, valid := normalizeSetVarHint(trimmed)
		if isSetVar {
			if valid {
				if disableMPP && isMPPSetVarHint(setVarHint) {
					continue
				}
				setVarHints = append(setVarHints, setVarHint)
			}
			continue
		}
		baseHints = append(baseHints, buildHintSQL(trimmed, tables, noArgHints))
	}
	return baseHints, setVarHints
}

func dqpDisableMPP(gen *generator.Generator) bool {
	return gen != nil && gen.Config.Oracles.DisableMPP
}

func isMPPSetVarHint(hint string) bool {
	trimmed := strings.TrimSpace(hint)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SET_VAR(") || !strings.HasSuffix(trimmed, ")") {
		return false
	}
	assignment := strings.TrimSpace(trimmed[len("SET_VAR(") : len(trimmed)-1])
	name, _, ok := strings.Cut(assignment, "=")
	if !ok {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "tidb_allow_mpp", "tidb_enforce_mpp":
		return true
	default:
		return false
	}
}

func normalizeSetVarHint(raw string) (hint string, isSetVar bool, valid bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false, false
	}
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "SET_VAR(") {
		if strings.HasSuffix(trimmed, ")") {
			return trimmed, true, true
		}
		return "", true, false
	}
	if strings.Contains(trimmed, "=") {
		return "SET_VAR(" + trimmed + ")", true, true
	}
	return "", false, false
}

func dqpHintReward(mismatch bool) float64 {
	if mismatch {
		// Prefer variants that expose optimizer-dependent result differences.
		return 1.0
	}
	return 0.2
}

func dqpReplaySetVarAssignment(hint string) (string, bool) {
	for _, token := range splitTopLevelHintList(hint) {
		trimmed := strings.TrimSpace(token)
		if trimmed == "" {
			continue
		}
		upper := strings.ToUpper(trimmed)
		if !strings.HasPrefix(upper, "SET_VAR(") || !strings.HasSuffix(trimmed, ")") {
			continue
		}
		body := strings.TrimSpace(trimmed[len("SET_VAR(") : len(trimmed)-1])
		assignment, ok := normalizeReplaySetVarAssignment(body)
		if !ok {
			continue
		}
		return assignment, true
	}
	return "", false
}

func normalizeReplaySetVarAssignment(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false
	}
	if strings.Contains(trimmed, "*/") ||
		strings.Contains(trimmed, "/*") ||
		strings.Contains(trimmed, "--") ||
		strings.Contains(trimmed, ";") {
		return "", false
	}
	if strings.Count(trimmed, "=") != 1 {
		return "", false
	}
	parts := strings.SplitN(trimmed, "=", 2)
	name := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])
	if !replaySetVarNamePattern.MatchString(name) {
		return "", false
	}
	if !replaySetVarUnquotedPattern.MatchString(value) && !replaySetVarSingleQuotedExpr.MatchString(value) {
		return "", false
	}
	return name + "=" + value, true
}

func splitTopLevelHintList(hints string) []string {
	if strings.TrimSpace(hints) == "" {
		return nil
	}
	out := make([]string, 0, 4)
	depth := 0
	inString := false
	escape := false
	start := 0
	for i := 0; i < len(hints); i++ {
		ch := hints[i]
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
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				out = append(out, hints[start:i])
				start = i + 1
			}
		}
	}
	out = append(out, hints[start:])
	return out
}

func cteHasUnstableLimit(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	for _, cte := range query.With {
		if cte.Query == nil {
			continue
		}
		if cte.Query.Limit != nil && len(cte.Query.OrderBy) == 0 {
			return true
		}
		if cteHasUnstableLimit(cte.Query) {
			return true
		}
	}
	return false
}

func toggleHints(on, off string) []string {
	if strings.TrimSpace(on) == "" || strings.TrimSpace(off) == "" {
		return nil
	}
	return []string{on, off}
}

func joinReorderThresholdHints(gen *generator.Generator, tableCount int) []string {
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
	if upper > lower && gen != nil {
		value = lower + gen.Rand.Intn(upper-lower+1)
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
	return len(table.Indexes) > 0
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

func pickHintsWithBandit(gen *generator.Generator, candidates []string, limit int) []string {
	if gen == nil {
		return pickHintsBandit(nil, candidates, limit, 0, 0)
	}
	return pickHintsBandit(gen.Rand, candidates, limit, gen.Config.Adaptive.WindowSize, gen.Config.Adaptive.UCBExploration)
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
