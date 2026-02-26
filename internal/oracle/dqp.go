package oracle

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
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

const dqpBuildMaxTries = 10
const dqpComplexityJoinCountThreshold = 4
const dqpComplexitySetOpsThreshold = 2
const dqpComplexityDerivedThreshold = 3
const dqpComplexityFalseJoinThreshold = 3
const dqpBaseHintPickLimitDefault = 3
const dqpSetVarHintPickMaxDefault = 3

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
		},
		SkipReasonOverrides: map[string]string{
			"constraint:limit":            "dqp:limit",
			"constraint:window":           "dqp:window",
			"constraint:set_ops":          "dqp:set_ops",
			"constraint:nondeterministic": "dqp:nondeterministic",
			"constraint:predicate_guard":  "dqp:predicate_guard",
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
	if details, skip := dqpComplexitySkipDetails(query); skip {
		return Result{OK: true, Oracle: o.Name(), Details: details}
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
	baseSig, err := exec.QuerySignature(ctx, query.SignatureSQL())
	if err != nil {
		reason, code := sqlErrorReason("dqp", err)
		details := map[string]any{"error_reason": reason}
		if code != 0 {
			details["error_code"] = int(code)
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Err: err, Details: details}
	}

	hasCTE := len(query.With) > 0
	hasPartition := queryHasPartitionedTable(query, state)
	variants := buildDQPVariants(query, state, hasSemi, hasCorr, hasAgg, hasSubquery, hasCTE, hasPartition, gen)
	for _, variant := range variants {
		variantSig, err := exec.QuerySignature(ctx, variant.signatureSQL)
		if err != nil {
			continue
		}
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
			}
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}}
}

type dqpComplexityStats struct {
	joinCount        int
	setOps           int
	derivedTables    int
	alwaysFalseJoins int
}

func dqpComplexitySkipDetails(query *generator.SelectQuery) (map[string]any, bool) {
	stats := dqpCollectComplexityStats(query)
	if stats.setOps >= dqpComplexitySetOpsThreshold && stats.derivedTables >= dqpComplexityDerivedThreshold {
		return map[string]any{
			"skip_reason":                 "dqp:complexity_guard",
			"dqp_complexity_reason":       "set_ops_and_derived_tables",
			"dqp_complexity_join_count":   stats.joinCount,
			"dqp_complexity_set_ops":      stats.setOps,
			"dqp_complexity_derived":      stats.derivedTables,
			"dqp_complexity_false_joins":  stats.alwaysFalseJoins,
			"dqp_complexity_threshold_id": "dqp_complexity_setops_derived_v1",
		}, true
	}
	if stats.alwaysFalseJoins >= dqpComplexityFalseJoinThreshold {
		return map[string]any{
			"skip_reason":                 "dqp:complexity_guard",
			"dqp_complexity_reason":       "always_false_join_chain",
			"dqp_complexity_join_count":   stats.joinCount,
			"dqp_complexity_set_ops":      stats.setOps,
			"dqp_complexity_derived":      stats.derivedTables,
			"dqp_complexity_false_joins":  stats.alwaysFalseJoins,
			"dqp_complexity_threshold_id": "dqp_complexity_false_join_v1",
		}, true
	}
	if stats.joinCount >= dqpComplexityJoinCountThreshold && stats.alwaysFalseJoins*2 >= stats.joinCount {
		return map[string]any{
			"skip_reason":                 "dqp:complexity_guard",
			"dqp_complexity_reason":       "false_join_ratio_high",
			"dqp_complexity_join_count":   stats.joinCount,
			"dqp_complexity_set_ops":      stats.setOps,
			"dqp_complexity_derived":      stats.derivedTables,
			"dqp_complexity_false_joins":  stats.alwaysFalseJoins,
			"dqp_complexity_threshold_id": "dqp_complexity_false_ratio_v1",
		}, true
	}
	return nil, false
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
}

func buildDQPVariants(query *generator.SelectQuery, state *schema.State, hasSemi bool, hasCorr bool, hasAgg bool, hasSubquery bool, hasCTE bool, hasPartition bool, gen *generator.Generator) []dqpVariant {
	tables := make([]string, 0, 1+len(query.From.Joins))
	tables = append(tables, query.From.BaseTable)
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
	externalBaseHints, externalSetVarHints := dqpExternalHintCandidates(gen, tables, noArgHints)
	baseHints := dqpHintsForQuery(gen, tables, hasJoin, hasSemi, hasCorr, hasAgg, noArgHints, externalBaseHints)
	variants := make([]dqpVariant, 0, len(baseHints)+1)

	for _, hintSQL := range baseHints {
		variantSQL := injectHint(query, hintSQL)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: hintSQL})
	}

	setVarHints := dqpSetVarHints(gen, len(tables), hasJoin, hasSemi, hasCorr, hasSubquery, hasCTE, hasPartition, externalSetVarHints)
	for _, hintSQL := range setVarHints {
		variantSQL := injectHint(query, hintSQL)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: hintSQL})
	}
	for _, hintSQL := range buildCombinedHints(setVarHints, baseHints, MaxCombinedHintVariants) {
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
			for _, hint := range pickHintsWithBandit(gen, hints, len(hints)) {
				variantSQL := injectHint(query, hint)
				variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
				variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: hint})
			}
		}
	}

	return variants
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
			HintInlMergeJoin,
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
	limit := dqpSetVarHintPickMax(gen)
	if limit > len(candidates) {
		limit = len(candidates)
	}
	if limit <= 0 {
		return nil
	}
	if gen != nil {
		limit = 1 + gen.Rand.Intn(limit)
	}
	return pickHintsWithBandit(gen, candidates, limit)
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

func dqpSetVarHintCandidates(gen *generator.Generator, tableCount int, hasJoin bool, hasSemi bool, hasCorr bool, hasSubquery bool, hasCTE bool, hasPartition bool, externalSetVarHints []string) []string {
	var candidates []string
	if hasJoin {
		candidates = append(candidates, toggleHints(SetVarEnableHashJoinOn, SetVarEnableHashJoinOff)...)
		candidates = append(candidates, toggleHints(SetVarEnableOuterJoinReorderOn, SetVarEnableOuterJoinReorderOff)...)
		candidates = append(candidates, toggleHints(SetVarEnableInlJoinInnerMultiOn, SetVarEnableInlJoinInnerMultiOff)...)
		candidates = append(candidates, toggleHints(SetVarAllowMPPOn, SetVarAllowMPPOff)...)
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
				setVarHints = append(setVarHints, setVarHint)
			}
			continue
		}
		baseHints = append(baseHints, buildHintSQL(trimmed, tables, noArgHints))
	}
	return baseHints, setVarHints
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
