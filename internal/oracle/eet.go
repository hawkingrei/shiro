package oracle

import (
	"context"
	"fmt"
	"math/rand"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"

	"shiro/internal/util"
)

// EET implements the Equivalent Expression Transformation oracle.
//
// It rewrites predicates using semantics-preserving AST transformations and
// checks that the transformed query returns the same signature.
type EET struct{}

// Name returns the oracle identifier.
func (o EET) Name() string { return "EET" }

const eetBuildMaxTries = 10
const eetTransformRetryMax = 3
const eetComplexityJoinTableThresholdDefault = 5

const eetComplexityConstraintJoinTables = "constraint:eet_complexity_join_tables"

func eetPredicatePolicy(gen *generator.Generator) predicatePolicy {
	policy := predicatePolicyFor(gen)
	policy.allowNot = true
	policy.allowIsNull = true
	policy.allowSubquery = true
	return policy
}

// Run builds a query, applies an equivalent predicate rewrite, and compares
// signatures for mismatches.
func (o EET) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	policy := eetPredicatePolicy(gen)
	complexityThreshold := eetComplexityJoinTableThreshold(gen)
	spec := QuerySpec{
		Oracle:          "eet",
		Profile:         ProfileByName("EET"),
		PredicatePolicy: policy,
		PredicateGuard:  true,
		MaxTries:        eetBuildMaxTries,
		Constraints: generator.SelectQueryConstraints{
			RequireDeterministic: true,
			DisallowSetOps:       true,
			QueryGuardReason: func(query *generator.SelectQuery) (bool, string) {
				reason := eetQueryGuardReason(query, policy, complexityThreshold)
				return reason == "", reason
			},
		},
		SkipReasonOverrides: map[string]string{
			"constraint:nondeterministic":     "eet:nondeterministic",
			"constraint:predicate_guard":      "eet:predicate_guard",
			"constraint:set_ops":              "eet:set_ops",
			eetComplexityConstraintJoinTables: "eet:complexity_guard",
		},
	}

	var (
		query          *generator.SelectQuery
		baseSQL        string
		transformedSQL string
		details        map[string]any
	)
	for attempt := 0; attempt < eetTransformRetryMax; attempt++ {
		query, details = buildQueryWithSpec(gen, spec)
		if query == nil {
			return Result{OK: true, Oracle: o.Name(), Details: details}
		}
		query = query.Clone()
		if state != nil {
			rewriteUsingToOn(query, state)
		}
		if queryHasUsingQualifiedRefs(query) {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:using_qualified_ref"}}
		}
		if gen != nil && !gen.ValidateQueryScope(query) {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:scope_invalid"}}
		}
		if len(query.OrderBy) > 0 {
			if orderByAllConstant(query.OrderBy, len(query.Items)) {
				if query.Limit != nil {
					return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:order_by_constant"}}
				}
				// Constant ORDER BY does not affect signature without LIMIT; drop it to keep EET coverage.
				query.OrderBy = nil
			} else if orderByDistinctKeys(query.OrderBy, len(query.Items)) < 2 {
				if query.Limit != nil {
					return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:order_by_insufficient_columns"}}
				}
				// Non-deterministic ORDER BY is irrelevant to signature without LIMIT; drop it instead of skipping.
				query.OrderBy = nil
			}
		}
		if !eetDistinctOrderByCompatible(query) {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:distinct_order_by"}}
		}
		if eetHasUnstableWindowRank(query) {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:window_rank_unstable_order"}}
		}
		if eetHasUnstableWindowAggregate(query) {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:window_agg_unstable_order"}}
		}
		if skipReason, reason := signaturePrecheck(query, state, "eet"); skipReason != "" {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{
				"skip_reason":     skipReason,
				"precheck_reason": reason,
			}}
		}

		baseSQL = query.SQLString()
		var err error
		transformedSQL, details, err = applyEETTransform(baseSQL, gen)
		if err != nil {
			details["error_reason"] = "eet:parse_error"
			return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Err: err, Details: details}
		}
		if strings.TrimSpace(transformedSQL) == "" || transformedSQL == baseSQL {
			if _, ok := details["skip_reason"]; !ok {
				details["skip_reason"] = "eet:no_transform"
			}
			if attempt+1 < eetTransformRetryMax && eetShouldRetryNoTransform(details) {
				continue
			}
			return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Details: details}
		}
		break
	}

	origSig, err := exec.QuerySignature(ctx, query.SignatureSQL())
	if err != nil {
		if eetIsDistinctOrderByErr(err) {
			details["skip_reason"] = "eet:distinct_order_by_runtime"
			return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Details: details}
		}
		reason, bugHint := eetSignatureErrorDetails(err, "base")
		details["error_reason"] = reason
		if bugHint != "" {
			details["bug_hint"] = bugHint
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Err: err, Details: details}
	}
	transformedSig, err := exec.QuerySignature(ctx, signatureSQLFor(transformedSQL, query.ColumnAliases()))
	if err != nil {
		if eetIsDistinctOrderByErr(err) {
			details["skip_reason"] = "eet:distinct_order_by_runtime"
			return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL, transformedSQL}, Details: details}
		}
		reason, bugHint := eetSignatureErrorDetails(err, "transform")
		details["error_reason"] = reason
		if bugHint != "" {
			details["bug_hint"] = bugHint
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{transformedSQL}, Err: err, Details: details}
	}

	if origSig != transformedSig {
		expectedExplain, expectedExplainErr := explainSQL(ctx, exec, query.SignatureSQL())
		actualSigSQL := signatureSQLFor(transformedSQL, query.ColumnAliases())
		actualExplain, actualExplainErr := explainSQL(ctx, exec, actualSigSQL)
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{baseSQL, transformedSQL},
			Expected: fmt.Sprintf("cnt=%d checksum=%d", origSig.Count, origSig.Checksum),
			Actual:   fmt.Sprintf("cnt=%d checksum=%d", transformedSig.Count, transformedSig.Checksum),
			Details: map[string]any{
				"replay_kind":          "signature",
				"replay_expected_sql":  query.SignatureSQL(),
				"replay_actual_sql":    actualSigSQL,
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
				"rewrite":              details["rewrite"],
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL, transformedSQL}, Details: details}
}

func eetComplexityJoinTableThreshold(gen *generator.Generator) int {
	if gen == nil || gen.Config.Oracles.EETComplexityJoinTableThreshold <= 0 {
		return eetComplexityJoinTableThresholdDefault
	}
	return gen.Config.Oracles.EETComplexityJoinTableThreshold
}

func eetQueryGuardReason(query *generator.SelectQuery, policy predicatePolicy, complexityThreshold int) string {
	if !eetQueryHasPredicate(query) {
		return "constraint:query_guard"
	}
	if !queryHasPredicateMatch(query, policy) {
		return "constraint:predicate_guard"
	}
	if queryTableFactorCountWithCTE(query) > complexityThreshold {
		return eetComplexityConstraintJoinTables
	}
	return ""
}

func eetShouldRetryNoTransform(details map[string]any) bool {
	if len(details) == 0 {
		return false
	}
	reason, _ := details["skip_reason"].(string)
	switch reason {
	case "eet:no_transform", "eet:no_rewrite_kind", "eet:rewrite_no_boolean_target", "eet:rewrite_no_literal_target":
		return true
	default:
		return false
	}
}

func eetSignatureErrorDetails(err error, stage string) (reason string, classification string) {
	if err == nil {
		return fmt.Sprintf("eet:%s_signature_error", stage), ""
	}
	if eetIsDistinctOrderByErr(err) {
		return "eet:distinct_order_by", ""
	}
	switch {
	case IsPlanRefMissingErr(err):
		return "eet:signature_plan_ref_missing", "tidb:plan_reference_missing"
	case IsSchemaColumnMissingErr(err):
		return "eet:signature_missing_column", "tidb:schema_column_missing"
	}
	return fmt.Sprintf("eet:%s_signature_error", stage), ""
}

func eetIsDistinctOrderByErr(err error) bool {
	if err == nil {
		return false
	}
	if code, ok := mysqlErrCode(err); ok && code == 3065 {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "expression #") && strings.Contains(msg, "order by clause") && strings.Contains(msg, "select list")
}

type eetRewriteKind string

const (
	eetRewriteDoubleNot       eetRewriteKind = "double_not"
	eetRewriteAndTrue         eetRewriteKind = "and_true"
	eetRewriteOrFalse         eetRewriteKind = "or_false"
	eetRewriteNumericIdentity eetRewriteKind = "numeric_identity"
	eetRewriteStringIdentity  eetRewriteKind = "string_identity"
	eetRewriteDateIdentity    eetRewriteKind = "date_identity"
)

func applyEETTransform(sqlText string, gen *generator.Generator) (string, map[string]any, error) {
	details := map[string]any{}
	if strings.TrimSpace(sqlText) == "" {
		return "", details, nil
	}
	p := parser.New()
	stmt, err := p.ParseOneStmt(sqlText, "", "")
	if err != nil {
		return "", details, err
	}
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		if _, isSetOpr := stmt.(*ast.SetOprStmt); isSetOpr {
			details["skip_reason"] = "eet:set_ops"
			return "", details, nil
		}
		details["skip_reason"] = "eet:non_select"
		return "", details, nil
	}
	if !selectHasPredicate(sel) {
		details["skip_reason"] = "eet:no_predicate"
		return "", details, nil
	}
	resolver := buildColumnTypeResolver(sel, gen)
	kind, changed, reason := rewriteSelectPredicates(sel, gen, resolver)
	if kind == "" {
		details["skip_reason"] = "eet:no_rewrite_kind"
		return "", details, nil
	}
	details["rewrite"] = string(kind)
	if !changed {
		if reason == "" {
			reason = "eet:no_transform"
		}
		details["skip_reason"] = reason
		return "", details, nil
	}
	restored, err := restoreEETSQL(sel)
	if err != nil {
		details["error_reason"] = "eet:restore_error"
		return "", details, err
	}
	return restored, details, nil
}

func queryHasUsingQualifiedRefs(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	for _, cte := range query.With {
		if queryHasUsingQualifiedRefs(cte.Query) {
			return true
		}
	}
	usingCols := make(map[string]struct{})
	for _, join := range query.From.Joins {
		if len(join.Using) == 0 {
			continue
		}
		for _, col := range join.Using {
			usingCols[col] = struct{}{}
		}
	}
	if len(usingCols) == 0 {
		return false
	}
	exprs := []generator.Expr{}
	for _, item := range query.Items {
		exprs = append(exprs, item.Expr)
	}
	if query.Where != nil {
		exprs = append(exprs, query.Where)
	}
	if query.Having != nil {
		exprs = append(exprs, query.Having)
	}
	exprs = append(exprs, query.GroupBy...)
	for _, ob := range query.OrderBy {
		exprs = append(exprs, ob.Expr)
	}
	for _, join := range query.From.Joins {
		if join.On != nil {
			exprs = append(exprs, join.On)
		}
	}
	for _, expr := range exprs {
		if hasUsingQualifiedRef(expr, usingCols) {
			return true
		}
	}
	return false
}

func eetDistinctOrderByCompatible(query *generator.SelectQuery) bool {
	if query == nil || !query.Distinct || len(query.OrderBy) == 0 {
		return true
	}
	selectExprs := make(map[string]struct{}, len(query.Items))
	selectAliases := make(map[string]struct{}, len(query.Items))
	for _, item := range query.Items {
		if item.Expr != nil {
			selectExprs[eetExprKey(item.Expr)] = struct{}{}
		}
		alias := strings.TrimSpace(strings.ToLower(item.Alias))
		if alias != "" {
			selectAliases[alias] = struct{}{}
		}
	}
	for _, ob := range query.OrderBy {
		if ordinal, ok := orderByLiteralInt(ob.Expr); ok {
			if ordinal >= 1 && ordinal <= len(query.Items) {
				continue
			}
			return false
		}
		if col, ok := ob.Expr.(generator.ColumnExpr); ok && col.Ref.Table == "" {
			name := strings.TrimSpace(strings.ToLower(col.Ref.Name))
			if name != "" {
				if _, ok := selectAliases[name]; ok {
					continue
				}
			}
		}
		if _, ok := selectExprs[eetExprKey(ob.Expr)]; ok {
			continue
		}
		return false
	}
	return true
}

func eetHasUnstableWindowRank(query *generator.SelectQuery) bool {
	return eetHasUnstableWindowBy(query, eetWindowRankUnstable)
}

func eetHasUnstableWindowAggregate(query *generator.SelectQuery) bool {
	return eetHasUnstableWindowBy(query, eetWindowAggregateUnstable)
}

func eetHasUnstableWindowBy(query *generator.SelectQuery, unstable func(generator.WindowExpr, map[string]generator.WindowDef) bool) bool {
	if query == nil {
		return false
	}
	windowDefs := make(map[string]generator.WindowDef, len(query.WindowDefs))
	for _, def := range query.WindowDefs {
		name := strings.ToLower(strings.TrimSpace(def.Name))
		if name == "" {
			continue
		}
		windowDefs[name] = def
	}
	for _, item := range query.Items {
		if eetExprHasUnstableWindowBy(item.Expr, windowDefs, unstable) {
			return true
		}
	}
	if query.Where != nil && eetExprHasUnstableWindowBy(query.Where, windowDefs, unstable) {
		return true
	}
	if query.Having != nil && eetExprHasUnstableWindowBy(query.Having, windowDefs, unstable) {
		return true
	}
	for _, expr := range query.GroupBy {
		if eetExprHasUnstableWindowBy(expr, windowDefs, unstable) {
			return true
		}
	}
	for _, ob := range query.OrderBy {
		if eetExprHasUnstableWindowBy(ob.Expr, windowDefs, unstable) {
			return true
		}
	}
	for _, join := range query.From.Joins {
		if join.On != nil && eetExprHasUnstableWindowBy(join.On, windowDefs, unstable) {
			return true
		}
	}
	return false
}

func eetExprHasUnstableWindowBy(expr generator.Expr, defs map[string]generator.WindowDef, unstable func(generator.WindowExpr, map[string]generator.WindowDef) bool) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case generator.WindowExpr:
		if unstable(e, defs) {
			return true
		}
		for _, arg := range e.Args {
			if eetExprHasUnstableWindowBy(arg, defs, unstable) {
				return true
			}
		}
		for _, part := range e.PartitionBy {
			if eetExprHasUnstableWindowBy(part, defs, unstable) {
				return true
			}
		}
		for _, ob := range e.OrderBy {
			if eetExprHasUnstableWindowBy(ob.Expr, defs, unstable) {
				return true
			}
		}
		return false
	case generator.UnaryExpr:
		return eetExprHasUnstableWindowBy(e.Expr, defs, unstable)
	case generator.BinaryExpr:
		return eetExprHasUnstableWindowBy(e.Left, defs, unstable) || eetExprHasUnstableWindowBy(e.Right, defs, unstable)
	case generator.FuncExpr:
		for _, arg := range e.Args {
			if eetExprHasUnstableWindowBy(arg, defs, unstable) {
				return true
			}
		}
		return false
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if eetExprHasUnstableWindowBy(w.When, defs, unstable) || eetExprHasUnstableWindowBy(w.Then, defs, unstable) {
				return true
			}
		}
		if e.Else != nil {
			return eetExprHasUnstableWindowBy(e.Else, defs, unstable)
		}
		return false
	case generator.InExpr:
		if eetExprHasUnstableWindowBy(e.Left, defs, unstable) {
			return true
		}
		for _, item := range e.List {
			if eetExprHasUnstableWindowBy(item, defs, unstable) {
				return true
			}
		}
		return false
	case generator.SubqueryExpr:
		return eetHasUnstableWindowBy(e.Query, unstable)
	case generator.ExistsExpr:
		return eetHasUnstableWindowBy(e.Query, unstable)
	case generator.GroupByOrdinalExpr:
		if e.Expr == nil {
			return false
		}
		return eetExprHasUnstableWindowBy(e.Expr, defs, unstable)
	default:
		return false
	}
}

func eetWindowRankUnstable(expr generator.WindowExpr, defs map[string]generator.WindowDef) bool {
	if !eetRankWindowFunction(expr.Name) {
		return false
	}
	partitionBy, orderBy := eetResolveWindowSpec(expr, defs)
	if len(orderBy) == 0 {
		return true
	}
	allConstant := true
	for _, ob := range orderBy {
		if !exprIsConstant(ob.Expr) {
			allConstant = false
			break
		}
	}
	if allConstant {
		return true
	}
	partitionKeys := make(map[string]struct{}, len(partitionBy))
	for _, expr := range partitionBy {
		key := eetExprKey(expr)
		if key == "" {
			continue
		}
		partitionKeys[key] = struct{}{}
	}
	if len(partitionKeys) == 0 {
		return false
	}
	for _, ob := range orderBy {
		key := eetExprKey(ob.Expr)
		if key == "" {
			return false
		}
		if _, ok := partitionKeys[key]; !ok {
			return false
		}
	}
	return true
}

func eetWindowAggregateUnstable(expr generator.WindowExpr, defs map[string]generator.WindowDef) bool {
	if !eetAggregateWindowFunction(expr.Name) {
		return false
	}
	partitionBy, orderBy := eetResolveWindowSpec(expr, defs)
	if len(orderBy) == 0 {
		return true
	}
	allConstant := true
	for _, ob := range orderBy {
		if !exprIsConstant(ob.Expr) {
			allConstant = false
			break
		}
	}
	if allConstant {
		return true
	}
	partitionKeys := make(map[string]struct{}, len(partitionBy))
	for _, expr := range partitionBy {
		key := eetExprKey(expr)
		if key == "" {
			continue
		}
		partitionKeys[key] = struct{}{}
	}
	if len(partitionKeys) == 0 {
		return false
	}
	for _, ob := range orderBy {
		key := eetExprKey(ob.Expr)
		if key == "" {
			return false
		}
		if _, ok := partitionKeys[key]; !ok {
			return false
		}
	}
	return true
}

func eetResolveWindowSpec(expr generator.WindowExpr, defs map[string]generator.WindowDef) (partitionBy []generator.Expr, orderBy []generator.OrderBy) {
	partitionBy = expr.PartitionBy
	orderBy = expr.OrderBy
	name := strings.ToLower(strings.TrimSpace(expr.WindowName))
	if name == "" {
		return partitionBy, orderBy
	}
	def, ok := defs[name]
	if !ok {
		return partitionBy, orderBy
	}
	if len(partitionBy) == 0 {
		partitionBy = def.PartitionBy
	}
	if len(orderBy) == 0 {
		orderBy = def.OrderBy
	}
	return partitionBy, orderBy
}

func eetRankWindowFunction(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "ROW_NUMBER", "RANK", "DENSE_RANK":
		return true
	default:
		return false
	}
}

func eetAggregateWindowFunction(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "SUM", "AVG", "COUNT", "MIN", "MAX", "BIT_AND", "BIT_OR", "BIT_XOR",
		"STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP", "VARIANCE":
		return true
	default:
		return false
	}
}

func eetExprKey(expr generator.Expr) string {
	if expr == nil {
		return ""
	}
	b := generator.SQLBuilder{}
	expr.Build(&b)
	return strings.ToLower(strings.TrimSpace(b.String()))
}

func hasUsingQualifiedRef(expr generator.Expr, usingCols map[string]struct{}) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case generator.ColumnExpr:
		if e.Ref.Table == "" {
			return false
		}
		_, ok := usingCols[e.Ref.Name]
		return ok
	case generator.UnaryExpr:
		return hasUsingQualifiedRef(e.Expr, usingCols)
	case generator.BinaryExpr:
		return hasUsingQualifiedRef(e.Left, usingCols) || hasUsingQualifiedRef(e.Right, usingCols)
	case generator.FuncExpr:
		for _, arg := range e.Args {
			if hasUsingQualifiedRef(arg, usingCols) {
				return true
			}
		}
		return false
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if hasUsingQualifiedRef(w.When, usingCols) || hasUsingQualifiedRef(w.Then, usingCols) {
				return true
			}
		}
		if e.Else != nil {
			return hasUsingQualifiedRef(e.Else, usingCols)
		}
		return false
	case generator.InExpr:
		if hasUsingQualifiedRef(e.Left, usingCols) {
			return true
		}
		for _, item := range e.List {
			if hasUsingQualifiedRef(item, usingCols) {
				return true
			}
		}
		return false
	case generator.SubqueryExpr:
		return queryHasUsingQualifiedRefs(e.Query)
	case generator.ExistsExpr:
		return queryHasUsingQualifiedRefs(e.Query)
	case generator.WindowExpr:
		for _, arg := range e.Args {
			if hasUsingQualifiedRef(arg, usingCols) {
				return true
			}
		}
		for _, part := range e.PartitionBy {
			if hasUsingQualifiedRef(part, usingCols) {
				return true
			}
		}
		for _, ob := range e.OrderBy {
			if hasUsingQualifiedRef(ob.Expr, usingCols) {
				return true
			}
		}
		return false
	case generator.GroupByOrdinalExpr:
		if e.Expr == nil {
			return false
		}
		return hasUsingQualifiedRef(e.Expr, usingCols)
	default:
		return false
	}
}

func orderByAllConstant(orderBy []generator.OrderBy, itemCount int) bool {
	if len(orderBy) == 0 {
		return false
	}
	for _, ob := range orderBy {
		if orderByOrdinal(ob.Expr, itemCount) {
			return false
		}
		if !exprIsConstant(ob.Expr) {
			return false
		}
	}
	return true
}

func orderByDistinctKeys(orderBy []generator.OrderBy, itemCount int) int {
	seen := make(map[string]struct{})
	for _, ob := range orderBy {
		if ordinal, ok := orderByOrdinalIndex(ob.Expr, itemCount); ok {
			seen[fmt.Sprintf("ord:%d", ordinal)] = struct{}{}
			continue
		}
		for _, col := range ob.Expr.Columns() {
			if col.Table == "" || col.Name == "" {
				continue
			}
			key := col.Table + "." + col.Name
			seen[key] = struct{}{}
		}
	}
	return len(seen)
}

func orderByOrdinal(expr generator.Expr, itemCount int) bool {
	_, ok := generator.OrderByOrdinalIndex(expr, itemCount)
	return ok
}

func orderByOrdinalIndex(expr generator.Expr, itemCount int) (int, bool) {
	return generator.OrderByOrdinalIndex(expr, itemCount)
}

func exprIsConstant(expr generator.Expr) bool {
	switch v := expr.(type) {
	case generator.LiteralExpr, generator.ParamExpr:
		return true
	case generator.ColumnExpr:
		return false
	case generator.SubqueryExpr, generator.ExistsExpr, generator.WindowExpr:
		return false
	case generator.UnaryExpr:
		return exprIsConstant(v.Expr)
	case generator.BinaryExpr:
		return exprIsConstant(v.Left) && exprIsConstant(v.Right)
	case generator.FuncExpr:
		for _, arg := range v.Args {
			if !exprIsConstant(arg) {
				return false
			}
		}
		return true
	case generator.CaseExpr:
		for _, w := range v.Whens {
			if !exprIsConstant(w.When) || !exprIsConstant(w.Then) {
				return false
			}
		}
		if v.Else != nil {
			return exprIsConstant(v.Else)
		}
		return true
	case generator.InExpr:
		if !exprIsConstant(v.Left) {
			return false
		}
		for _, item := range v.List {
			if !exprIsConstant(item) {
				return false
			}
		}
		return true
	default:
		return len(expr.Columns()) == 0
	}
}

func rewritePredicate(expr ast.ExprNode, kind eetRewriteKind) ast.ExprNode {
	if expr == nil {
		return nil
	}
	switch kind {
	case eetRewriteAndTrue:
		return &ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L:  expr,
			R:  ast.NewValueExpr(1, "", ""),
		}
	case eetRewriteOrFalse:
		return &ast.BinaryOperationExpr{
			Op: opcode.LogicOr,
			L:  expr,
			R:  ast.NewValueExpr(0, "", ""),
		}
	default:
		return &ast.UnaryOperationExpr{
			Op: opcode.Not,
			V: &ast.UnaryOperationExpr{
				Op: opcode.Not,
				V:  expr,
			},
		}
	}
}

func rewriteSelectPredicates(sel *ast.SelectStmt, gen *generator.Generator, resolver *columnTypeResolver) (eetRewriteKind, bool, string) {
	preferred := pickEETRewriteKind(sel, gen, resolver)
	if preferred == "" {
		return "", false, "eet:no_rewrite_kind"
	}
	kinds := []eetRewriteKind{
		preferred,
		eetRewriteDoubleNot,
		eetRewriteAndTrue,
		eetRewriteOrFalse,
		eetRewriteNumericIdentity,
		eetRewriteStringIdentity,
		eetRewriteDateIdentity,
	}
	seen := make(map[eetRewriteKind]struct{}, len(kinds))
	lastReason := "eet:no_transform"
	for _, kind := range kinds {
		if kind == "" {
			continue
		}
		if _, ok := seen[kind]; ok {
			continue
		}
		seen[kind] = struct{}{}
		if kind == eetRewriteDoubleNot || kind == eetRewriteAndTrue || kind == eetRewriteOrFalse {
			if rewriteBooleanPredicateInSelect(sel, kind, gen) {
				return kind, true, ""
			}
			lastReason = "eet:rewrite_no_boolean_target"
			continue
		}
		if rewriteLiteralPredicateInSelect(sel, kind, gen, resolver) {
			return kind, true, ""
		}
		lastReason = "eet:rewrite_no_literal_target"
	}
	return preferred, false, lastReason
}

func pickEETRewriteKind(sel *ast.SelectStmt, gen *generator.Generator, resolver *columnTypeResolver) eetRewriteKind {
	if sel == nil {
		return ""
	}
	if resolver == nil {
		resolver = buildColumnTypeResolver(sel, gen)
	}
	available := collectLiteralKinds(sel, resolver)
	if gen == nil {
		if available == 0 {
			return eetRewriteDoubleNot
		}
		switch {
		case available&literalNumeric != 0:
			return eetRewriteNumericIdentity
		case available&literalString != 0:
			return eetRewriteStringIdentity
		default:
			return eetRewriteDateIdentity
		}
	}

	weights := gen.Config.Oracles.EETRewrites
	candidates := []eetRewriteKind{
		eetRewriteDoubleNot,
		eetRewriteAndTrue,
		eetRewriteOrFalse,
		eetRewriteNumericIdentity,
		eetRewriteStringIdentity,
		eetRewriteDateIdentity,
	}
	weightValues := []int{
		weights.DoubleNot,
		weights.AndTrue,
		weights.OrFalse,
		weights.NumericIdentity,
		weights.StringIdentity,
		weights.DateIdentity,
	}
	if available&literalNumeric == 0 {
		weightValues[3] = 0
	}
	if available&literalString == 0 {
		weightValues[4] = 0
	}
	if available&literalDate == 0 {
		weightValues[5] = 0
	}
	if sumWeights(weightValues) == 0 {
		return pickBooleanRewrite(gen)
	}
	return candidates[util.PickWeighted(gen.Rand, weightValues)]
}

func rewriteBooleanPredicateInSelect(sel *ast.SelectStmt, kind eetRewriteKind, gen *generator.Generator) bool {
	targets := collectPredicateTargets(sel)
	if len(targets) == 0 {
		return false
	}
	var r *rand.Rand
	if gen != nil {
		r = gen.Rand
	}
	order := pickTargetOrder(targets, r)
	for _, idx := range order {
		target := targets[idx]
		target.set(rewritePredicate(target.get(), kind))
		return true
	}
	return false
}

func pickBooleanRewrite(gen *generator.Generator) eetRewriteKind {
	if gen == nil {
		return eetRewriteDoubleNot
	}
	weights := gen.Config.Oracles.EETRewrites
	candidates := []eetRewriteKind{eetRewriteDoubleNot, eetRewriteAndTrue, eetRewriteOrFalse}
	weightValues := []int{weights.DoubleNot, weights.AndTrue, weights.OrFalse}
	if sumWeights(weightValues) == 0 {
		return candidates[gen.Rand.Intn(len(candidates))]
	}
	return candidates[util.PickWeighted(gen.Rand, weightValues)]
}

func sumWeights(values []int) int {
	total := 0
	for _, v := range values {
		if v > 0 {
			total += v
		}
	}
	return total
}

func rewriteLiteralPredicateInSelect(sel *ast.SelectStmt, kind eetRewriteKind, gen *generator.Generator, resolver *columnTypeResolver) bool {
	targets := collectPredicateTargets(sel)
	if len(targets) == 0 {
		return false
	}
	var r *rand.Rand
	if gen != nil {
		r = gen.Rand
	}
	order := pickTargetOrder(targets, r)
	for _, idx := range order {
		target := targets[idx]
		if next, ok := rewriteLiteralInExpr(target.get(), kind); ok {
			target.set(next)
			return true
		}
		if next, ok := rewriteColumnIdentityInExpr(target.get(), kind, resolver); ok {
			target.set(next)
			return true
		}
	}
	return false
}

func restoreEETSQL(node ast.Node) (string, error) {
	var b strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &b)
	if err := node.Restore(ctx); err != nil {
		return "", err
	}
	return b.String(), nil
}

func signatureSQLFor(sqlText string, aliases []string) string {
	cols := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		cols = append(cols, fmt.Sprintf("q.%s", alias))
	}
	if len(cols) == 0 {
		return fmt.Sprintf("SELECT COUNT(*) AS cnt, 0 AS checksum FROM (%s) q", sqlText)
	}
	checksumExpr := fmt.Sprintf("IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0)", strings.Join(cols, ", "))
	return fmt.Sprintf("SELECT COUNT(*) AS cnt, %s AS checksum FROM (%s) q", checksumExpr, sqlText)
}

type literalKind int

const (
	literalNumeric literalKind = 1 << iota
	literalString
	literalDate
)

func collectLiteralKinds(sel *ast.SelectStmt, resolver *columnTypeResolver) literalKind {
	var kinds literalKind
	if sel == nil {
		return kinds
	}
	kinds |= collectLiteralKindsExpr(sel.Where, resolver)
	if sel.Having != nil && sel.Having.Expr != nil {
		kinds |= collectLiteralKindsExpr(sel.Having.Expr, resolver)
	}
	kinds |= collectLiteralKindsJoin(sel.From, resolver)
	return kinds
}

func collectLiteralKindsJoin(from *ast.TableRefsClause, resolver *columnTypeResolver) literalKind {
	if from == nil || from.TableRefs == nil {
		return 0
	}
	return collectLiteralKindsResultSet(from.TableRefs, resolver)
}

func collectLiteralKindsResultSet(node ast.ResultSetNode, resolver *columnTypeResolver) literalKind {
	switch v := node.(type) {
	case *ast.Join:
		kinds := collectLiteralKindsResultSet(v.Left, resolver)
		kinds |= collectLiteralKindsResultSet(v.Right, resolver)
		if v.On != nil && v.On.Expr != nil {
			kinds |= collectLiteralKindsExpr(v.On.Expr, resolver)
		}
		return kinds
	case *ast.TableSource:
		switch src := v.Source.(type) {
		case *ast.Join:
			return collectLiteralKindsResultSet(src, resolver)
		case *ast.SelectStmt:
			return collectLiteralKindsSelect(src, resolver)
		case *ast.SetOprStmt:
			return collectLiteralKindsSetOpr(src, resolver)
		}
	}
	return 0
}

func collectLiteralKindsSelect(sel *ast.SelectStmt, resolver *columnTypeResolver) literalKind {
	if sel == nil {
		return 0
	}
	kinds := collectLiteralKindsExpr(sel.Where, resolver)
	if sel.Having != nil && sel.Having.Expr != nil {
		kinds |= collectLiteralKindsExpr(sel.Having.Expr, resolver)
	}
	kinds |= collectLiteralKindsJoin(sel.From, resolver)
	return kinds
}

func collectLiteralKindsSetOpr(stmt *ast.SetOprStmt, resolver *columnTypeResolver) literalKind {
	if stmt == nil || stmt.SelectList == nil {
		return 0
	}
	var kinds literalKind
	for _, sel := range stmt.SelectList.Selects {
		switch v := sel.(type) {
		case *ast.SelectStmt:
			kinds |= collectLiteralKindsSelect(v, resolver)
		case *ast.SetOprStmt:
			kinds |= collectLiteralKindsSetOpr(v, resolver)
		}
	}
	return kinds
}

func collectLiteralKindsExpr(expr ast.ExprNode, resolver *columnTypeResolver) literalKind {
	switch e := expr.(type) {
	case *ast.ColumnNameExpr:
		return literalKindForColumn(e, resolver)
	case ast.ValueExpr:
		return literalKindsForValue(e)
	case *ast.BinaryOperationExpr:
		return collectLiteralKindsExpr(e.L, resolver) | collectLiteralKindsExpr(e.R, resolver)
	case *ast.UnaryOperationExpr:
		return collectLiteralKindsExpr(e.V, resolver)
	case *ast.PatternInExpr:
		kinds := collectLiteralKindsExpr(e.Expr, resolver)
		for _, item := range e.List {
			kinds |= collectLiteralKindsExpr(item, resolver)
		}
		return kinds
	case *ast.BetweenExpr:
		kinds := collectLiteralKindsExpr(e.Expr, resolver)
		kinds |= collectLiteralKindsExpr(e.Left, resolver)
		kinds |= collectLiteralKindsExpr(e.Right, resolver)
		return kinds
	case *ast.PatternLikeOrIlikeExpr:
		kinds := collectLiteralKindsExpr(e.Expr, resolver)
		kinds |= collectLiteralKindsExpr(e.Pattern, resolver)
		return kinds
	case *ast.IsNullExpr:
		return collectLiteralKindsExpr(e.Expr, resolver)
	case *ast.FuncCallExpr:
		var kinds literalKind
		for _, arg := range e.Args {
			kinds |= collectLiteralKindsExpr(arg, resolver)
		}
		return kinds
	case *ast.CaseExpr:
		var kinds literalKind
		if e.Value != nil {
			kinds |= collectLiteralKindsExpr(e.Value, resolver)
		}
		for _, w := range e.WhenClauses {
			kinds |= collectLiteralKindsExpr(w.Expr, resolver)
			kinds |= collectLiteralKindsExpr(w.Result, resolver)
		}
		if e.ElseClause != nil {
			kinds |= collectLiteralKindsExpr(e.ElseClause, resolver)
		}
		return kinds
	default:
		return 0
	}
}

func literalKindsForValue(v ast.ValueExpr) literalKind {
	switch val := v.GetValue().(type) {
	case int, int8, int16, int32, int64:
		return literalNumeric
	case uint, uint8, uint16, uint32, uint64:
		return literalNumeric
	case float32, float64:
		return literalNumeric
	case string:
		return classifyStringLiteral(val)
	case []byte:
		return literalString
	default:
		if val == nil {
			return 0
		}
		switch val.(type) {
		case fmt.Stringer:
			return literalString
		default:
			return 0
		}
	}
}

func classifyStringLiteral(val string) literalKind {
	if isDateLiteralString(val) || isDateTimeLiteralString(val) {
		return literalDate
	}
	return literalString
}

type columnTypeResolver struct {
	state   *schema.State
	aliases map[string]string
}

func buildColumnTypeResolver(sel *ast.SelectStmt, gen *generator.Generator) *columnTypeResolver {
	if gen == nil || gen.State == nil || sel == nil {
		return nil
	}
	aliases := buildTableAliasMap(sel.From)
	if len(aliases) == 0 {
		aliases = nil
	}
	return &columnTypeResolver{
		state:   gen.State,
		aliases: aliases,
	}
}

func buildTableAliasMap(from *ast.TableRefsClause) map[string]string {
	if from == nil || from.TableRefs == nil {
		return nil
	}
	aliases := make(map[string]string)
	addAlias := func(alias, name string) {
		if alias == "" || name == "" {
			return
		}
		aliases[alias] = name
	}
	var walk func(node ast.ResultSetNode)
	walk = func(node ast.ResultSetNode) {
		switch v := node.(type) {
		case *ast.Join:
			walk(v.Left)
			walk(v.Right)
		case *ast.TableSource:
			switch src := v.Source.(type) {
			case *ast.TableName:
				name := src.Name.O
				alias := v.AsName.O
				if alias == "" {
					alias = name
				}
				addAlias(alias, name)
			case *ast.Join:
				walk(src)
			case *ast.SelectStmt, *ast.SetOprStmt:
				if v.AsName.O != "" {
					addAlias(v.AsName.O, "")
				}
			}
		}
	}
	walk(from.TableRefs)
	return aliases
}

func literalKindForColumn(col *ast.ColumnNameExpr, resolver *columnTypeResolver) literalKind {
	if col == nil || resolver == nil || resolver.state == nil {
		return 0
	}
	typ, ok := resolver.columnType(col)
	if !ok {
		return 0
	}
	switch typ {
	case schema.TypeInt, schema.TypeBigInt, schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal, schema.TypeBool:
		return literalNumeric
	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return literalDate
	default:
		return literalString
	}
}

func (r *columnTypeResolver) columnType(col *ast.ColumnNameExpr) (schema.ColumnType, bool) {
	if col == nil || r == nil || r.state == nil {
		return 0, false
	}
	table := col.Name.Table.O
	name := col.Name.Name.O
	if name == "" {
		return 0, false
	}
	if table != "" && r.aliases != nil {
		if mapped, ok := r.aliases[table]; ok && mapped != "" {
			table = mapped
		}
	}
	if table == "" {
		return 0, false
	}
	tbl, ok := r.state.TableByName(table)
	if !ok {
		return 0, false
	}
	colDef, ok := tbl.ColumnByName(name)
	if !ok {
		return 0, false
	}
	return colDef.Type, true
}

func isDateLiteralString(value string) bool {
	if len(value) != 10 {
		return false
	}
	for i, ch := range value {
		switch i {
		case 4, 7:
			if ch != '-' {
				return false
			}
		default:
			if ch < '0' || ch > '9' {
				return false
			}
		}
	}
	return true
}

func isDateTimeLiteralString(value string) bool {
	if len(value) != 19 {
		return false
	}
	for i, ch := range value {
		switch i {
		case 4, 7:
			if ch != '-' {
				return false
			}
		case 10:
			if ch != ' ' {
				return false
			}
		case 13, 16:
			if ch != ':' {
				return false
			}
		default:
			if ch < '0' || ch > '9' {
				return false
			}
		}
	}
	return true
}

func rewriteLiteralInExpr(expr ast.ExprNode, kind eetRewriteKind) (ast.ExprNode, bool) {
	switch e := expr.(type) {
	case ast.ValueExpr:
		return rewriteLiteralValue(e, kind)
	case *ast.BinaryOperationExpr:
		if next, ok := rewriteLiteralInExpr(e.L, kind); ok {
			e.L = next
			return e, true
		}
		if next, ok := rewriteLiteralInExpr(e.R, kind); ok {
			e.R = next
			return e, true
		}
		return e, false
	case *ast.UnaryOperationExpr:
		if next, ok := rewriteLiteralInExpr(e.V, kind); ok {
			e.V = next
			return e, true
		}
		return e, false
	case *ast.PatternInExpr:
		if next, ok := rewriteLiteralInExpr(e.Expr, kind); ok {
			e.Expr = next
			return e, true
		}
		for i, item := range e.List {
			if next, ok := rewriteLiteralInExpr(item, kind); ok {
				e.List[i] = next
				return e, true
			}
		}
		return e, false
	case *ast.BetweenExpr:
		if next, ok := rewriteLiteralInExpr(e.Expr, kind); ok {
			e.Expr = next
			return e, true
		}
		if next, ok := rewriteLiteralInExpr(e.Left, kind); ok {
			e.Left = next
			return e, true
		}
		if next, ok := rewriteLiteralInExpr(e.Right, kind); ok {
			e.Right = next
			return e, true
		}
		return e, false
	case *ast.PatternLikeOrIlikeExpr:
		if next, ok := rewriteLiteralInExpr(e.Expr, kind); ok {
			e.Expr = next
			return e, true
		}
		if next, ok := rewriteLiteralInExpr(e.Pattern, kind); ok {
			e.Pattern = next
			return e, true
		}
		return e, false
	case *ast.IsNullExpr:
		if next, ok := rewriteLiteralInExpr(e.Expr, kind); ok {
			e.Expr = next
			return e, true
		}
		return e, false
	case *ast.FuncCallExpr:
		for i, arg := range e.Args {
			if next, ok := rewriteLiteralInExpr(arg, kind); ok {
				e.Args[i] = next
				return e, true
			}
		}
		return e, false
	case *ast.CaseExpr:
		if e.Value != nil {
			if next, ok := rewriteLiteralInExpr(e.Value, kind); ok {
				e.Value = next
				return e, true
			}
		}
		for i, w := range e.WhenClauses {
			if next, ok := rewriteLiteralInExpr(w.Expr, kind); ok {
				e.WhenClauses[i].Expr = next
				return e, true
			}
			if next, ok := rewriteLiteralInExpr(w.Result, kind); ok {
				e.WhenClauses[i].Result = next
				return e, true
			}
		}
		if e.ElseClause != nil {
			if next, ok := rewriteLiteralInExpr(e.ElseClause, kind); ok {
				e.ElseClause = next
				return e, true
			}
		}
		return e, false
	default:
		return e, false
	}
}

func rewriteLiteralValue(expr ast.ValueExpr, kind eetRewriteKind) (ast.ExprNode, bool) {
	switch kind {
	case eetRewriteNumericIdentity:
		if literalKindsForValue(expr)&literalNumeric == 0 {
			return expr, false
		}
		return &ast.BinaryOperationExpr{
			Op: opcode.Plus,
			L:  expr,
			R:  ast.NewValueExpr(0, "", ""),
		}, true
	case eetRewriteStringIdentity:
		if literalKindsForValue(expr)&literalString == 0 {
			return expr, false
		}
		return &ast.FuncCallExpr{
			Tp:     ast.FuncCallExprTypeGeneric,
			FnName: ast.NewCIStr("CONCAT"),
			Args: []ast.ExprNode{
				expr,
				ast.NewValueExpr("", "", ""),
			},
		}, true
	case eetRewriteDateIdentity:
		if literalKindsForValue(expr)&literalDate == 0 {
			return expr, false
		}
		return &ast.FuncCallExpr{
			Tp:     ast.FuncCallExprTypeKeyword,
			FnName: ast.NewCIStr("DATE_ADD"),
			Args: []ast.ExprNode{
				expr,
				ast.NewValueExpr(0, "", ""),
				&ast.TimeUnitExpr{Unit: ast.TimeUnitDay},
			},
		}, true
	default:
		return expr, false
	}
}

func rewriteColumnIdentityInExpr(expr ast.ExprNode, kind eetRewriteKind, resolver *columnTypeResolver) (ast.ExprNode, bool) {
	switch e := expr.(type) {
	case *ast.ColumnNameExpr:
		if literalKindForColumn(e, resolver)&kindMask(kind) == 0 {
			return e, false
		}
		return rewriteColumnIdentity(e, kind)
	case *ast.BinaryOperationExpr:
		if next, ok := rewriteColumnIdentityInExpr(e.L, kind, resolver); ok {
			e.L = next
			return e, true
		}
		if next, ok := rewriteColumnIdentityInExpr(e.R, kind, resolver); ok {
			e.R = next
			return e, true
		}
		return e, false
	case *ast.UnaryOperationExpr:
		if next, ok := rewriteColumnIdentityInExpr(e.V, kind, resolver); ok {
			e.V = next
			return e, true
		}
		return e, false
	case *ast.PatternInExpr:
		if next, ok := rewriteColumnIdentityInExpr(e.Expr, kind, resolver); ok {
			e.Expr = next
			return e, true
		}
		for i, item := range e.List {
			if next, ok := rewriteColumnIdentityInExpr(item, kind, resolver); ok {
				e.List[i] = next
				return e, true
			}
		}
		return e, false
	case *ast.BetweenExpr:
		if next, ok := rewriteColumnIdentityInExpr(e.Expr, kind, resolver); ok {
			e.Expr = next
			return e, true
		}
		if next, ok := rewriteColumnIdentityInExpr(e.Left, kind, resolver); ok {
			e.Left = next
			return e, true
		}
		if next, ok := rewriteColumnIdentityInExpr(e.Right, kind, resolver); ok {
			e.Right = next
			return e, true
		}
		return e, false
	case *ast.PatternLikeOrIlikeExpr:
		if next, ok := rewriteColumnIdentityInExpr(e.Expr, kind, resolver); ok {
			e.Expr = next
			return e, true
		}
		if next, ok := rewriteColumnIdentityInExpr(e.Pattern, kind, resolver); ok {
			e.Pattern = next
			return e, true
		}
		return e, false
	case *ast.IsNullExpr:
		if next, ok := rewriteColumnIdentityInExpr(e.Expr, kind, resolver); ok {
			e.Expr = next
			return e, true
		}
		return e, false
	case *ast.FuncCallExpr:
		for i, arg := range e.Args {
			if next, ok := rewriteColumnIdentityInExpr(arg, kind, resolver); ok {
				e.Args[i] = next
				return e, true
			}
		}
		return e, false
	case *ast.CaseExpr:
		if e.Value != nil {
			if next, ok := rewriteColumnIdentityInExpr(e.Value, kind, resolver); ok {
				e.Value = next
				return e, true
			}
		}
		for i, w := range e.WhenClauses {
			if next, ok := rewriteColumnIdentityInExpr(w.Expr, kind, resolver); ok {
				e.WhenClauses[i].Expr = next
				return e, true
			}
			if next, ok := rewriteColumnIdentityInExpr(w.Result, kind, resolver); ok {
				e.WhenClauses[i].Result = next
				return e, true
			}
		}
		if e.ElseClause != nil {
			if next, ok := rewriteColumnIdentityInExpr(e.ElseClause, kind, resolver); ok {
				e.ElseClause = next
				return e, true
			}
		}
		return e, false
	default:
		return e, false
	}
}

func kindMask(kind eetRewriteKind) literalKind {
	switch kind {
	case eetRewriteNumericIdentity:
		return literalNumeric
	case eetRewriteStringIdentity:
		return literalString
	case eetRewriteDateIdentity:
		return literalDate
	default:
		return 0
	}
}

func rewriteColumnIdentity(col *ast.ColumnNameExpr, kind eetRewriteKind) (ast.ExprNode, bool) {
	switch kind {
	case eetRewriteNumericIdentity:
		return &ast.BinaryOperationExpr{
			Op: opcode.Plus,
			L:  col,
			R:  ast.NewValueExpr(0, "", ""),
		}, true
	case eetRewriteStringIdentity:
		return &ast.FuncCallExpr{
			Tp:     ast.FuncCallExprTypeGeneric,
			FnName: ast.NewCIStr("CONCAT"),
			Args: []ast.ExprNode{
				col,
				ast.NewValueExpr("", "", ""),
			},
		}, true
	case eetRewriteDateIdentity:
		return &ast.FuncCallExpr{
			Tp:     ast.FuncCallExprTypeKeyword,
			FnName: ast.NewCIStr("DATE_ADD"),
			Args: []ast.ExprNode{
				col,
				ast.NewValueExpr(0, "", ""),
				&ast.TimeUnitExpr{Unit: ast.TimeUnitDay},
			},
		}, true
	default:
		return col, false
	}
}

type predicateTarget struct {
	get func() ast.ExprNode
	set func(ast.ExprNode)
}

func collectPredicateTargets(sel *ast.SelectStmt) []predicateTarget {
	var targets []predicateTarget
	if sel == nil {
		return targets
	}
	if sel.Where != nil {
		targets = append(targets, predicateTarget{
			get: func() ast.ExprNode { return sel.Where },
			set: func(expr ast.ExprNode) { sel.Where = expr },
		})
	}
	if sel.Having != nil && sel.Having.Expr != nil {
		targets = append(targets, predicateTarget{
			get: func() ast.ExprNode { return sel.Having.Expr },
			set: func(expr ast.ExprNode) { sel.Having.Expr = expr },
		})
	}
	targets = append(targets, collectJoinTargets(sel.From)...)
	return targets
}

func collectJoinTargets(from *ast.TableRefsClause) []predicateTarget {
	if from == nil || from.TableRefs == nil {
		return nil
	}
	return collectJoinTargetsResultSet(from.TableRefs)
}

func collectJoinTargetsResultSet(node ast.ResultSetNode) []predicateTarget {
	switch v := node.(type) {
	case *ast.Join:
		var targets []predicateTarget
		if v.On != nil && v.On.Expr != nil {
			on := v.On
			targets = append(targets, predicateTarget{
				get: func() ast.ExprNode { return on.Expr },
				set: func(expr ast.ExprNode) { on.Expr = expr },
			})
		}
		targets = append(targets, collectJoinTargetsResultSet(v.Left)...)
		if v.Right != nil {
			targets = append(targets, collectJoinTargetsResultSet(v.Right)...)
		}
		return targets
	case *ast.TableSource:
		switch src := v.Source.(type) {
		case *ast.Join:
			return collectJoinTargetsResultSet(src)
		case *ast.SelectStmt:
			return collectPredicateTargets(src)
		case *ast.SetOprStmt:
			return collectSetOprTargets(src)
		default:
			return nil
		}
	default:
		return nil
	}
}

func collectSetOprTargets(stmt *ast.SetOprStmt) []predicateTarget {
	if stmt == nil || stmt.SelectList == nil {
		return nil
	}
	var targets []predicateTarget
	for _, sel := range stmt.SelectList.Selects {
		switch v := sel.(type) {
		case *ast.SelectStmt:
			targets = append(targets, collectPredicateTargets(v)...)
		case *ast.SetOprStmt:
			targets = append(targets, collectSetOprTargets(v)...)
		}
	}
	return targets
}

func pickTargetOrder(targets []predicateTarget, r *rand.Rand) []int {
	count := len(targets)
	order := make([]int, count)
	for i := 0; i < count; i++ {
		order[i] = i
	}
	if count <= 1 || r == nil {
		return order
	}
	for i := count - 1; i > 0; i-- {
		j := r.Intn(i + 1)
		order[i], order[j] = order[j], order[i]
	}
	return order
}

func selectHasPredicate(sel *ast.SelectStmt) bool {
	if sel == nil {
		return false
	}
	if sel.Where != nil {
		return true
	}
	if sel.Having != nil && sel.Having.Expr != nil {
		return true
	}
	if sel.From != nil && sel.From.TableRefs != nil {
		return joinHasOn(sel.From.TableRefs)
	}
	return false
}

func joinHasOn(node ast.ResultSetNode) bool {
	switch v := node.(type) {
	case *ast.Join:
		if v.On != nil && v.On.Expr != nil {
			return true
		}
		if joinHasOn(v.Left) {
			return true
		}
		if v.Right != nil && joinHasOn(v.Right) {
			return true
		}
		return false
	case *ast.TableSource:
		switch src := v.Source.(type) {
		case *ast.Join:
			return joinHasOn(src)
		case *ast.SelectStmt:
			return selectHasPredicate(src)
		case *ast.SetOprStmt:
			return setOprHasPredicate(src)
		}
		return false
	default:
		return false
	}
}

func setOprHasPredicate(stmt *ast.SetOprStmt) bool {
	if stmt == nil || stmt.SelectList == nil {
		return false
	}
	for _, sel := range stmt.SelectList.Selects {
		switch v := sel.(type) {
		case *ast.SelectStmt:
			if selectHasPredicate(v) {
				return true
			}
		case *ast.SetOprStmt:
			if setOprHasPredicate(v) {
				return true
			}
		}
	}
	return false
}

func eetQueryHasPredicate(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	if query.Where != nil {
		return true
	}
	if query.Having != nil {
		return true
	}
	for _, join := range query.From.Joins {
		if join.On != nil {
			return true
		}
	}
	return false
}

func queryHasPredicateMatch(query *generator.SelectQuery, policy predicatePolicy) bool {
	if query == nil {
		return false
	}
	if query.Where != nil && predicateMatches(query.Where, policy) {
		return true
	}
	if query.Having != nil && predicateMatches(query.Having, policy) {
		return true
	}
	for _, join := range query.From.Joins {
		if join.On != nil && predicateMatches(join.On, policy) {
			return true
		}
	}
	return false
}
