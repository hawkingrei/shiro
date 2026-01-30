package oracle

import (
	"context"
	"fmt"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/test_driver"

	"shiro/internal/util"
)

// EET implements the Equivalent Expression Transformation oracle.
//
// It rewrites predicates using semantics-preserving AST transformations and
// checks that the transformed query returns the same signature.
type EET struct{}

// Name returns the oracle identifier.
func (o EET) Name() string { return "EET" }

// Run builds a query, applies an equivalent predicate rewrite, and compares
// signatures for mismatches.
func (o EET) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, _ *schema.State) Result {
	policy := predicatePolicyFor(gen)
	builder := generator.NewSelectQueryBuilder(gen).
		RequireDeterministic().
		DisallowSubquery().
		PredicateGuard(func(expr generator.Expr) bool {
			return predicateMatches(expr, policy)
		}).
		QueryGuard(func(query *generator.SelectQuery) bool {
			return eetQueryHasPredicate(query)
		})

	query, reason, attempts := builder.BuildWithReason()
	if query == nil {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": builderSkipReason("eet", reason), "builder_reason": reason, "builder_attempts": attempts}}
	}
	if !queryDeterministic(query) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:nondeterministic"}}
	}
	if query.Where != nil && !predicateMatches(query.Where, policy) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:predicate_guard"}}
	}

	baseSQL := query.SQLString()
	transformedSQL, details, err := applyEETTransform(baseSQL, gen)
	if err != nil {
		details["error_reason"] = "eet:parse_error"
		return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Err: err, Details: details}
	}
	if strings.TrimSpace(transformedSQL) == "" || transformedSQL == baseSQL {
		details["skip_reason"] = "eet:no_transform"
		return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Details: details}
	}

	origSig, err := exec.QuerySignature(ctx, query.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Err: err, Details: map[string]any{"error_reason": "eet:base_signature_error"}}
	}
	transformedSig, err := exec.QuerySignature(ctx, signatureSQLFor(transformedSQL, query.ColumnAliases()))
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{transformedSQL}, Err: err, Details: map[string]any{"error_reason": "eet:transform_signature_error"}}
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
		details["skip_reason"] = "eet:non_select"
		return "", details, nil
	}
	if !selectHasPredicate(sel) {
		details["skip_reason"] = "eet:no_predicate"
		return "", details, nil
	}
	kind, changed := rewriteSelectPredicates(sel, gen)
	if !changed {
		details["skip_reason"] = "eet:no_transform"
		return "", details, nil
	}
	details["rewrite"] = string(kind)
	return restoreEETSQL(sel), details, nil
}

func pickEETRewrite(gen *generator.Generator) eetRewriteKind {
	if gen == nil {
		return eetRewriteDoubleNot
	}
	switch gen.Rand.Intn(3) {
	case 0:
		return eetRewriteDoubleNot
	case 1:
		return eetRewriteAndTrue
	default:
		return eetRewriteOrFalse
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

func rewriteSelectPredicates(sel *ast.SelectStmt, gen *generator.Generator) (eetRewriteKind, bool) {
	kind := pickEETRewriteKind(sel, gen)
	if kind == "" {
		return "", false
	}
	if kind == eetRewriteDoubleNot || kind == eetRewriteAndTrue || kind == eetRewriteOrFalse {
		if rewriteBooleanPredicateInSelect(sel, kind) {
			return kind, true
		}
		return "", false
	}
	if rewriteLiteralPredicateInSelect(sel, kind) {
		return kind, true
	}
	return "", false
}

func pickEETRewriteKind(sel *ast.SelectStmt, gen *generator.Generator) eetRewriteKind {
	if sel == nil {
		return ""
	}
	available := collectLiteralKinds(sel)
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

func rewriteBooleanPredicateInSelect(sel *ast.SelectStmt, kind eetRewriteKind) bool {
	if sel.Where != nil {
		sel.Where = rewritePredicate(sel.Where, kind)
		return true
	}
	if sel.Having != nil && sel.Having.Expr != nil {
		sel.Having.Expr = rewritePredicate(sel.Having.Expr, kind)
		return true
	}
	return rewriteJoinOn(sel.From, func(expr ast.ExprNode) (ast.ExprNode, bool) {
		return rewritePredicate(expr, kind), true
	})
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

func rewriteLiteralPredicateInSelect(sel *ast.SelectStmt, kind eetRewriteKind) bool {
	if sel.Where != nil {
		if next, ok := rewriteLiteralInExpr(sel.Where, kind); ok {
			sel.Where = next
			return true
		}
	}
	if sel.Having != nil && sel.Having.Expr != nil {
		if next, ok := rewriteLiteralInExpr(sel.Having.Expr, kind); ok {
			sel.Having.Expr = next
			return true
		}
	}
	return rewriteJoinOn(sel.From, func(expr ast.ExprNode) (ast.ExprNode, bool) {
		return rewriteLiteralInExpr(expr, kind)
	})
}

func restoreEETSQL(node ast.Node) string {
	var b strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &b)
	if err := node.Restore(ctx); err != nil {
		return ""
	}
	return b.String()
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

func collectLiteralKinds(sel *ast.SelectStmt) literalKind {
	var kinds literalKind
	if sel == nil {
		return kinds
	}
	kinds |= collectLiteralKindsExpr(sel.Where)
	if sel.Having != nil && sel.Having.Expr != nil {
		kinds |= collectLiteralKindsExpr(sel.Having.Expr)
	}
	kinds |= collectLiteralKindsJoin(sel.From)
	return kinds
}

func collectLiteralKindsJoin(from *ast.TableRefsClause) literalKind {
	if from == nil || from.TableRefs == nil {
		return 0
	}
	return collectLiteralKindsResultSet(from.TableRefs)
}

func collectLiteralKindsResultSet(node ast.ResultSetNode) literalKind {
	switch v := node.(type) {
	case *ast.Join:
		kinds := collectLiteralKindsResultSet(v.Left)
		kinds |= collectLiteralKindsResultSet(v.Right)
		if v.On != nil && v.On.Expr != nil {
			kinds |= collectLiteralKindsExpr(v.On.Expr)
		}
		return kinds
	case *ast.TableSource:
		switch src := v.Source.(type) {
		case *ast.Join:
			return collectLiteralKindsResultSet(src)
		case *ast.SelectStmt:
			return collectLiteralKindsSelect(src)
		case *ast.SetOprStmt:
			return collectLiteralKindsSetOpr(src)
		}
	}
	return 0
}

func collectLiteralKindsSelect(sel *ast.SelectStmt) literalKind {
	if sel == nil {
		return 0
	}
	kinds := collectLiteralKindsExpr(sel.Where)
	if sel.Having != nil && sel.Having.Expr != nil {
		kinds |= collectLiteralKindsExpr(sel.Having.Expr)
	}
	kinds |= collectLiteralKindsJoin(sel.From)
	return kinds
}

func collectLiteralKindsSetOpr(stmt *ast.SetOprStmt) literalKind {
	if stmt == nil || stmt.SelectList == nil {
		return 0
	}
	var kinds literalKind
	for _, sel := range stmt.SelectList.Selects {
		switch v := sel.(type) {
		case *ast.SelectStmt:
			kinds |= collectLiteralKindsSelect(v)
		case *ast.SetOprStmt:
			kinds |= collectLiteralKindsSetOpr(v)
		}
	}
	return kinds
}

func collectLiteralKindsExpr(expr ast.ExprNode) literalKind {
	switch e := expr.(type) {
	case *test_driver.ValueExpr:
		return literalKindsForValue(e)
	case *ast.BinaryOperationExpr:
		return collectLiteralKindsExpr(e.L) | collectLiteralKindsExpr(e.R)
	case *ast.UnaryOperationExpr:
		return collectLiteralKindsExpr(e.V)
	case *ast.PatternInExpr:
		kinds := collectLiteralKindsExpr(e.Expr)
		for _, item := range e.List {
			kinds |= collectLiteralKindsExpr(item)
		}
		return kinds
	case *ast.BetweenExpr:
		kinds := collectLiteralKindsExpr(e.Expr)
		kinds |= collectLiteralKindsExpr(e.Left)
		kinds |= collectLiteralKindsExpr(e.Right)
		return kinds
	case *ast.PatternLikeOrIlikeExpr:
		kinds := collectLiteralKindsExpr(e.Expr)
		kinds |= collectLiteralKindsExpr(e.Pattern)
		return kinds
	case *ast.IsNullExpr:
		return collectLiteralKindsExpr(e.Expr)
	case *ast.FuncCallExpr:
		var kinds literalKind
		for _, arg := range e.Args {
			kinds |= collectLiteralKindsExpr(arg)
		}
		return kinds
	case *ast.CaseExpr:
		var kinds literalKind
		if e.Value != nil {
			kinds |= collectLiteralKindsExpr(e.Value)
		}
		for _, w := range e.WhenClauses {
			kinds |= collectLiteralKindsExpr(w.Expr)
			kinds |= collectLiteralKindsExpr(w.Result)
		}
		if e.ElseClause != nil {
			kinds |= collectLiteralKindsExpr(e.ElseClause)
		}
		return kinds
	default:
		return 0
	}
}

func literalKindsForValue(v *test_driver.ValueExpr) literalKind {
	switch v.Kind() {
	case test_driver.KindInt64, test_driver.KindUint64, test_driver.KindFloat32, test_driver.KindFloat64, test_driver.KindMysqlDecimal:
		return literalNumeric
	case test_driver.KindMysqlTime, test_driver.KindMysqlDuration:
		return literalDate
	case test_driver.KindString, test_driver.KindBytes:
		return literalString
	default:
		return 0
	}
}

func rewriteLiteralInExpr(expr ast.ExprNode, kind eetRewriteKind) (ast.ExprNode, bool) {
	switch e := expr.(type) {
	case *test_driver.ValueExpr:
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

func rewriteLiteralValue(expr *test_driver.ValueExpr, kind eetRewriteKind) (ast.ExprNode, bool) {
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
			FnName: ast.NewCIStr("ADDDATE"),
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

func rewriteJoinOn(from *ast.TableRefsClause, rewrite func(ast.ExprNode) (ast.ExprNode, bool)) bool {
	if from == nil || from.TableRefs == nil {
		return false
	}
	return rewriteJoinOnResultSet(from.TableRefs, rewrite)
}

func rewriteJoinOnResultSet(node ast.ResultSetNode, rewrite func(ast.ExprNode) (ast.ExprNode, bool)) bool {
	switch v := node.(type) {
	case *ast.Join:
		if v.On != nil && v.On.Expr != nil {
			if next, ok := rewrite(v.On.Expr); ok {
				v.On.Expr = next
				return true
			}
		}
		if rewriteJoinOnResultSet(v.Left, rewrite) {
			return true
		}
		if v.Right != nil {
			return rewriteJoinOnResultSet(v.Right, rewrite)
		}
		return false
	case *ast.TableSource:
		switch src := v.Source.(type) {
		case *ast.Join:
			return rewriteJoinOnResultSet(src, rewrite)
		case *ast.SelectStmt:
			return rewriteSelectPredicatesNested(src, rewrite)
		case *ast.SetOprStmt:
			return rewriteSetOprPredicates(src, rewrite)
		}
		return false
	default:
		return false
	}
}

func rewriteSelectPredicatesNested(sel *ast.SelectStmt, rewrite func(ast.ExprNode) (ast.ExprNode, bool)) bool {
	if sel == nil {
		return false
	}
	if sel.Where != nil {
		if next, ok := rewrite(sel.Where); ok {
			sel.Where = next
			return true
		}
	}
	if sel.Having != nil && sel.Having.Expr != nil {
		if next, ok := rewrite(sel.Having.Expr); ok {
			sel.Having.Expr = next
			return true
		}
	}
	return rewriteJoinOn(sel.From, rewrite)
}

func rewriteSetOprPredicates(stmt *ast.SetOprStmt, rewrite func(ast.ExprNode) (ast.ExprNode, bool)) bool {
	if stmt == nil || stmt.SelectList == nil {
		return false
	}
	for _, sel := range stmt.SelectList.Selects {
		switch v := sel.(type) {
		case *ast.SelectStmt:
			if rewriteSelectPredicatesNested(v, rewrite) {
				return true
			}
		case *ast.SetOprStmt:
			if rewriteSetOprPredicates(v, rewrite) {
				return true
			}
		}
	}
	return false
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
