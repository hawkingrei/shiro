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

func eetPredicatePolicy(gen *generator.Generator) predicatePolicy {
	policy := predicatePolicyFor(gen)
	policy.allowNot = true
	policy.allowIsNull = true
	return policy
}

// Run builds a query, applies an equivalent predicate rewrite, and compares
// signatures for mismatches.
func (o EET) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, _ *schema.State) Result {
	policy := eetPredicatePolicy(gen)
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
	if query.Having != nil && !predicateMatches(query.Having, policy) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:having_guard"}}
	}
	for _, join := range query.From.Joins {
		if join.On != nil && !predicateMatches(join.On, policy) {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:on_guard"}}
		}
	}
	if query.Limit != nil && orderByAllConstant(query.OrderBy) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "eet:order_by_constant"}}
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
	restored, err := restoreEETSQL(sel)
	if err != nil {
		details["error_reason"] = "eet:restore_error"
		return "", details, err
	}
	return restored, details, nil
}

func orderByAllConstant(orderBy []generator.OrderBy) bool {
	if len(orderBy) == 0 {
		return false
	}
	for _, ob := range orderBy {
		if !exprIsConstant(ob.Expr) {
			return false
		}
	}
	return true
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

func rewriteSelectPredicates(sel *ast.SelectStmt, gen *generator.Generator) (eetRewriteKind, bool) {
	kind := pickEETRewriteKind(sel, gen)
	if kind == "" {
		return "", false
	}
	if kind == eetRewriteDoubleNot || kind == eetRewriteAndTrue || kind == eetRewriteOrFalse {
		if rewriteBooleanPredicateInSelect(sel, kind, gen) {
			return kind, true
		}
		return "", false
	}
	if rewriteLiteralPredicateInSelect(sel, kind, gen) {
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

func rewriteLiteralPredicateInSelect(sel *ast.SelectStmt, kind eetRewriteKind, gen *generator.Generator) bool {
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
	case ast.ValueExpr:
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
