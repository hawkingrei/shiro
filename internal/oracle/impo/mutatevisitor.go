package impo

import (
	"bytes"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/opcode"

	"github.com/pkg/errors"
	"log"
	"strings"
)

// Candidate is a mutation candidate consisting of name, direction, node, and flag.
//
// U: 1: upper mutation, 0: lower mutation;
//
// Flag: 1: positive, 0: negative.
//
// (U ^ Flag)^1): 1: the mutated result will expand; 0: the mutated result will shrink.
//
// For example:
//
//	SELECT * FROM T WHERE (X > 0) IS FALSE; -- sql1
//	SELECT * FROM T WHERE (X >= 0) IS FALSE; -- sql2
//
// sql(x>0) -> sql2(x>=0) is an upper mutation, U = 1. However, IS FALSE brings negative impact, Flag = 0.
// Therefore, the mutated result will shrink, (U ^ Flag)^1) = 0
type Candidate struct {
	MutationName string   // mutation name
	U            int      // 1: upper mutation, 0: lower mutation;
	Node         ast.Node // candidate node
	Flag         int      // 1: positive, 0: negative
}

// MutateVisitor traverses AST nodes and collects mutation candidates.
//
// There are lots of functions under MutateVisitor, we classify them by prefix:
//
//	visitxxx: calculate Flag, call miningxxx
//	miningxxx: call addxxx
//	addxxx: add mutation U/L to Candidates
//
// We have made a lot of effort to analyze the features of mysql:
//
// - analyzed https://dev.mysql.com/doc/refman/8.0/en/
//
// - analyzed all 175 ast.Node of tidb parser(https://github.com/pingcap/tidb/tree/v5.4.2/parser),
// of which 57 nodes are related to query, including 31 operators and 274 functions.
//
// Through the analysis we get the following strategies:
//
// (1) visit strategies:
// We recursively traverse each ast node and its descendants. Stop recursion when meet:
//
// - numerical operations, such as |, &, ~, <<, >>, +, -, *, /(DIV), %(MOD), ^,
// see https://dev.mysql.com/doc/refman/8.0/en/numeric-functions.html
//
// - logical operation XOR. we will visit the descendants of OR(||), AND(&&), NOT(!), but stop recursion when meet XOR.
// see https://dev.mysql.com/doc/refman/8.0/en/logical-operators.html
//
// - comparison operations exclude IS [NOT] TRUE/IS [NOT] FALSE, such as =, >=, >, <=, <, !=, <>, <=>,
// IS NULL, IN, BETWEEN AND, LIKE, REGEXP.
// see https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html
//
//	comparison operations are important mutation points.
//	For convenience, we only focus on the top-level comparison operations, so we stop recursion when meet comparison operations.
//	Note that we will visit the descendants of IS TRUE/ IS FALSE, they are equivalent to logical operations.
//	Moreover, we only focus on true/false, so we do care about IS NULL, <=>, just stop recursion.
//
// - subqueries without ANY,ALL,SOME,IN,EXISTS. such as SELECT X FROM T1 WHERE X > (SELECT 1)
//
// - control flow. such as CASE, IF, see https://dev.mysql.com/doc/refman/8.0/en/flow-control-statements.html
//
// - functions. Currently we do not analyze the implication in functions.
//
// - unknown features. Although we have made a lot of effort to analyze the features of mysql, we may still be ill-considered.
// Therefore, we conservatively stop recursion when meet unknown features.
//
// (2) mutation strategies: We will calculate candidate mutation points during visit, then mutate them. see allmutations.go
type MutateVisitor struct {
	Root       ast.Node
	Candidates map[string][]*Candidate // mutation name : slice of *Candidate
	subqDepth  int
}

func (v *MutateVisitor) visit(in ast.Node, flag int) {
	switch in := in.(type) {
	case *ast.SetOprStmt:
		v.visitSetOprStmt(in, flag)
	case *ast.SelectStmt:
		v.visitSelect(in, flag)
	}
}

func (v *MutateVisitor) visitSetOprStmt(in *ast.SetOprStmt, flag int) {
	if in == nil {
		return
	}
	v.visitWithClause(in.SelectList.With, flag)
	v.visitSetOprSelectList(in.SelectList, flag)
}

// visitSetOprSelectList: miningSetOprSelectList
func (v *MutateVisitor) visitSetOprSelectList(in *ast.SetOprSelectList, flag int) {
	// Note that MySQL only has UNION [ALL]
	if in == nil {
		return
	}
	v.visitWithClause(in.With, flag)
	for _, sel := range in.Selects {
		switch sel := sel.(type) {
		case *ast.SetOprSelectList:
			v.visitSetOprSelectList(sel, flag)
		case *ast.SelectStmt:
			v.visitSelect(sel, flag)
		}
	}

	v.miningSetOprSelectList(in, flag)
}

func (v *MutateVisitor) visitWithClause(in *ast.WithClause, flag int) {
	if in == nil {
		return
	}
	// cannot support recursive WITH
	if in.IsRecursive {
		return
	}
	for _, cte := range in.CTEs {
		v.visitSubqueryExprWithContext(cte.Query, flag, false)
	}
}

func (v *MutateVisitor) visitSubqueryExpr(in *ast.SubqueryExpr, flag int) {
	v.visitSubqueryExprWithContext(in, flag, false)
}

func (v *MutateVisitor) visitSubqueryExprWithContext(in *ast.SubqueryExpr, flag int, allowDistinct bool) {
	if in == nil {
		return
	}
	if allowDistinct {
		v.maybeAddDistinctNested(in, flag)
	}
	v.subqDepth++
	v.visitResultSetNode(in.Query, flag)
	v.subqDepth--
}

// visitResultSetNode: important bridge, include
// SelectStmt, SubqueryExpr, TableSource, TableName, Join and SetOprStmt.
func (v *MutateVisitor) visitResultSetNode(in ast.ResultSetNode, flag int) {
	if in == nil {
		return
	}
	switch in := in.(type) {
	case *ast.SelectStmt:
		v.visitSelect(in, flag)
	case *ast.SubqueryExpr:
		v.visitSubqueryExpr(in, flag)
	case *ast.TableSource:
		v.visitTableSource(in, flag)
	case *ast.TableName:
		// skip
	case *ast.Join:
		v.visitJoin(in, flag)
	case *ast.SetOprStmt:
		v.visitSetOprStmt(in, flag)
	}
}

func (v *MutateVisitor) visitTableSource(in *ast.TableSource, flag int) {
	if in == nil {
		return
	}
	v.visitResultSetNode(in.Source, flag)
}

// visitSelect: miningSelectStmt
func (v *MutateVisitor) visitSelect(in *ast.SelectStmt, flag int) {
	if in == nil {
		return
	}

	// from
	v.visitTableRefClause(in.From, flag)
	// where
	v.visitExprNode(in.Where, flag)
	// having
	v.visitHavingClause(in.Having, flag)
	// with
	v.visitWithClause(in.With, flag)

	v.miningSelectStmt(in, flag)
}

func (v *MutateVisitor) visitTableRefClause(in *ast.TableRefsClause, flag int) {
	if in == nil {
		return
	}
	v.visitJoin(in.TableRefs, flag)
}

func (v *MutateVisitor) visitJoin(in *ast.Join, flag int) {
	if in == nil {
		return
	}
	// skip left | right join
	if in.Tp == ast.LeftJoin || in.Tp == ast.RightJoin {
		return
	}
	v.visitResultSetNode(in.Left, flag)
	v.visitResultSetNode(in.Right, flag)
	// on
	v.visitOnCondition(in.On, flag)

	v.miningJoin(in, flag)
}

func (v *MutateVisitor) visitOnCondition(in *ast.OnCondition, flag int) {
	if in == nil {
		return
	}
	v.visitExprNode(in.Expr, flag)
}

func (v *MutateVisitor) visitHavingClause(in *ast.HavingClause, flag int) {
	if in == nil {
		return
	}
	v.visitExprNode(in.Expr, flag)
}

// visitExprNode: important bridge
func (v *MutateVisitor) visitExprNode(in ast.ExprNode, flag int) {
	if in == nil {
		return
	}
	switch in := in.(type) {
	case *ast.BetweenExpr:
		v.visitBetweenExpr(in, flag)
	case *ast.BinaryOperationExpr:
		v.visitBinaryOperationExpr(in, flag)
	case *ast.CaseExpr:
		// skip
	case *ast.SubqueryExpr:
		v.visitSubqueryExpr(in, flag)
	case *ast.CompareSubqueryExpr:
		v.visitCompareSubqueryExpr(in, flag)
	case *ast.TableNameExpr:
		// skip
	case *ast.ColumnNameExpr:
		// skip
	case *ast.DefaultExpr:
		// skip
	case *ast.ExistsSubqueryExpr:
		v.visitExistsSubqueryExpr(in, flag)
	case *ast.PatternInExpr:
		v.visitPatternInExpr(in, flag)
	case *ast.IsNullExpr:
		v.visitIsNullExpr(in, flag)
	case *ast.IsTruthExpr:
		v.visitIsTruthExpr(in, flag)
	case *ast.PatternLikeOrIlikeExpr:
		v.visitPatternLikeOrIlikeExpr(in, flag)
	//case ast.ParamMarkerExpr:
	case *ast.ParenthesesExpr:
		v.visitParenthesesExpr(in, flag)
	case *ast.PositionExpr:
		// skip
	case *ast.PatternRegexpExpr:
		v.visitPatternRegexpExpr(in, flag)
	case *ast.RowExpr:
		// skip
	case *ast.UnaryOperationExpr:
		v.visitUnaryOperationExpr(in, flag)
	case *ast.ValuesExpr:
		// skip
	case *ast.VariableExpr:
		// skip
	case *ast.MaxValueExpr:
		// skip
		// https://dev.mysql.com/doc/refman/8.0/en/partitioning-range.html
	case *ast.MatchAgainst:
		// skip, todo match
	case *ast.SetCollationExpr:
		// skip
	case *ast.FuncCallExpr:
		v.visitFuncCallExpr(in, flag)
	case *ast.FuncCastExpr:
		v.visitFuncCastExpr(in, flag)
	case *ast.TrimDirectionExpr:
		v.visitTrimDirectionExpr(in, flag)
	case *ast.AggregateFuncExpr:
		// skip
	case *ast.WindowFuncExpr:
		// skip
	case *ast.TimeUnitExpr:
		// skip
	case *ast.GetFormatSelectorExpr:
		// skip
	}
}

func (v *MutateVisitor) visitBetweenExpr(in *ast.BetweenExpr, flag int) {
	if in == nil {
		return
	}
	if in.Not {
		flag = flag ^ 1
	}
	v.miningBetweenExpr(in, flag)
}

// visitBinaryOperationExpr: important bridge, miningBinaryOperationExpr
func (v *MutateVisitor) visitBinaryOperationExpr(in *ast.BinaryOperationExpr, flag int) {
	if in == nil {
		return
	}
	switch in.Op {
	case opcode.LogicAnd:
		v.visitExprNode(in.L, flag)
		v.visitExprNode(in.R, flag)
	case opcode.LeftShift:
		// numeric skip
	case opcode.RightShift:
		// numeric skip
	case opcode.LogicOr:
		v.visitExprNode(in.L, flag)
		v.visitExprNode(in.R, flag)
	case opcode.GE:
		// cmp mutation, see miningBinaryOperationExpr
	case opcode.LE:
		// cmp mutation, see miningBinaryOperationExpr
	case opcode.EQ:
		// cmp mutation, see miningBinaryOperationExpr
	case opcode.NE:
		// cmp mutation, see miningBinaryOperationExpr
	case opcode.LT:
		// cmp mutation, see miningBinaryOperationExpr
	case opcode.GT:
		// cmp mutation, see miningBinaryOperationExpr
	case opcode.Plus:
		// numeric skip
	case opcode.Minus:
		// numeric skip
	case opcode.And:
		// numeric skip
	case opcode.Or:
		// numeric skip
	case opcode.Mod:
		// numeric skip
	case opcode.Xor:
		// numeric skip
	case opcode.Div:
		// numeric skip
	case opcode.Mul:
		// numeric skip
	//case opcode.Not:
	//case opcode.Not2:
	//case opcode.BitNeg:
	case opcode.IntDiv:
		// numeric skip
	case opcode.LogicXor:
		// skip
	case opcode.NullEQ:
		// skip
		//case opcode.In:
		//case opcode.Like:
		//case opcode.Case:
		//case opcode.Regexp:
		//case opcode.IsNull:
		//case opcode.IsTruth:
		//case opcode.IsFalsity:
	}

	v.miningBinaryOperationExpr(in, flag)
}

// visitCompareSubqueryExpr: miningCompareSubqueryExpr
func (v *MutateVisitor) visitCompareSubqueryExpr(in *ast.CompareSubqueryExpr, flag int) {
	if in == nil {
		return
	}
	// before All
	v.miningCompareSubqueryExpr(in, flag)
	// in.all false: ANY, in.all true: ALL
	if in.All {
		flag = flag ^ 1
	}
	if subq, ok := (in.R).(*ast.SubqueryExpr); ok {
		v.visitSubqueryExprWithContext(subq, flag, true)
		v.maybeAddOrderByRemoval(subq, flag)
		v.maybeAddLimitExpansion(subq, flag)
	}
}

func (v *MutateVisitor) visitExistsSubqueryExpr(in *ast.ExistsSubqueryExpr, flag int) {
	if in == nil {
		return
	}
	if in.Not {
		flag = flag ^ 1
	}
	v.miningExistsSubqueryExpr(in, flag)
	if subq, ok := (in.Sel).(*ast.SubqueryExpr); ok {
		v.visitSubqueryExprWithContext(subq, flag, true)
		v.maybeAddOrderByRemoval(subq, flag)
		v.maybeAddLimitExpansion(subq, flag)
	}
}

// visitPatternInExpr: miningPatternInExpr
func (v *MutateVisitor) visitPatternInExpr(in *ast.PatternInExpr, flag int) {
	if in == nil {
		return
	}
	if in.Not {
		flag = flag ^ 1
	}
	// IN (XXX,XXX,XXX) OR IN (SUBQUERY)?
	switch (in.Sel).(type) {
	case *ast.SubqueryExpr:
		subq := (in.Sel).(*ast.SubqueryExpr)
		v.visitSubqueryExprWithContext(subq, flag, true)
		v.maybeAddOrderByRemoval(subq, flag)
		v.maybeAddLimitExpansion(subq, flag)
	default:
		// after in.Not
		v.miningPatternInExpr(in, flag)
	}
}

func (v *MutateVisitor) visitIsNullExpr(in *ast.IsNullExpr, _ int) {
	if in == nil {
		return
	}
	// skip
}

func (v *MutateVisitor) visitIsTruthExpr(in *ast.IsTruthExpr, flag int) {
	if in == nil {
		return
	}
	if in.Not {
		// IS NOT
		flag = flag ^ 1
	}
	if in.True <= 0 {
		// FALSE
		flag = flag ^ 1
	}
	v.visitExprNode(in.Expr, flag)
}

// visitParenthesesExpr: ()
func (v *MutateVisitor) visitParenthesesExpr(in *ast.ParenthesesExpr, flag int) {
	if in == nil {
		return
	}
	v.visitExprNode(in.Expr, flag)
}

// visitPatternLikeOrIlikeExpr: miningPatternLikeOrIlikeExpr
func (v *MutateVisitor) visitPatternLikeOrIlikeExpr(in *ast.PatternLikeOrIlikeExpr, flag int) {
	if in == nil {
		return
	}
	if in.Not {
		flag = flag ^ 1
	}
	// after in.Not
	v.miningPatternLikeOrIlikeExpr(in, flag)
}

// visitPatternRegexpExpr: miningPatternRegexpExpr
func (v *MutateVisitor) visitPatternRegexpExpr(in *ast.PatternRegexpExpr, flag int) {
	if in == nil {
		return
	}
	if in.Not {
		flag = flag ^ 1
	}
	// after in.Not
	v.miningPatternRegexpExpr(in, flag)
}

// visitUnaryOperationExpr: important bridge
func (v *MutateVisitor) visitUnaryOperationExpr(in *ast.UnaryOperationExpr, flag int) {
	if in == nil {
		return
	}
	switch in.Op {
	case opcode.Plus:
		// numeric skip
	case opcode.Minus:
		// numeric skip
	case opcode.Not:
		flag = flag ^ 1
		v.visitExprNode(in.V, flag)
	case opcode.Not2:
		flag = flag ^ 1
		v.visitExprNode(in.V, flag)
	case opcode.BitNeg:
		// numeric skip
	}
}

func (v *MutateVisitor) visitFuncCallExpr(in *ast.FuncCallExpr, _ int) {
	if in == nil {
		return
	}
	// skip func call
}

func (v *MutateVisitor) visitFuncCastExpr(in *ast.FuncCastExpr, _ int) {
	if in == nil {
		return
	}
	// skip cast
}

func (v *MutateVisitor) visitTrimDirectionExpr(in *ast.TrimDirectionExpr, _ int) {
	if in == nil {
		return
	}
	// skip trim
}

func (v *MutateVisitor) miningSetOprSelectList(in *ast.SetOprSelectList, flag int) {
	// FixMRmUnionAllL
	v.addFixMRmUnionAllL(in, flag)
	// FixMRmUnionL
	v.addFixMRmUnionL(in, flag)
}

func (v *MutateVisitor) miningSelectStmt(in *ast.SelectStmt, flag int) {
	if v.subqDepth == 0 {
		// FixMDistinctU
		v.addFixMDistinctU(in, flag)
		// FixMDistinctL
		v.addFixMDistinctL(in, flag)
	}
	// FixMUnionAllU
	v.addFixMUnionAllU(in, flag)
	// FixMUnionAllL
	v.addFixMUnionAllL(in, flag)
	// FixMWhere1U
	v.addFixMWhere1U(in, flag)
	// FixMWhere0L
	v.addFixMWhere0L(in, flag)
	// FixMHaving1U
	v.addFixMHaving1U(in, flag)
	// FixMHaving0L
	v.addFixMHaving0L(in, flag)
}

func (v *MutateVisitor) maybeAddDistinctNested(in *ast.SubqueryExpr, flag int) {
	if in == nil {
		return
	}
	sel, ok := in.Query.(*ast.SelectStmt)
	if !ok {
		return
	}
	if !isSafeDistinctSubquery(sel) {
		return
	}
	if sel.Distinct {
		v.addCandidate(FixMDistinctU, 1, sel, flag)
		return
	}
	v.addCandidate(FixMDistinctL, 0, sel, flag)
}

func (v *MutateVisitor) maybeAddOrderByRemoval(in *ast.SubqueryExpr, flag int) {
	if in == nil {
		return
	}
	sel, ok := in.Query.(*ast.SelectStmt)
	if !ok {
		return
	}
	if !isSafeOrderByRemovalSubquery(sel) {
		return
	}
	v.addCandidate(FixMRmOrderByL, 0, sel, flag)
}

func (v *MutateVisitor) maybeAddLimitExpansion(in *ast.SubqueryExpr, flag int) {
	if in == nil {
		return
	}
	sel, ok := in.Query.(*ast.SelectStmt)
	if !ok {
		return
	}
	if !isSafeLimitExpansionSubquery(sel) {
		return
	}
	v.addCandidate(FixMLimitU, 1, sel, flag)
}

func isSafeDistinctSubquery(sel *ast.SelectStmt) bool {
	if sel == nil {
		return false
	}
	if sel.With != nil {
		return false
	}
	if sel.OrderBy != nil || sel.Limit != nil {
		return false
	}
	if sel.GroupBy != nil || sel.Having != nil {
		return false
	}
	if len(sel.WindowSpecs) > 0 {
		return false
	}
	if sel.AfterSetOperator != nil {
		return false
	}
	return true
}

func (v *MutateVisitor) miningJoin(in *ast.Join, flag int) {
	// FixMOn1U
	v.addFixMOn1U(in, flag)
	// FixMOn0L
	v.addFixMOn0L(in, flag)
}

func (v *MutateVisitor) miningBinaryOperationExpr(in *ast.BinaryOperationExpr, flag int) {
	// FixMCmpOpU
	v.addFixMCmpOpU(in, flag)
	// FixMCmpOpL
	v.addFixMCmpOpL(in, flag)
}

func (v *MutateVisitor) miningCompareSubqueryExpr(in *ast.CompareSubqueryExpr, flag int) {
	// FixMCmpOpU
	v.addFixMCmpOpU(in, flag)
	// FixMCmpOpL
	v.addFixMCmpOpL(in, flag)
	// FixMAnyAllU / FixMAnyAllL
	v.addFixMAnyAll(in, flag)
}

func (v *MutateVisitor) miningPatternInExpr(in *ast.PatternInExpr, flag int) {
	// FixMInNullU
	v.addFixMInNullU(in, flag)
	// FixMInListU / FixMInListL
	v.addFixMInListU(in, flag)
	v.addFixMInListL(in, flag)
}

func (v *MutateVisitor) miningBetweenExpr(in *ast.BetweenExpr, flag int) {
	// FixMBetweenU / FixMBetweenL
	v.addFixMBetweenU(in, flag)
	v.addFixMBetweenL(in, flag)
}

func (v *MutateVisitor) miningExistsSubqueryExpr(in *ast.ExistsSubqueryExpr, flag int) {
	// FixMExistsU / FixMExistsL
	v.addFixMExistsU(in, flag)
	v.addFixMExistsL(in, flag)
}

func (v *MutateVisitor) miningPatternLikeOrIlikeExpr(in *ast.PatternLikeOrIlikeExpr, flag int) {
	// RdMLikeU
	v.addRdMLikeU(in, flag)
	// RdMLikeL
	v.addRdMLikeL(in, flag)
}

func (v *MutateVisitor) miningPatternRegexpExpr(in *ast.PatternRegexpExpr, flag int) {
	// RdMRegExpU
	v.addRdMRegExpU(in, flag)
	// RdMRegExpL
	v.addRdMRegExpL(in, flag)
}

func (v *MutateVisitor) addCandidate(mutationName string, u int, in ast.Node, flag int) {
	if strings.HasSuffix(mutationName, "U") && u == 0 {
		log.Fatal("strings.HasSuffix(mutationName, \"U\") && u == 0")
	}
	if strings.HasSuffix(mutationName, "L") && u != 0 {
		log.Fatal("strings.HasSuffix(mutationName, \"L\") && u != 0")
	}
	var ls []*Candidate
	ok := false
	if ls, ok = v.Candidates[mutationName]; !ok {
		ls = make([]*Candidate, 0, 1)
	}
	ls = append(ls, &Candidate{
		MutationName: mutationName,
		U:            u,
		Node:         in,
		Flag:         flag,
	})
	v.Candidates[mutationName] = ls
}

func restore(rootNode ast.Node) ([]byte, error) {
	buf := new(bytes.Buffer)
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, buf)
	err := rootNode.Restore(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "restore error")
	}
	return buf.Bytes(), nil
}
