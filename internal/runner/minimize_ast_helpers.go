package runner

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

func selectUsesCTE(stmt *ast.SelectStmt) bool {
	if stmt == nil || stmt.With == nil {
		return false
	}
	names := cteNames(stmt.With)
	if len(names) == 0 {
		return false
	}
	if stmt.From == nil || stmt.From.TableRefs == nil {
		return false
	}
	return tableRefsUsesNames(stmt.From.TableRefs, names)
}

func setOprUsesCTE(stmt *ast.SetOprStmt) bool {
	if stmt == nil || stmt.With == nil {
		return false
	}
	names := cteNames(stmt.With)
	if len(names) == 0 {
		return false
	}
	if stmt.SelectList == nil {
		return false
	}
	for _, sel := range stmt.SelectList.Selects {
		switch v := sel.(type) {
		case *ast.SelectStmt:
			if v.From != nil && v.From.TableRefs != nil && tableRefsUsesNames(v.From.TableRefs, names) {
				return true
			}
		case *ast.SetOprStmt:
			if setOprUsesCTE(v) {
				return true
			}
		}
	}
	return false
}

func cteNames(with *ast.WithClause) []string {
	if with == nil {
		return nil
	}
	names := make([]string, 0, len(with.CTEs))
	for _, cte := range with.CTEs {
		if cte == nil {
			continue
		}
		name := strings.TrimSpace(cte.Name.O)
		if name != "" {
			names = append(names, name)
		}
	}
	return names
}

func tableRefsUsesNames(node ast.ResultSetNode, names []string) bool {
	switch v := node.(type) {
	case *ast.Join:
		if tableRefsUsesNames(v.Left, names) {
			return true
		}
		if tableRefsUsesNames(v.Right, names) {
			return true
		}
	case *ast.TableSource:
		return tableSourceUsesNames(v, names)
	case *ast.SelectStmt:
		if v.From != nil && v.From.TableRefs != nil {
			return tableRefsUsesNames(v.From.TableRefs, names)
		}
	case *ast.SetOprStmt:
		return setOprUsesCTE(v)
	}
	return false
}

func tableSourceUsesNames(source *ast.TableSource, names []string) bool {
	if source == nil {
		return false
	}
	switch v := source.Source.(type) {
	case *ast.TableName:
		for _, name := range names {
			if strings.EqualFold(name, v.Name.O) {
				return true
			}
		}
	case *ast.Join:
		return tableRefsUsesNames(v, names)
	case *ast.SelectStmt:
		if v.From != nil && v.From.TableRefs != nil {
			return tableRefsUsesNames(v.From.TableRefs, names)
		}
	case *ast.SetOprStmt:
		return setOprUsesCTE(v)
	}
	return false
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
	case *ast.ParenthesesExpr:
		if v.Expr != nil {
			return simplifyExpr(v.Expr)
		}
		return ast.NewValueExpr(1, "", "")
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

func predicateReduceCandidates(expr ast.ExprNode) []ast.ExprNode {
	if expr == nil {
		return nil
	}
	var out []ast.ExprNode
	collectPredicateReduceCandidates(expr, &out)
	return out
}

func collectPredicateReduceCandidates(expr ast.ExprNode, out *[]ast.ExprNode) {
	if expr == nil || out == nil {
		return
	}
	expr = unwrapParenthesesExpr(expr)
	switch v := expr.(type) {
	case *ast.BinaryOperationExpr:
		switch v.Op {
		case opcode.LogicAnd, opcode.LogicOr:
			if v.L != nil {
				left := unwrapParenthesesExpr(v.L)
				if left != nil {
					*out = append(*out, left)
					collectPredicateReduceCandidates(left, out)
				}
			}
			if v.R != nil {
				right := unwrapParenthesesExpr(v.R)
				if right != nil {
					*out = append(*out, right)
					collectPredicateReduceCandidates(right, out)
				}
			}
		}
	case *ast.UnaryOperationExpr:
		if v.V != nil {
			inner := unwrapParenthesesExpr(v.V)
			if inner != nil {
				*out = append(*out, inner)
				collectPredicateReduceCandidates(inner, out)
			}
		}
	}
}

func unwrapParenthesesExpr(expr ast.ExprNode) ast.ExprNode {
	for {
		paren, ok := expr.(*ast.ParenthesesExpr)
		if !ok || paren == nil || paren.Expr == nil {
			return expr
		}
		expr = paren.Expr
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
	if stmt.With != nil && !setOprUsesCTE(stmt) {
		stmt.With = nil
	}
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

func splitInsertPrefixValues(stmt string) (prefix string, values string, ok bool) {
	upper := strings.ToUpper(stmt)
	idx := strings.Index(upper, "VALUES")
	if idx == -1 {
		return "", "", false
	}
	prefix = strings.TrimSpace(stmt[:idx+len("VALUES")])
	values = strings.TrimSpace(stmt[idx+len("VALUES"):])
	if values == "" {
		return "", "", false
	}
	return prefix, values, true
}
