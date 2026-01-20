package runner

import (
	"context"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

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
	if n.With != nil && !selectUsesCTE(n) {
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
	if n.With != nil && !setOprUsesCTE(n) {
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
