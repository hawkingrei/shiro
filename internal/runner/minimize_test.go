package runner

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

func TestSelectCandidatesKeepCTEWhenReferenced(t *testing.T) {
	sql := "WITH cte_0 AS (SELECT 1 AS c0) SELECT c0 FROM cte_0"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	for _, cand := range candidates {
		upper := strings.ToUpper(cand)
		if strings.Contains(upper, "CTE_0") && !strings.Contains(upper, "WITH") {
			t.Fatalf("cte removed while still referenced: %s", cand)
		}
	}
}

func TestSelectCandidatesDropSingleCTEFromWithList(t *testing.T) {
	sql := "WITH cte_0 AS (SELECT 1 AS c0), cte_1 AS (SELECT 2 AS c1) SELECT c0 FROM cte_0"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	found := false
	for _, cand := range candidates {
		candNode, err := p.ParseOneStmt(cand, "", "")
		if err != nil {
			continue
		}
		candSel, ok := candNode.(*ast.SelectStmt)
		if !ok || candSel.With == nil || len(candSel.With.CTEs) != 1 {
			continue
		}
		found = true
		break
	}
	if !found {
		t.Fatalf("expected candidate that drops one CTE from WITH list")
	}
}

func TestSelectCandidatesReduceNestedBoolPredicateBranch(t *testing.T) {
	sql := "SELECT * FROM t WHERE (a > b OR c > d) AND e > f"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	found := false
	for _, cand := range candidates {
		candNode, err := p.ParseOneStmt(cand, "", "")
		if err != nil {
			continue
		}
		candSel, ok := candNode.(*ast.SelectStmt)
		if !ok || candSel.Where == nil {
			continue
		}
		if isColumnExpression(candSel.Where, "e", "f") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected candidate that keeps nested right branch predicate e > f")
	}
}

func TestSetOprCandidatesDropSingleCTEFromWithList(t *testing.T) {
	sql := "WITH cte_0 AS (SELECT 1 AS c0), cte_1 AS (SELECT 2 AS c0) SELECT c0 FROM cte_0 UNION SELECT c0 FROM cte_0"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	setOpr, ok := node.(*ast.SetOprStmt)
	if !ok {
		t.Fatalf("expected set operation stmt")
	}
	candidates := setOprCandidates(p, setOpr)
	found := false
	for _, cand := range candidates {
		candNode, err := p.ParseOneStmt(cand, "", "")
		if err != nil {
			continue
		}
		candSet, ok := candNode.(*ast.SetOprStmt)
		if !ok || candSet.With == nil || len(candSet.With.CTEs) != 1 {
			continue
		}
		found = true
		break
	}
	if !found {
		t.Fatalf("expected candidate that drops one CTE from WITH list in set operation")
	}
}

func TestSelectCandidatesReduceNestedBoolHavingBranch(t *testing.T) {
	sql := "SELECT a, SUM(b) AS s FROM t GROUP BY a HAVING (SUM(b) > 1 OR SUM(b) < -1) AND a > 0"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	found := false
	for _, cand := range candidates {
		candNode, err := p.ParseOneStmt(cand, "", "")
		if err != nil {
			continue
		}
		candSel, ok := candNode.(*ast.SelectStmt)
		if !ok || candSel.Having == nil {
			continue
		}
		if isColumnExpression(candSel.Having.Expr, "a") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected candidate that keeps nested right branch in HAVING")
	}
}

func TestSelectCandidatesReduceNestedBoolPredicateBranchUnderNot(t *testing.T) {
	sql := "SELECT * FROM t WHERE NOT ((a > b OR c > d) AND e > f)"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	found := false
	for _, cand := range candidates {
		candNode, err := p.ParseOneStmt(cand, "", "")
		if err != nil {
			continue
		}
		candSel, ok := candNode.(*ast.SelectStmt)
		if !ok || candSel.Where == nil {
			continue
		}
		if isColumnExpression(candSel.Where, "e", "f") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected candidate that can reduce NOT-wrapped nested boolean predicate branch")
	}
}

func isColumnExpression(expr ast.ExprNode, columns ...string) bool {
	if expr == nil {
		return false
	}
	expr = unwrapParenthesesExpr(expr)
	bin, ok := expr.(*ast.BinaryOperationExpr)
	if ok && (bin.Op == opcode.LogicAnd || bin.Op == opcode.LogicOr) {
		return false
	}
	expected := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		trimmed := strings.ToLower(strings.TrimSpace(col))
		if trimmed != "" {
			expected[trimmed] = struct{}{}
		}
	}
	if len(expected) == 0 {
		return false
	}
	collector := &columnNameCollector{columns: map[string]struct{}{}}
	expr.Accept(collector)
	for col := range expected {
		if _, ok := collector.columns[col]; !ok {
			return false
		}
	}
	return true
}

type columnNameCollector struct {
	columns map[string]struct{}
}

func (c *columnNameCollector) Enter(in ast.Node) (ast.Node, bool) {
	if c == nil || c.columns == nil {
		return in, false
	}
	col, ok := in.(*ast.ColumnNameExpr)
	if !ok || col.Name.Name.O == "" {
		return in, false
	}
	c.columns[strings.ToLower(col.Name.Name.O)] = struct{}{}
	return in, false
}

func (c *columnNameCollector) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
