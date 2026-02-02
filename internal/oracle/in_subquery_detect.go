package oracle

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

// DetectInSubquerySQL reports whether the SQL contains IN/NOT IN with subquery.
func DetectInSubquerySQL(sqlText string) (bool, bool) {
	if strings.TrimSpace(sqlText) == "" {
		return false, false
	}
	p := parser.New()
	stmt, err := p.ParseOneStmt(sqlText, "", "")
	if err != nil {
		return false, false
	}
	visitor := &inSubqueryVisitor{}
	stmt.Accept(visitor)
	return visitor.inSubquery, visitor.notInSubquery
}

type inSubqueryVisitor struct {
	inSubquery    bool
	notInSubquery bool
}

func (v *inSubqueryVisitor) Enter(node ast.Node) (ast.Node, bool) {
	if v.inSubquery && v.notInSubquery {
		return node, true
	}
	if expr, ok := node.(*ast.PatternInExpr); ok {
		hasSubquery := expr.Sel != nil
		if !hasSubquery {
			for _, item := range expr.List {
				if _, ok := item.(*ast.SubqueryExpr); ok {
					hasSubquery = true
					break
				}
			}
		}
		if hasSubquery {
			if expr.Not {
				v.notInSubquery = true
			} else {
				v.inSubquery = true
			}
		}
	}
	return node, false
}

func (v *inSubqueryVisitor) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}
