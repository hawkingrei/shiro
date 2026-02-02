package oracle

import (
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

// SQLSubqueryFeatures captures IN/EXISTS usage in a SQL statement.
type SQLSubqueryFeatures struct {
	HasInSubquery     bool
	HasNotInSubquery  bool
	HasInList         bool
	HasNotInList      bool
	HasExistsSubquery bool
	HasNotExists      bool
}

var sqlParserPool = sync.Pool{
	New: func() any {
		return parser.New()
	},
}

// DetectSubqueryFeaturesSQL parses SQL and reports IN/EXISTS usage.
func DetectSubqueryFeaturesSQL(sqlText string) (features SQLSubqueryFeatures) {
	if strings.TrimSpace(sqlText) == "" {
		return SQLSubqueryFeatures{}
	}
	p := sqlParserPool.Get().(*parser.Parser)
	stmt, err := p.ParseOneStmt(sqlText, "", "")
	sqlParserPool.Put(p)
	if err != nil {
		return SQLSubqueryFeatures{}
	}
	visitor := &subqueryFeatureVisitor{}
	stmt.Accept(visitor)
	return visitor.features
}

// DetectInSubquerySQL reports whether the SQL contains IN/NOT IN with subquery.
func DetectInSubquerySQL(sqlText string) (hasInSubquery bool, hasNotInSubquery bool) {
	features := DetectSubqueryFeaturesSQL(sqlText)
	return features.HasInSubquery, features.HasNotInSubquery
}

type subqueryFeatureVisitor struct {
	features SQLSubqueryFeatures
}

func (v *subqueryFeatureVisitor) Enter(node ast.Node) (ast.Node, bool) {
	if v.features.HasInSubquery &&
		v.features.HasNotInSubquery &&
		v.features.HasInList &&
		v.features.HasNotInList &&
		v.features.HasExistsSubquery &&
		v.features.HasNotExists {
		return node, true
	}
	switch expr := node.(type) {
	case *ast.UnaryOperationExpr:
		if expr.Op == opcode.Not {
			if inExpr := unwrapPatternIn(expr.V); inExpr != nil {
				if patternInHasSubquery(inExpr) {
					v.features.HasNotInSubquery = true
					return node, true
				}
			}
		}
	case *ast.PatternInExpr:
		if patternInHasSubquery(expr) {
			if expr.Not {
				v.features.HasNotInSubquery = true
			} else {
				v.features.HasInSubquery = true
			}
		} else {
			if expr.Not {
				v.features.HasNotInList = true
			} else {
				v.features.HasInList = true
			}
		}
	case *ast.ExistsSubqueryExpr:
		if expr.Not {
			v.features.HasNotExists = true
		} else {
			v.features.HasExistsSubquery = true
		}
	}
	return node, false
}

func (v *subqueryFeatureVisitor) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

func unwrapPatternIn(node ast.ExprNode) *ast.PatternInExpr {
	switch expr := node.(type) {
	case *ast.PatternInExpr:
		return expr
	case *ast.ParenthesesExpr:
		return unwrapPatternIn(expr.Expr)
	default:
		return nil
	}
}

func patternInHasSubquery(expr *ast.PatternInExpr) bool {
	if expr == nil {
		return false
	}
	if expr.Sel != nil {
		return true
	}
	for _, item := range expr.List {
		if _, ok := item.(*ast.SubqueryExpr); ok {
			return true
		}
	}
	return false
}
