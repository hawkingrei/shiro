package oracle

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
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

// DetectSubqueryFeaturesSQL parses SQL and reports IN/EXISTS usage.
func DetectSubqueryFeaturesSQL(sqlText string) (features SQLSubqueryFeatures) {
	if strings.TrimSpace(sqlText) == "" {
		return SQLSubqueryFeatures{}
	}
	p := parser.New()
	stmt, err := p.ParseOneStmt(sqlText, "", "")
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
	case *ast.PatternInExpr:
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
