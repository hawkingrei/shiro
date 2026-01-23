package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
)

// rmAgg removes aggregate functions and GROUP BY clauses.
func rmAgg(in ast.Node) bool {
	changed := false
	if selectStmt, ok := in.(*ast.SelectStmt); ok {
		if selectStmt.GroupBy != nil {
			changed = true
			selectStmt.GroupBy = nil
		}
	}
	if aggFunExpr, ok := in.(*ast.AggregateFuncExpr); ok {
		changed = true
		aggFunExpr.F = ""
		aggFunExpr.Distinct = false
		aggFunExpr.Order = nil
		aggFunExpr.Args = []ast.ExprNode{
			&test_driver.ValueExpr{Datum: test_driver.NewDatum(1)},
		}
	}
	return changed
}
