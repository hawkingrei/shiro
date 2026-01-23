package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
)

// rmWindow removes window functions and WINDOW specs.
func rmWindow(in ast.Node) bool {
	changed := false
	if selectStmt, ok := in.(*ast.SelectStmt); ok {
		if selectStmt.WindowSpecs != nil {
			changed = true
			selectStmt.WindowSpecs = nil
		}
	}
	if fieldList, ok := in.(*ast.FieldList); ok {
		for _, field := range fieldList.Fields {
			if field.Expr == nil {
				continue
			}
			if _, ok := field.Expr.(*ast.WindowFuncExpr); ok {
				changed = true
				field.Expr = &test_driver.ValueExpr{Datum: test_driver.NewDatum(1)}
			}
		}
	}
	return changed
}
