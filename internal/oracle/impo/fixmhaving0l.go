package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMHaving0L: FixMHaving0L, *ast.SelectStmt: HAVING xxx -> HAVING 0
func (v *MutateVisitor) addFixMHaving0L(in *ast.SelectStmt, flag int) {
	if in.Having != nil && in.Having.Expr != nil {
		v.addCandidate(FixMHaving0L, 0, in, flag)
	}
}

// doFixMHaving0L: FixMHaving0L, *ast.SelectStmt: HAVING xxx -> HAVING 0
func doFixMHaving0L(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in.(type) {
	case *ast.SelectStmt:
		sel := in.(*ast.SelectStmt)
		// check
		if sel.Having == nil || sel.Having.Expr == nil {
			return nil, errors.New("[doFixMHaving0L]sel.Having == nil || sel.Having.Expr == nil")
		}
		// mutate
		old := sel.Having.Expr

		// HAVING xxx -> HAVING 0
		sel.Having.Expr = &test_driver.ValueExpr{
			Datum: test_driver.NewDatum(0),
		}

		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMHaving0L]restore error")
		}
		// recover
		sel.Having.Expr = old
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMHaving0L]type nil")
	default:
		return nil, errors.New("[doFixMHaving0L]type default " + reflect.TypeOf(in).String())
	}
}
