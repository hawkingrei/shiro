package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMHaving1U: FixMHaving1U, *ast.SelectStmt: HAVING xxx -> HAVING  1
func (v *MutateVisitor) addFixMHaving1U(in *ast.SelectStmt, flag int) {
	if in.Having != nil && in.Having.Expr != nil {
		v.addCandidate(FixMHaving1U, 1, in, flag)
	}
}

// doFixMHaving1U: FixMHaving1U, *ast.SelectStmt: HAVING xxx -> HAVING 1
func doFixMHaving1U(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.SelectStmt:
		// check
		if in.Having == nil || in.Having.Expr == nil {
			return nil, errors.New("[doFixMHaving1U]sel.Having == nil || sel.Having.Expr == nil")
		}
		// mutate
		old := in.Having.Expr

		// HAVING xxx -> HAVING 1
		in.Having.Expr = &test_driver.ValueExpr{
			Datum: test_driver.NewDatum(1),
		}

		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMHaving1U]restore error")
		}
		// recover
		in.Having.Expr = old
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMHaving1U]type nil")
	default:
		return nil, errors.New("[doFixMHaving1U]type default " + reflect.TypeOf(in).String())
	}
}
