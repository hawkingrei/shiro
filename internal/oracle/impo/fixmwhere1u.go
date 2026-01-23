package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMWhere1U: FixMWhere1U, *ast.SelectStmt: WHERE xxx -> WHERE 1
func (v *MutateVisitor) addFixMWhere1U(in *ast.SelectStmt, flag int) {
	if in.Where != nil {
		v.addCandidate(FixMWhere1U, 1, in, flag)
	}
}

// doFixMWhere1U: FixMWhere1U, *ast.SelectStmt: WHERE xxx -> WHERE 1
func doFixMWhere1U(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.SelectStmt:
		// check
		if in.Where == nil {
			return nil, errors.New("[FixMWhere1U]sel.Where == nil")
		}
		// mutate
		old := in.Where

		// WHERE xxx -> WHERE 1
		in.Where = &test_driver.ValueExpr{
			Datum: test_driver.NewDatum(1),
		}

		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[FixMWhere1U]restore error")
		}
		// recover
		in.Where = old
		return sql, nil
	case nil:
		return nil, errors.New("[FixMWhere1U]type nil")
	default:
		return nil, errors.New("[FixMWhere1U]type default " + reflect.TypeOf(in).String())
	}
}
