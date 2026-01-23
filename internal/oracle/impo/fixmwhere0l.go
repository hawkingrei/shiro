package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMWhere0L: FixMWhere0L: *ast.SelectStmt: WHERE xxx -> WHERE 0
func (v *MutateVisitor) addFixMWhere0L(in *ast.SelectStmt, flag int) {
	if in.Where != nil {
		v.addCandidate(FixMWhere0L, 0, in, flag)
	}
}

// doFixMWhere0L: FixMWhere0L: *ast.SelectStmt: WHERE xxx -> WHERE 0
func doFixMWhere0L(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.SelectStmt:
		// check
		if in.Where == nil {
			return nil, errors.New("[FixMWhere0L]sel.Where == nil")
		}
		// mutate
		old := in.Where

		// WHERE xxx -> WHERE 0
		in.Where = &test_driver.ValueExpr{
			Datum: test_driver.NewDatum(0),
		}

		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[FixMWhere0L]restore error")
		}
		// recover
		in.Where = old
		return sql, nil
	case nil:
		return nil, errors.New("[FixMWhere0L]type nil")
	default:
		return nil, errors.New("[FixMWhere0L]type default " + reflect.TypeOf(in).String())
	}
}
