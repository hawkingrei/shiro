package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMDistinctU: FixMDistinctU, *ast.SelectStmt: Distinct true -> false
func (v *MutateVisitor) addFixMDistinctU(in *ast.SelectStmt, flag int) {
	if in.Distinct {
		v.addCandidate(FixMDistinctU, 1, in, flag)
	}
}

// doFixMDistinctU: FixMDistinctU, *ast.SelectStmt: Distinct true -> false
func doFixMDistinctU(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in.(type) {
	case *ast.SelectStmt:
		sel := in.(*ast.SelectStmt)
		// check
		if !sel.Distinct {
			return nil, errors.New("[doFixMDistinctU]in.Distinct is false")
		}
		// mutate
		sel.Distinct = false
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMDistinctU]restore error")
		}
		// recover
		sel.Distinct = true
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMDistinctU]type nil")
	default:
		return nil, errors.New("[doFixMDistinctU]type default " + reflect.TypeOf(in).String())
	}
}
