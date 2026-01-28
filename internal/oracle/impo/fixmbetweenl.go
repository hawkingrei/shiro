package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMBetweenL: FixMBetweenL, *ast.BetweenExpr: tighten bounds.
func (v *MutateVisitor) addFixMBetweenL(in *ast.BetweenExpr, flag int) {
	if in != nil {
		v.addCandidate(FixMBetweenL, 0, in, flag)
	}
}

// doFixMBetweenL: FixMBetweenL, *ast.BetweenExpr: tighten bounds for numeric literals.
func doFixMBetweenL(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.BetweenExpr:
		left, lok := in.Left.(*test_driver.ValueExpr)
		right, rok := in.Right.(*test_driver.ValueExpr)
		if !lok || !rok {
			return nil, errors.New("[doFixMBetweenL]bounds are not literal")
		}
		oldLeft := left.Datum
		oldRight := right.Datum
		newLeft, okLeft := shiftDatum(oldLeft, 1)
		newRight, okRight := shiftDatum(oldRight, -1)
		if !okLeft && !okRight {
			return nil, errors.New("[doFixMBetweenL]unable to tighten bounds")
		}
		if okLeft {
			left.Datum = newLeft
		}
		if okRight {
			right.Datum = newRight
		}
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMBetweenL]restore error")
		}
		left.Datum = oldLeft
		right.Datum = oldRight
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMBetweenL]type nil")
	default:
		return nil, errors.New("[doFixMBetweenL]type default " + reflect.TypeOf(in).String())
	}
}
