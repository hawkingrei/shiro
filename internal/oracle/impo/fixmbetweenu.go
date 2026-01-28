package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMBetweenU: FixMBetweenU, *ast.BetweenExpr: widen bounds.
func (v *MutateVisitor) addFixMBetweenU(in *ast.BetweenExpr, flag int) {
	if in != nil {
		v.addCandidate(FixMBetweenU, 1, in, flag)
	}
}

// doFixMBetweenU: FixMBetweenU, *ast.BetweenExpr: widen bounds for numeric literals.
func doFixMBetweenU(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.BetweenExpr:
		left, lok := in.Left.(*test_driver.ValueExpr)
		right, rok := in.Right.(*test_driver.ValueExpr)
		if !lok || !rok {
			return nil, errors.New("[doFixMBetweenU]bounds are not literal")
		}
		oldLeft := left.Datum
		oldRight := right.Datum
		newLeft, okLeft := shiftDatum(oldLeft, -1)
		newRight, okRight := shiftDatum(oldRight, 1)
		if !okLeft && !okRight {
			return nil, errors.New("[doFixMBetweenU]unable to widen bounds")
		}
		if okLeft {
			left.Datum = newLeft
		}
		if okRight {
			right.Datum = newRight
		}
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMBetweenU]restore error")
		}
		left.Datum = oldLeft
		right.Datum = oldRight
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMBetweenU]type nil")
	default:
		return nil, errors.New("[doFixMBetweenU]type default " + reflect.TypeOf(in).String())
	}
}

func shiftDatum(d test_driver.Datum, delta int64) (test_driver.Datum, bool) {
	switch d.Kind() {
	case test_driver.KindInt64:
		return test_driver.NewDatum(d.GetInt64() + delta), true
	case test_driver.KindUint64:
		cur := d.GetUint64()
		if delta < 0 && cur == 0 {
			return test_driver.Datum{}, false
		}
		if delta < 0 && cur < uint64(-delta) {
			return test_driver.Datum{}, false
		}
		return test_driver.NewDatum(uint64(int64(cur) + delta)), true
	case test_driver.KindFloat64:
		return test_driver.NewDatum(d.GetFloat64() + float64(delta)), true
	case test_driver.KindFloat32:
		return test_driver.NewDatum(d.GetFloat32() + float32(delta)), true
	default:
		return test_driver.Datum{}, false
	}
}
