package impo

import (
	"math"

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
		cur := d.GetInt64()
		if delta > 0 && cur == math.MaxInt64 {
			return test_driver.Datum{}, false
		}
		if delta < 0 && cur == math.MinInt64 {
			return test_driver.Datum{}, false
		}
		return test_driver.NewDatum(cur + delta), true
	case test_driver.KindUint64:
		cur := d.GetUint64()
		if delta > 0 {
			add := uint64(delta)
			if cur > math.MaxUint64-add {
				return test_driver.Datum{}, false
			}
			return test_driver.NewDatum(cur + add), true
		}
		if delta < 0 {
			sub := uint64(-delta)
			if cur < sub {
				return test_driver.Datum{}, false
			}
			return test_driver.NewDatum(cur - sub), true
		}
		return test_driver.NewDatum(cur), true
	case test_driver.KindFloat64:
		val := d.GetFloat64() + float64(delta)
		if math.IsInf(val, 0) {
			return test_driver.Datum{}, false
		}
		return test_driver.NewDatum(val), true
	case test_driver.KindFloat32:
		val := d.GetFloat32() + float32(delta)
		if math.IsInf(float64(val), 0) {
			return test_driver.Datum{}, false
		}
		return test_driver.NewDatum(val), true
	default:
		return test_driver.Datum{}, false
	}
}
