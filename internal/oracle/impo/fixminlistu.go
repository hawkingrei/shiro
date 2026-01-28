package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMInListU: FixMInListU, *ast.PatternInExpr: in(x, ...) -> in(x, ..., newValue)
func (v *MutateVisitor) addFixMInListU(in *ast.PatternInExpr, flag int) {
	if in.Sel == nil && len(in.List) > 0 {
		if hasLiteralValueExpr(in.List) {
			v.addCandidate(FixMInListU, 1, in, flag)
		}
	}
}

// doFixMInListU: FixMInListU, *ast.PatternInExpr: append a new literal value.
func doFixMInListU(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.PatternInExpr:
		if in.Sel != nil || len(in.List) == 0 {
			return nil, errors.New("[doFixMInListU]pin.Sel != nil || pin.List == nil")
		}
		newExpr := buildExpandedLiteral(in.List)
		if newExpr == nil {
			return nil, errors.New("[doFixMInListU]no suitable literal")
		}
		oldList := in.List
		in.List = append(append([]ast.ExprNode{}, oldList...), newExpr)
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMInListU]restore error")
		}
		in.List = oldList
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMInListU]type nil")
	default:
		return nil, errors.New("[doFixMInListU]type default " + reflect.TypeOf(in).String())
	}
}

func hasLiteralValueExpr(list []ast.ExprNode) bool {
	for _, expr := range list {
		if _, ok := expr.(*test_driver.ValueExpr); ok {
			return true
		}
	}
	return false
}

func buildExpandedLiteral(list []ast.ExprNode) ast.ExprNode {
	for _, expr := range list {
		val, ok := expr.(*test_driver.ValueExpr)
		if !ok {
			continue
		}
		d := val.Datum
		switch d.Kind() {
		case test_driver.KindInt64:
			return &test_driver.ValueExpr{Datum: test_driver.NewDatum(d.GetInt64() + 1)}
		case test_driver.KindUint64:
			return &test_driver.ValueExpr{Datum: test_driver.NewDatum(d.GetUint64() + 1)}
		case test_driver.KindFloat64:
			return &test_driver.ValueExpr{Datum: test_driver.NewDatum(d.GetFloat64() + 1)}
		case test_driver.KindFloat32:
			return &test_driver.ValueExpr{Datum: test_driver.NewDatum(d.GetFloat32() + 1)}
		case test_driver.KindString:
			return &test_driver.ValueExpr{Datum: test_driver.NewDatum(d.GetString() + "_x")}
		}
	}
	return nil
}
