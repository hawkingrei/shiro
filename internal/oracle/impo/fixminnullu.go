package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMInNullU: FixMInNullU, *ast.PatternInExpr: in(x,x,x) -> in(x,x,x,null)
func (v *MutateVisitor) addFixMInNullU(in *ast.PatternInExpr, flag int) {
	if in.Sel == nil && in.List != nil {
		v.addCandidate(FixMInNullU, 1, in, flag)
	}
}

// doFixMInNullU: FixMInNullU, *ast.PatternInExpr: in(x,x,x) -> in(x,x,x,null)
func doFixMInNullU(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in.(type) {
	case *ast.PatternInExpr:
		pin := in.(*ast.PatternInExpr)
		// check
		if pin.Sel != nil || pin.List == nil {
			return nil, errors.New("[doFixMInNullU]pin.Sel != nil || pin.List == nil")
		}
		// mutate
		oldList := pin.List
		newList := make([]ast.ExprNode, 0)
		for _, expr := range oldList {
			newList = append(newList, expr)
		}
		// add null expr
		newList = append(newList, &test_driver.ValueExpr{
			Datum: test_driver.NewDatum(nil),
		})
		pin.List = newList
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMInNullU]restore error")
		}
		// recover
		pin.List = oldList
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMInNullU]type nil")
	default:
		return nil, errors.New("[doFixMInNullU]type default " + reflect.TypeOf(in).String())
	}
}
