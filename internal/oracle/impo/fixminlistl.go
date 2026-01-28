package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMInListL: FixMInListL, *ast.PatternInExpr: in(x, ...) -> remove one element.
func (v *MutateVisitor) addFixMInListL(in *ast.PatternInExpr, flag int) {
	if in.Sel == nil && len(in.List) > 1 {
		v.addCandidate(FixMInListL, 0, in, flag)
	}
}

// doFixMInListL: FixMInListL, *ast.PatternInExpr: drop the last list element.
func doFixMInListL(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.PatternInExpr:
		if in.Sel != nil || len(in.List) <= 1 {
			return nil, errors.New("[doFixMInListL]pin.Sel != nil || len(pin.List) <= 1")
		}
		oldList := in.List
		in.List = append([]ast.ExprNode{}, oldList[:len(oldList)-1]...)
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMInListL]restore error")
		}
		in.List = oldList
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMInListL]type nil")
	default:
		return nil, errors.New("[doFixMInListL]type default " + reflect.TypeOf(in).String())
	}
}
