package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"

	"github.com/pkg/errors"
	"reflect"
)

// addFixMRmUnionL: FixMRmUnionL, *ast.SetOprSelectList: remove Selects[1:] for UNION
func (v *MutateVisitor) addFixMRmUnionL(in *ast.SetOprSelectList, flag int) {
	if len(in.Selects) == 2 {
		if sel, ok := in.Selects[1].(*ast.SelectStmt); ok {
			if sel.AfterSetOperator != nil && *sel.AfterSetOperator == ast.Union {
				v.addCandidate(FixMRmUnionL, 0, in, flag)
			}
		}
	}
}

// doFixMRmUnionL: FixMRmUnionL, *ast.SetOprSelectList: remove Selects[1:] for UNION
func doFixMRmUnionL(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.SetOprSelectList:
		if len(in.Selects) <= 1 {
			return nil, errors.New("[doFixMRmUnionL]len(lst.Selects) <= 1")
		}
		oldSels := in.Selects
		newSels := make([]ast.Node, 0, 1)
		newSels = append(newSels, oldSels[0])
		in.Selects = newSels
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMRmUnionL]restore error")
		}
		in.Selects = oldSels
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMRmUnionL]type nil")
	default:
		return nil, errors.New("[doFixMRmUnionL]type default " + reflect.TypeOf(in).String())
	}
}
