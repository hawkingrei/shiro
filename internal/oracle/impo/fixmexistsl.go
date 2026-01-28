package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMExistsL: FixMExistsL, *ast.ExistsSubqueryExpr: EXISTS -> EXISTS(SELECT 1 WHERE 1=0).
func (v *MutateVisitor) addFixMExistsL(in *ast.ExistsSubqueryExpr, flag int) {
	if in != nil {
		v.addCandidate(FixMExistsL, 0, in, flag)
	}
}

// doFixMExistsL: FixMExistsL, replace subquery with empty-result SELECT.
func doFixMExistsL(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.ExistsSubqueryExpr:
		oldSel := in.Sel
		subq, err := buildConstSubquery(false)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMExistsL]build subquery")
		}
		in.Sel = subq
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMExistsL]restore error")
		}
		in.Sel = oldSel
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMExistsL]type nil")
	default:
		return nil, errors.New("[doFixMExistsL]type default " + reflect.TypeOf(in).String())
	}
}
