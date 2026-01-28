package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMAnyAll: FixMAnyAllU/FixMAnyAllL, *ast.CompareSubqueryExpr: toggle ALL/ANY.
func (v *MutateVisitor) addFixMAnyAll(in *ast.CompareSubqueryExpr, flag int) {
	if in == nil {
		return
	}
	if in.All {
		v.addCandidate(FixMAnyAllU, 1, in, flag)
	} else {
		v.addCandidate(FixMAnyAllL, 0, in, flag)
	}
}

// doFixMAnyAllU: FixMAnyAllU, ALL -> ANY.
func doFixMAnyAllU(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.CompareSubqueryExpr:
		old := in.All
		in.All = false
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMAnyAllU]restore error")
		}
		in.All = old
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMAnyAllU]type nil")
	default:
		return nil, errors.New("[doFixMAnyAllU]type default " + reflect.TypeOf(in).String())
	}
}

// doFixMAnyAllL: FixMAnyAllL, ANY -> ALL.
func doFixMAnyAllL(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.CompareSubqueryExpr:
		old := in.All
		in.All = true
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMAnyAllL]restore error")
		}
		in.All = old
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMAnyAllL]type nil")
	default:
		return nil, errors.New("[doFixMAnyAllL]type default " + reflect.TypeOf(in).String())
	}
}
