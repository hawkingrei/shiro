package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"

	"github.com/pkg/errors"
	"reflect"
)

// addFixMRmOrderByL: FixMRmOrderByL, *ast.SelectStmt: remove ORDER BY in safe subquery.
func (v *MutateVisitor) addFixMRmOrderByL(in *ast.SelectStmt, flag int) {
	if in == nil {
		return
	}
	if !isSafeOrderByRemovalSubquery(in) {
		return
	}
	v.addCandidate(FixMRmOrderByL, 0, in, flag)
}

// doFixMRmOrderByL: FixMRmOrderByL, *ast.SelectStmt: ORDER BY -> nil.
func doFixMRmOrderByL(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.SelectStmt:
		if in.OrderBy == nil {
			return nil, errors.New("[doFixMRmOrderByL]order by is nil")
		}
		oldOrder := in.OrderBy
		in.OrderBy = nil
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMRmOrderByL]restore error")
		}
		in.OrderBy = oldOrder
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMRmOrderByL]type nil")
	default:
		return nil, errors.New("[doFixMRmOrderByL]type default " + reflect.TypeOf(in).String())
	}
}

func isSafeOrderByRemovalSubquery(sel *ast.SelectStmt) bool {
	if sel == nil {
		return false
	}
	if sel.OrderBy == nil {
		return false
	}
	if sel.Limit != nil {
		return false
	}
	if sel.With != nil {
		return false
	}
	if sel.AfterSetOperator != nil {
		return false
	}
	if len(sel.WindowSpecs) > 0 {
		return false
	}
	return true
}
