package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMDistinctL: FixMDistinctL: *ast.SelectStmt: Distinct false -> true
func (v *MutateVisitor) addFixMDistinctL(in *ast.SelectStmt, flag int) {
	// ERROR 3065 (HY000): Expression #1 of ORDER BY clause is not in SELECT list,
	// references column xxx which is not in SELECT list; this is incompatible with DISTINCT
	// order by + distinct may error
	// with + distinct is hard to simplify, ban it.
	if !in.Distinct && in.OrderBy == nil && in.With == nil {
		v.addCandidate(FixMDistinctL, 0, in, flag)
	}
}

// doFixMDistinctL: FixMDistinctL: *ast.SelectStmt: Distinct false -> true
func doFixMDistinctL(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in.(type) {
	case *ast.SelectStmt:
		sel := in.(*ast.SelectStmt)
		// check
		if sel.Distinct {
			return nil, errors.New("[doFixMDistinctL]in.Distinct is true")
		}
		// mutate
		sel.Distinct = true
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMDistinctL]restore error")
		}
		// recover
		sel.Distinct = false
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMDistinctL]type nil")
	default:
		return nil, errors.New("[doFixMDistinctL]type default " + reflect.TypeOf(in).String())
	}
}
