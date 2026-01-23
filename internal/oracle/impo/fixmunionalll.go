package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMUnionAllL: FixMUnionAllL: *ast.SelectStmt: AfterSetOperator UNION ALL -> UNION
func (v *MutateVisitor) addFixMUnionAllL(in *ast.SelectStmt, flag int) {
	if in.AfterSetOperator != nil && *in.AfterSetOperator == ast.UnionAll {
		v.addCandidate(FixMUnionAllL, 0, in, flag)
	}
}

// doFixMUnionAllL: FixMUnionAllL, *ast.SelectStmt: AfterSetOperator UNION ALL -> UNION
func doFixMUnionAllL(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in.(type) {
	case *ast.SelectStmt:
		sel := in.(*ast.SelectStmt)
		// check
		if sel.AfterSetOperator == nil || *sel.AfterSetOperator != ast.UnionAll {
			return nil, errors.New("[doFixMUnionAllL]sel.AfterSetOperator == nil || *sel.AfterSetOperator != ast.UnionAll")
		}
		// mutate
		*sel.AfterSetOperator = ast.Union
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMUnionAllL]restore error")
		}
		// recover
		*sel.AfterSetOperator = ast.UnionAll
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMUnionAllL]type nil")
	default:
		return nil, errors.New("[doFixMUnionAllL]type default " + reflect.TypeOf(in).String())
	}
}
