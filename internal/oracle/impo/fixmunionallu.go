package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMUnionAllU: FixMUnionAllU, *ast.SelectStmt: AfterSetOperator UNION -> UNION ALL
func (v *MutateVisitor) addFixMUnionAllU(in *ast.SelectStmt, flag int) {
	if in.AfterSetOperator != nil && *in.AfterSetOperator == ast.Union {
		v.addCandidate(FixMUnionAllU, 1, in, flag)
	}
}

// doFixMUnionAllU: FixMUnionAllU, *ast.SelectStmt: AfterSetOperator UNION -> UNION ALL
func doFixMUnionAllU(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in.(type) {
	case *ast.SelectStmt:
		sel := in.(*ast.SelectStmt)
		// check
		if sel.AfterSetOperator == nil || *sel.AfterSetOperator != ast.Union {
			return nil, errors.New("[doFixMUnionAllU]sel.AfterSetOperator == nil || *sel.AfterSetOperator != ast.Union")
		}
		// mutate
		*sel.AfterSetOperator = ast.UnionAll
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMUnionAllU]restore error")
		}
		// recover
		*sel.AfterSetOperator = ast.Union
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMUnionAllU]type nil")
	default:
		return nil, errors.New("[doFixMUnionAllU]type default " + reflect.TypeOf(in).String())
	}
}
