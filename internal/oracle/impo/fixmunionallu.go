package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
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
	switch in := in.(type) {
	case *ast.SelectStmt:
		// check
		if in.AfterSetOperator == nil || *in.AfterSetOperator != ast.Union {
			return nil, errors.New("[doFixMUnionAllU]sel.AfterSetOperator == nil || *sel.AfterSetOperator != ast.Union")
		}
		// mutate
		*in.AfterSetOperator = ast.UnionAll
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMUnionAllU]restore error")
		}
		// recover
		*in.AfterSetOperator = ast.Union
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMUnionAllU]type nil")
	default:
		return nil, errors.New("[doFixMUnionAllU]type default " + reflect.TypeOf(in).String())
	}
}
