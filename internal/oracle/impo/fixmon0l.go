package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMOn0L: FixMOn0L, *ast.Join: ON xxx -> ON 0
func (v *MutateVisitor) addFixMOn0L(in *ast.Join, flag int) {
	if in.On != nil {
		v.addCandidate(FixMOn0L, 0, in, flag)
	}
}

// doFixMOn0L: FixMOn0L, *ast.Join: ON xxx -> ON 0
func doFixMOn0L(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.Join:
		// check
		if in.On == nil {
			return nil, errors.New("[FixMOn0L]join.On == nil")
		}
		// mutate
		old := in.On.Expr

		// ON xxx -> ON 0
		in.On.Expr = &test_driver.ValueExpr{
			Datum: test_driver.NewDatum(0),
		}

		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[FixMOn0L]restore error")
		}
		// recover
		in.On.Expr = old
		return sql, nil
	case nil:
		return nil, errors.New("[FixMOn0L]type nil")
	default:
		return nil, errors.New("[FixMOn0L]type default " + reflect.TypeOf(in).String())
	}
}
