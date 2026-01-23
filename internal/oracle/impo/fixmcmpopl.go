package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMCmpOpL: FixMCmpOpL, *ast.BinaryOperationExpr, *ast.CompareSubqueryExpr: a {>=|<=} b -> a {>|<} b
func (v *MutateVisitor) addFixMCmpOpL(in ast.Node, flag int) {
	var myOp *opcode.Op
	switch in := in.(type) {
	case *ast.BinaryOperationExpr:
		myOp = &in.Op
	case *ast.CompareSubqueryExpr:
		myOp = &in.Op
	default:
		return
	}
	switch *myOp {
	case opcode.LE:
	case opcode.GE:
	default:
		return
	}
	v.addCandidate(FixMCmpOpL, 0, in, flag)
}

// doFixMCmpOpL: FixMCmpOpL, *ast.BinaryOperationExpr, *ast.CompareSubqueryExpr: a {>=|<=} b -> a {>|<} b
func doFixMCmpOpL(rootNode ast.Node, in ast.Node) ([]byte, error) {
	// check
	var myOp *opcode.Op
	switch in := in.(type) {
	case *ast.BinaryOperationExpr:
		myOp = &in.Op
	case *ast.CompareSubqueryExpr:
		myOp = &in.Op
	case nil:
		return nil, errors.New("[doFixMCmpOpL]type nil")
	default:
		return nil, errors.New("[doFixMCmpOpL]type default " + reflect.TypeOf(in).String())
	}

	oldOp := *myOp
	var newOp opcode.Op
	switch oldOp {
	case opcode.LE:
		newOp = opcode.LT
	case opcode.GE:
		newOp = opcode.GT
	default:
		return nil, errors.New("[doFixMCmpOpL]Op default " + oldOp.String())
	}
	// mutate
	*myOp = newOp
	sql, err := restore(rootNode)
	if err != nil {
		return nil, errors.Wrap(err, "[doFixMCmpOpL]restore error")
	}
	// recover
	*myOp = oldOp
	return sql, nil
}
