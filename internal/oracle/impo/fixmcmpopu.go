package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMCmpOpU: FixMCmpOpU, *ast.BinaryOperationExpr, *ast.CompareSubqueryExpr: a {>|<|=} b -> a {>=|<=|>=} b
func (v *MutateVisitor) addFixMCmpOpU(in ast.Node, flag int) {
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
	case opcode.EQ:
	case opcode.LT:
	case opcode.GT:
	default:
		return
	}
	v.addCandidate(FixMCmpOpU, 1, in, flag)
}

// doFixMCmpOpU: FixMCmpOpU, *ast.BinaryOperationExpr, *ast.CompareSubqueryExpr: a {>|<|=} b -> a {>=|<=|>=} b
func doFixMCmpOpU(rootNode ast.Node, in ast.Node) ([]byte, error) {
	// check
	var myOp *opcode.Op
	switch in := in.(type) {
	case *ast.BinaryOperationExpr:
		myOp = &in.Op
	case *ast.CompareSubqueryExpr:
		myOp = &in.Op
	case nil:
		return nil, errors.New("[doFixMCmpOpU]type error nil")
	default:
		return nil, errors.New("[doFixMCmpOpU]type default " + reflect.TypeOf(in).String())
	}

	oldOp := *myOp
	var newOp opcode.Op
	switch oldOp {
	case opcode.EQ:
		newOp = opcode.GE
	case opcode.LT:
		newOp = opcode.LE
	case opcode.GT:
		newOp = opcode.GE
	default:
		return nil, errors.New("[doFixMCmpOpU]Op default " + oldOp.String())
	}
	// mutate
	*myOp = newOp
	sql, err := restore(rootNode)
	if err != nil {
		return nil, errors.Wrap(err, "[doFixMCmpOpU]restore error")
	}
	// recover
	*myOp = oldOp
	return sql, nil
}
