package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

// InitVisitor normalizes unsupported constructs for later mutation.
type InitVisitor struct{}

// Enter applies normalized rewrites and stops recursion for each node.
func (v *InitVisitor) Enter(in ast.Node) (ast.Node, bool) {
	rmAgg(in)
	rmHaving(in)
	rmOrderBy(in)
	rmWindow(in)
	rmLRJoin(in)
	rmLimit(in)
	rmUncertain(in)
	return in, false
}

// Leave keeps the node unchanged.
func (v *InitVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
