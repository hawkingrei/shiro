package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

// InitVisitor normalizes unsupported constructs for later mutation.
type InitVisitor struct {
	DisableStage1 bool
	KeepLRJoin    bool
	keepAggStack  []bool
	keepLimitStack []bool
	subqDepth     int
}

// Enter applies normalized rewrites and stops recursion for each node.
func (v *InitVisitor) Enter(in ast.Node) (ast.Node, bool) {
	if v.DisableStage1 {
		return in, false
	}
	if _, ok := in.(*ast.SubqueryExpr); ok {
		v.subqDepth++
	}
	if sel, ok := in.(*ast.SelectStmt); ok {
		keepAgg := sel.GroupBy != nil || sel.Having != nil
		v.keepAggStack = append(v.keepAggStack, keepAgg)
		keepLimit := v.subqDepth > 0 && sel.OrderBy != nil
		v.keepLimitStack = append(v.keepLimitStack, keepLimit)
	}
	if !v.keepAgg() {
		rmAgg(in)
		rmHaving(in)
	}
	if v.subqDepth == 0 {
		rmOrderBy(in)
	}
	rmWindow(in)
	if !v.KeepLRJoin {
		rmLRJoin(in)
	}
	if !v.keepLimit() {
		rmLimit(in)
	}
	rmUncertain(in)
	return in, false
}

// Leave keeps the node unchanged.
func (v *InitVisitor) Leave(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*ast.SelectStmt); ok {
		if len(v.keepAggStack) > 0 {
			v.keepAggStack = v.keepAggStack[:len(v.keepAggStack)-1]
		}
		if len(v.keepLimitStack) > 0 {
			v.keepLimitStack = v.keepLimitStack[:len(v.keepLimitStack)-1]
		}
	}
	if _, ok := in.(*ast.SubqueryExpr); ok {
		if v.subqDepth > 0 {
			v.subqDepth--
		}
	}
	return in, true
}

func (v *InitVisitor) keepAgg() bool {
	if len(v.keepAggStack) == 0 {
		return false
	}
	return v.keepAggStack[len(v.keepAggStack)-1]
}

func (v *InitVisitor) keepLimit() bool {
	if len(v.keepLimitStack) == 0 {
		return false
	}
	return v.keepLimitStack[len(v.keepLimitStack)-1]
}
