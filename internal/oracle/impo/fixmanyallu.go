package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMAnyAll: FixMAnyAllU/FixMAnyAllL, *ast.CompareSubqueryExpr: toggle ALL/ANY.
func (v *MutateVisitor) addFixMAnyAll(in *ast.CompareSubqueryExpr, flag int) {
	if in == nil || !isSafeAnyAllToggleSubquery(in) {
		return
	}
	if in.All {
		v.addCandidate(FixMAnyAllU, 1, in, flag)
	} else {
		v.addCandidate(FixMAnyAllL, 0, in, flag)
	}
}

// doFixMAnyAllU: FixMAnyAllU, ALL -> ANY.
func doFixMAnyAllU(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.CompareSubqueryExpr:
		old := in.All
		in.All = false
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMAnyAllU]restore error")
		}
		in.All = old
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMAnyAllU]type nil")
	default:
		return nil, errors.New("[doFixMAnyAllU]type default " + reflect.TypeOf(in).String())
	}
}

// doFixMAnyAllL: FixMAnyAllL, ANY -> ALL.
func doFixMAnyAllL(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.CompareSubqueryExpr:
		old := in.All
		in.All = true
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMAnyAllL]restore error")
		}
		in.All = old
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMAnyAllL]type nil")
	default:
		return nil, errors.New("[doFixMAnyAllL]type default " + reflect.TypeOf(in).String())
	}
}

func isSafeAnyAllToggleSubquery(in *ast.CompareSubqueryExpr) bool {
	if in == nil {
		return false
	}
	subq, ok := in.R.(*ast.SubqueryExpr)
	if !ok || subq == nil {
		return false
	}
	return subqueryGuaranteedNonEmpty(subq)
}

func subqueryGuaranteedNonEmpty(subq *ast.SubqueryExpr) bool {
	if subq == nil {
		return false
	}
	sel, ok := subq.Query.(*ast.SelectStmt)
	if !ok {
		return false
	}
	return selectGuaranteedNonEmpty(sel)
}

func selectGuaranteedNonEmpty(sel *ast.SelectStmt) bool {
	if sel == nil {
		return false
	}
	if sel.With != nil {
		return false
	}
	if sel.AfterSetOperator != nil {
		return false
	}
	if !limitAllowsRows(sel.Limit) {
		return false
	}
	if sel.Having != nil {
		return false
	}
	if sel.From == nil {
		return true
	}
	if sel.GroupBy != nil {
		return false
	}
	return selectHasAggregateField(sel)
}

func limitAllowsRows(limit *ast.Limit) bool {
	if limit == nil {
		return true
	}
	count, ok := limitLiteralValue(limit.Count)
	if !ok || count <= 0 {
		return false
	}
	if limit.Offset == nil {
		return true
	}
	offset, ok := limitLiteralValue(limit.Offset)
	return ok && offset == 0
}

func limitLiteralValue(expr ast.ExprNode) (int64, bool) {
	val, ok := expr.(*test_driver.ValueExpr)
	if !ok || val == nil {
		return 0, false
	}
	switch val.Kind() {
	case test_driver.KindInt64:
		return val.GetInt64(), true
	case test_driver.KindUint64:
		u := val.GetUint64()
		if u > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(u), true
	default:
		return 0, false
	}
}

func selectHasAggregateField(sel *ast.SelectStmt) bool {
	if sel == nil || sel.Fields == nil {
		return false
	}
	for _, field := range sel.Fields.Fields {
		if field == nil || field.Expr == nil {
			continue
		}
		if exprHasAggregate(field.Expr) {
			return true
		}
	}
	return false
}

func exprHasAggregate(expr ast.ExprNode) bool {
	if expr == nil {
		return false
	}
	visitor := &aggregateFinder{}
	expr.Accept(visitor)
	return visitor.found
}

type aggregateFinder struct {
	found bool
}

func (v *aggregateFinder) Enter(in ast.Node) (ast.Node, bool) {
	if v.found {
		return in, true
	}
	if _, ok := in.(*ast.AggregateFuncExpr); ok {
		v.found = true
		return in, true
	}
	return in, false
}

func (v *aggregateFinder) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
