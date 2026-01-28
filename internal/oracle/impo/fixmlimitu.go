package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"

	"github.com/pkg/errors"
	"reflect"
)

const limitExpandMax = int64(2147483647)

// addFixMLimitU: FixMLimitU, *ast.SelectStmt: expand LIMIT in safe subquery.
func (v *MutateVisitor) addFixMLimitU(in *ast.SelectStmt, flag int) {
	if in == nil {
		return
	}
	if !isSafeLimitExpansionSubquery(in) {
		return
	}
	v.addCandidate(FixMLimitU, 1, in, flag)
}

// doFixMLimitU: FixMLimitU, *ast.SelectStmt: LIMIT n -> LIMIT n+1 (capped).
func doFixMLimitU(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.SelectStmt:
		if in.Limit == nil || in.Limit.Count == nil {
			return nil, errors.New("[doFixMLimitU]limit is nil")
		}
		if in.Limit.Offset != nil {
			return nil, errors.New("[doFixMLimitU]limit offset not supported")
		}
		val, ok := in.Limit.Count.(*test_driver.ValueExpr)
		if !ok {
			return nil, errors.New("[doFixMLimitU]limit count not literal")
		}
		old := *val
		newVal, ok := expandLimitValue(val)
		if !ok {
			return nil, errors.New("[doFixMLimitU]limit count unsupported")
		}
		in.Limit.Count = &test_driver.ValueExpr{Datum: newVal}
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMLimitU]restore error")
		}
		in.Limit.Count = &old
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMLimitU]type nil")
	default:
		return nil, errors.New("[doFixMLimitU]type default " + reflect.TypeOf(in).String())
	}
}

func expandLimitValue(v *test_driver.ValueExpr) (test_driver.Datum, bool) {
	if v == nil {
		return test_driver.Datum{}, false
	}
	switch v.Kind() {
	case test_driver.KindInt64:
		cur := v.GetInt64()
		if cur <= 0 {
			return test_driver.Datum{}, false
		}
		if cur >= limitExpandMax {
			return test_driver.NewDatum(cur), true
		}
		return test_driver.NewDatum(cur + 1), true
	case test_driver.KindUint64:
		cur := v.GetUint64()
		if cur == 0 {
			return test_driver.Datum{}, false
		}
		if cur >= uint64(limitExpandMax) {
			return test_driver.NewDatum(cur), true
		}
		return test_driver.NewDatum(cur + 1), true
	default:
		return test_driver.Datum{}, false
	}
}

func isSafeLimitExpansionSubquery(sel *ast.SelectStmt) bool {
	if sel == nil {
		return false
	}
	if sel.Limit == nil || sel.Limit.Count == nil {
		return false
	}
	if sel.OrderBy == nil {
		return false
	}
	if sel.With != nil {
		return false
	}
	if sel.AfterSetOperator != nil {
		return false
	}
	if len(sel.WindowSpecs) > 0 {
		return false
	}
	return true
}
