package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
)

// rmLimit replaces LIMIT with a large count to avoid trimming results.
func rmLimit(in ast.Node) bool {
	if limit, ok := in.(*ast.Limit); ok {
		limit.Count = &test_driver.ValueExpr{Datum: test_driver.NewDatum(2147483647)}
		limit.Offset = nil
		return true
	}
	return false
}
