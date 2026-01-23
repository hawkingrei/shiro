package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// rmOrderBy drops ORDER BY to avoid alias or aggregate dependencies after rewrites.
func rmOrderBy(in ast.Node) bool {
	if selectStmt, ok := in.(*ast.SelectStmt); ok {
		if selectStmt.OrderBy != nil {
			selectStmt.OrderBy = nil
			return true
		}
	}
	return false
}
