package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// rmHaving drops HAVING to avoid alias or aggregate dependencies after rewrites.
func rmHaving(in ast.Node) bool {
	if selectStmt, ok := in.(*ast.SelectStmt); ok {
		if selectStmt.Having != nil {
			selectStmt.Having = nil
			return true
		}
	}
	return false
}
