package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// rmUncertain is a placeholder for removing non-deterministic functions.
func rmUncertain(in ast.Node) bool {
	return false
}
