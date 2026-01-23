package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// rmLRJoin rewrites LEFT/RIGHT JOIN into CROSS JOIN.
func rmLRJoin(in ast.Node) bool {
	if join, ok := in.(*ast.Join); ok {
		if join.Tp == ast.LeftJoin || join.Tp == ast.RightJoin {
			join.Tp = ast.CrossJoin
			join.NaturalJoin = false
			join.StraightJoin = false
			return true
		}
	}
	return false
}
