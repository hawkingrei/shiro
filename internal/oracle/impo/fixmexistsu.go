package impo

import (
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pkg/errors"
	"reflect"
)

// addFixMExistsU: FixMExistsU, *ast.ExistsSubqueryExpr: EXISTS -> EXISTS(SELECT 1).
func (v *MutateVisitor) addFixMExistsU(in *ast.ExistsSubqueryExpr, flag int) {
	if in != nil {
		v.addCandidate(FixMExistsU, 1, in, flag)
	}
}

// doFixMExistsU: FixMExistsU, replace subquery with SELECT 1 (always non-empty).
func doFixMExistsU(rootNode ast.Node, in ast.Node) ([]byte, error) {
	switch in := in.(type) {
	case *ast.ExistsSubqueryExpr:
		oldSel := in.Sel
		subq, err := buildConstSubquery(true)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMExistsU]build subquery")
		}
		in.Sel = subq
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doFixMExistsU]restore error")
		}
		in.Sel = oldSel
		return sql, nil
	case nil:
		return nil, errors.New("[doFixMExistsU]type nil")
	default:
		return nil, errors.New("[doFixMExistsU]type default " + reflect.TypeOf(in).String())
	}
}

func buildConstSubquery(alwaysTrue bool) (*ast.SubqueryExpr, error) {
	sql := "SELECT 1"
	if !alwaysTrue {
		sql = "SELECT 1 WHERE 1=0"
	}
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	if len(stmtNodes) == 0 {
		return nil, errors.New("empty const subquery")
	}
	root, ok := stmtNodes[0].(ast.ResultSetNode)
	if !ok {
		return nil, errors.New("const subquery is not a ResultSetNode")
	}
	return &ast.SubqueryExpr{Query: root}, nil
}
