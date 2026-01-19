package generator

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

// TestGeneratorFixedCases validates fixed SQL patterns used for generator constraints.
func TestGeneratorFixedCases(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "distinct-order-by",
			sql:  "SELECT DISTINCT a, b FROM t ORDER BY a, b",
		},
		{
			name: "having-aggregate",
			sql:  "SELECT a, SUM(b) FROM t GROUP BY a HAVING SUM(b) > 1",
		},
		{
			name: "cte-usage",
			sql:  "WITH cte AS (SELECT id FROM t) SELECT id FROM cte",
		},
		{
			name: "datetime-compare",
			sql:  "SELECT * FROM t WHERE dt > '2024-01-01 12:00:00'",
		},
		{
			name: "bool-compare",
			sql:  "SELECT * FROM t WHERE flag != 0",
		},
		{
			name: "numeric-func",
			sql:  "SELECT ABS(n) FROM t",
		},
		{
			name: "string-func",
			sql:  "SELECT LOWER(s) FROM t",
		},
	}

	p := parser.New()
	for _, tc := range cases {
		stmt, err := parseSelect(p, tc.sql)
		if err != nil {
			t.Fatalf("%s parse failed: %v\nsql=%s", tc.name, err, tc.sql)
		}
		if err := validateDistinctOrderBy(stmt); err != nil {
			t.Fatalf("%s: %v\nsql=%s", tc.name, err, tc.sql)
		}
		if err := validateHavingColumns(stmt); err != nil {
			t.Fatalf("%s: %v\nsql=%s", tc.name, err, tc.sql)
		}
		if err := validateCTEUsage(stmt); err != nil {
			t.Fatalf("%s: %v\nsql=%s", tc.name, err, tc.sql)
		}
	}
}

func parseSelect(p *parser.Parser, sql string) (*ast.SelectStmt, error) {
	nodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, err
	}
	switch v := nodes[0].(type) {
	case *ast.SelectStmt:
		return v, nil
	case *ast.SetOprStmt:
		if v.SelectList != nil && len(v.SelectList.Selects) > 0 {
			if sel, ok := v.SelectList.Selects[0].(*ast.SelectStmt); ok {
				return sel, nil
			}
		}
	}
	return nil, nil
}

func validateDistinctOrderBy(stmt *ast.SelectStmt) error {
	if stmt == nil || !stmt.Distinct || stmt.OrderBy == nil || stmt.Fields == nil {
		return nil
	}
	selectSet := make(map[string]struct{}, len(stmt.Fields.Fields))
	for _, f := range stmt.Fields.Fields {
		if f == nil || f.Expr == nil {
			continue
		}
		selectSet[restoreExpr(f.Expr)] = struct{}{}
	}
	for _, ob := range stmt.OrderBy.Items {
		if ob == nil || ob.Expr == nil {
			continue
		}
		if _, ok := selectSet[restoreExpr(ob.Expr)]; !ok {
			return errf("distinct order by not in select list")
		}
	}
	return nil
}

func validateHavingColumns(stmt *ast.SelectStmt) error {
	if stmt == nil || stmt.Having == nil {
		return nil
	}
	groupSet := make(map[string]struct{})
	if stmt.GroupBy != nil {
		for _, item := range stmt.GroupBy.Items {
			if item == nil || item.Expr == nil {
				continue
			}
			groupSet[restoreExpr(item.Expr)] = struct{}{}
		}
	}
	if len(groupSet) == 0 {
		return errf("having without group by")
	}
	if usesNonGroupColumn(stmt.Having.Expr, groupSet, false) {
		return errf("having uses non-group column")
	}
	return nil
}

func usesNonGroupColumn(expr ast.ExprNode, groupSet map[string]struct{}, inAgg bool) bool {
	switch v := expr.(type) {
	case *ast.ColumnNameExpr:
		if inAgg {
			return false
		}
		_, ok := groupSet[restoreExpr(v)]
		return !ok
	case *ast.FuncCallExpr:
		nextAgg := inAgg || isAggFunc(v.FnName.O)
		for _, arg := range v.Args {
			if usesNonGroupColumn(arg, groupSet, nextAgg) {
				return true
			}
		}
		return false
	case *ast.BinaryOperationExpr:
		return usesNonGroupColumn(v.L, groupSet, inAgg) || usesNonGroupColumn(v.R, groupSet, inAgg)
	case *ast.UnaryOperationExpr:
		return usesNonGroupColumn(v.V, groupSet, inAgg)
	case *ast.CaseExpr:
		if v.Value != nil && usesNonGroupColumn(v.Value, groupSet, inAgg) {
			return true
		}
		for _, w := range v.WhenClauses {
			if w == nil {
				continue
			}
			if w.Expr != nil && usesNonGroupColumn(w.Expr, groupSet, inAgg) {
				return true
			}
			if w.Result != nil && usesNonGroupColumn(w.Result, groupSet, inAgg) {
				return true
			}
		}
		if v.ElseClause != nil && usesNonGroupColumn(v.ElseClause, groupSet, inAgg) {
			return true
		}
		return false
	default:
		return false
	}
}

func isAggFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}

func validateCTEUsage(stmt *ast.SelectStmt) error {
	if stmt == nil || stmt.With == nil {
		return nil
	}
	names := cteNameSet(stmt.With)
	if len(names) == 0 {
		return nil
	}
	if stmt.From == nil || stmt.From.TableRefs == nil {
		return errf("cte defined but not referenced")
	}
	if !tableRefsHasCTE(stmt.From.TableRefs, names) {
		return errf("cte defined but not referenced")
	}
	return nil
}

func cteNameSet(with *ast.WithClause) map[string]struct{} {
	out := make(map[string]struct{})
	if with == nil {
		return out
	}
	for _, cte := range with.CTEs {
		if cte == nil {
			continue
		}
		name := strings.TrimSpace(cte.Name.O)
		if name != "" {
			out[name] = struct{}{}
		}
	}
	return out
}

func tableRefsHasCTE(join *ast.Join, names map[string]struct{}) bool {
	if join == nil {
		return false
	}
	if resultSetHasCTE(join.Left, names) || resultSetHasCTE(join.Right, names) {
		return true
	}
	return false
}

func resultSetHasCTE(node ast.ResultSetNode, names map[string]struct{}) bool {
	if node == nil {
		return false
	}
	switch v := node.(type) {
	case *ast.TableSource:
		return tableSourceHasCTE(v, names)
	case *ast.Join:
		return tableRefsHasCTE(v, names)
	default:
		return false
	}
}

func tableSourceHasCTE(source *ast.TableSource, names map[string]struct{}) bool {
	if source == nil {
		return false
	}
	switch v := source.Source.(type) {
	case *ast.TableName:
		_, ok := names[v.Name.O]
		return ok
	case *ast.Join:
		return tableRefsHasCTE(v, names)
	case *ast.SelectStmt:
		if v.From != nil && v.From.TableRefs != nil {
			return tableRefsHasCTE(v.From.TableRefs, names)
		}
	case *ast.SetOprStmt:
		if v.SelectList != nil {
			for _, sel := range v.SelectList.Selects {
				if s, ok := sel.(*ast.SelectStmt); ok {
					if s.From != nil && s.From.TableRefs != nil && tableRefsHasCTE(s.From.TableRefs, names) {
						return true
					}
				}
			}
		}
	}
	return false
}

func restoreExpr(expr ast.ExprNode) string {
	var b strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &b)
	_ = expr.Restore(ctx)
	return b.String()
}

func errf(msg string) error {
	return &fixedCaseError{msg: msg}
}

type fixedCaseError struct {
	msg string
}

func (e *fixedCaseError) Error() string {
	return e.msg
}
