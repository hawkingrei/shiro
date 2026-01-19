package oracle

import (
	"fmt"
	"strings"

	"shiro/internal/generator"
	"shiro/internal/schema"
)

func buildExpr(expr generator.Expr) string {
	b := generator.SQLBuilder{}
	expr.Build(&b)
	return b.String()
}

func buildFrom(query *generator.SelectQuery) string {
	from := query.From.BaseTable
	for _, join := range query.From.Joins {
		from += fmt.Sprintf(" %s %s", join.Type, join.Table)
		if len(join.Using) > 0 {
			from += " USING (" + strings.Join(join.Using, ", ") + ")"
		} else if join.On != nil {
			from += " ON " + buildExpr(join.On)
		}
	}
	return from
}

func buildWith(query *generator.SelectQuery) string {
	if len(query.With) == 0 {
		return ""
	}
	parts := make([]string, 0, len(query.With))
	for _, cte := range query.With {
		parts = append(parts, fmt.Sprintf("%s AS (%s)", cte.Name, cte.Query.SQLString()))
	}
	return "WITH " + strings.Join(parts, ", ") + " "
}

func tablesForQuery(query *generator.SelectQuery, state *schema.State) []schema.Table {
	if state == nil {
		return nil
	}
	names := []string{query.From.BaseTable}
	for _, join := range query.From.Joins {
		names = append(names, join.Table)
	}
	tables := make([]schema.Table, 0, len(names))
	for _, name := range names {
		if tbl, ok := state.TableByName(name); ok {
			tables = append(tables, tbl)
		}
	}
	return tables
}

func queryHasAggregate(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	for _, item := range query.Items {
		if exprHasAggregate(item.Expr) {
			return true
		}
	}
	return false
}

func queryHasSubquery(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	for _, item := range query.Items {
		if exprHasSubquery(item.Expr) {
			return true
		}
	}
	if query.Where != nil && exprHasSubquery(query.Where) {
		return true
	}
	if query.Having != nil && exprHasSubquery(query.Having) {
		return true
	}
	for _, expr := range query.GroupBy {
		if exprHasSubquery(expr) {
			return true
		}
	}
	for _, ob := range query.OrderBy {
		if exprHasSubquery(ob.Expr) {
			return true
		}
	}
	return false
}

func queryHasWindow(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	for _, item := range query.Items {
		if exprHasWindow(item.Expr) {
			return true
		}
	}
	if query.Where != nil && exprHasWindow(query.Where) {
		return true
	}
	if query.Having != nil && exprHasWindow(query.Having) {
		return true
	}
	for _, expr := range query.GroupBy {
		if exprHasWindow(expr) {
			return true
		}
	}
	for _, ob := range query.OrderBy {
		if exprHasWindow(ob.Expr) {
			return true
		}
	}
	for _, join := range query.From.Joins {
		if join.On != nil && exprHasWindow(join.On) {
			return true
		}
	}
	return false
}

func queryDeterministic(query *generator.SelectQuery) bool {
	if query == nil {
		return true
	}
	for _, item := range query.Items {
		if !item.Expr.Deterministic() {
			return false
		}
	}
	if query.Where != nil && !query.Where.Deterministic() {
		return false
	}
	if query.Having != nil && !query.Having.Deterministic() {
		return false
	}
	for _, expr := range query.GroupBy {
		if !expr.Deterministic() {
			return false
		}
	}
	for _, ob := range query.OrderBy {
		if !ob.Expr.Deterministic() {
			return false
		}
	}
	for _, join := range query.From.Joins {
		if join.On != nil && !join.On.Deterministic() {
			return false
		}
	}
	return true
}

func exprHasAggregate(expr generator.Expr) bool {
	switch e := expr.(type) {
	case generator.FuncExpr:
		if isAggregateFunc(e.Name) {
			return true
		}
		for _, arg := range e.Args {
			if exprHasAggregate(arg) {
				return true
			}
		}
		return false
	case generator.UnaryExpr:
		return exprHasAggregate(e.Expr)
	case generator.BinaryExpr:
		return exprHasAggregate(e.Left) || exprHasAggregate(e.Right)
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if exprHasAggregate(w.When) || exprHasAggregate(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return exprHasAggregate(e.Else)
		}
		return false
	case generator.InExpr:
		if exprHasAggregate(e.Left) {
			return true
		}
		for _, item := range e.List {
			if exprHasAggregate(item) {
				return true
			}
		}
		return false
	case generator.SubqueryExpr:
		return queryHasAggregate(e.Query)
	case generator.ExistsExpr:
		return queryHasAggregate(e.Query)
	default:
		return false
	}
}

func exprHasSubquery(expr generator.Expr) bool {
	switch e := expr.(type) {
	case generator.SubqueryExpr:
		return true
	case generator.ExistsExpr:
		return true
	case generator.InExpr:
		for _, item := range e.List {
			if exprHasSubquery(item) {
				return true
			}
		}
		return exprHasSubquery(e.Left)
	case generator.UnaryExpr:
		return exprHasSubquery(e.Expr)
	case generator.BinaryExpr:
		return exprHasSubquery(e.Left) || exprHasSubquery(e.Right)
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if exprHasSubquery(w.When) || exprHasSubquery(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return exprHasSubquery(e.Else)
		}
		return false
	case generator.FuncExpr:
		for _, arg := range e.Args {
			if exprHasSubquery(arg) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func exprHasWindow(expr generator.Expr) bool {
	switch e := expr.(type) {
	case generator.WindowExpr:
		return true
	case generator.SubqueryExpr:
		return queryHasWindow(e.Query)
	case generator.ExistsExpr:
		return queryHasWindow(e.Query)
	case generator.UnaryExpr:
		return exprHasWindow(e.Expr)
	case generator.BinaryExpr:
		return exprHasWindow(e.Left) || exprHasWindow(e.Right)
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if exprHasWindow(w.When) || exprHasWindow(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return exprHasWindow(e.Else)
		}
		return false
	case generator.InExpr:
		if exprHasWindow(e.Left) {
			return true
		}
		for _, item := range e.List {
			if exprHasWindow(item) {
				return true
			}
		}
		return false
	case generator.FuncExpr:
		for _, arg := range e.Args {
			if exprHasWindow(arg) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func isAggregateFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}

func isSimplePredicate(expr generator.Expr) bool {
	switch e := expr.(type) {
	case generator.BinaryExpr:
		if strings.EqualFold(e.Op, "AND") {
			return isSimplePredicate(e.Left) && isSimplePredicate(e.Right)
		}
		if !isComparisonOp(e.Op) {
			return false
		}
		return isSimpleOperand(e.Left) && isSimpleOperand(e.Right)
	default:
		return false
	}
}

func isComparisonOp(op string) bool {
	switch strings.TrimSpace(strings.ToUpper(op)) {
	case "=", "<>", "<", "<=", ">", ">=", "<=>":
		return true
	default:
		return false
	}
}

func isSimpleOperand(expr generator.Expr) bool {
	switch v := expr.(type) {
	case generator.ColumnExpr:
		return true
	case generator.LiteralExpr:
		return v.Value != nil
	default:
		return false
	}
}
