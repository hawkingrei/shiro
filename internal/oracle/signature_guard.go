package oracle

import (
	"shiro/internal/generator"
	"shiro/internal/schema"
)

func signaturePrecheck(query *generator.SelectQuery, state *schema.State, prefix string) (skipReason string, reason string) {
	if query == nil {
		return "", ""
	}
	if hasInvalidOrderByOrdinal(query.OrderBy, len(query.Items)) {
		return prefix + ":order_by_invalid_ordinal", "order_by_invalid_ordinal"
	}
	if state == nil {
		return "", ""
	}
	if reason, ok := queryUnknownQualifiedColumnReason(query, state); ok {
		return prefix + ":column_visibility_" + reason, "column_visibility_" + reason
	}
	return "", ""
}

func hasInvalidOrderByOrdinal(orderBy []generator.OrderBy, itemCount int) bool {
	if itemCount <= 0 {
		return false
	}
	for _, ob := range orderBy {
		ordinal, ok := orderByLiteralInt(ob.Expr)
		if !ok {
			continue
		}
		if ordinal < 1 || ordinal > itemCount {
			return true
		}
	}
	return false
}

func orderByLiteralInt(expr generator.Expr) (int, bool) {
	lit, ok := expr.(generator.LiteralExpr)
	if !ok {
		return 0, false
	}
	return literalIntValue(lit.Value)
}

func literalIntValue(value any) (int, bool) {
	maxInt := int(^uint(0) >> 1)
	switch v := value.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		if v > int64(maxInt) || v < -int64(maxInt)-1 {
			return 0, false
		}
		return int(v), true
	case uint:
		if v > uint(maxInt) {
			return 0, false
		}
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		if v > uint32(maxInt) {
			return 0, false
		}
		return int(v), true
	case uint64:
		if v > uint64(maxInt) {
			return 0, false
		}
		return int(v), true
	default:
		return 0, false
	}
}

func queryUnknownQualifiedColumnReason(query *generator.SelectQuery, state *schema.State) (string, bool) {
	if query == nil || state == nil {
		return "", false
	}
	for _, cte := range query.With {
		if reason, ok := queryUnknownQualifiedColumnReason(cte.Query, state); ok {
			return reason, true
		}
	}
	for _, op := range query.SetOps {
		if reason, ok := queryUnknownQualifiedColumnReason(op.Query, state); ok {
			return reason, true
		}
	}
	if reason, ok := queryUnknownQualifiedColumnReason(query.From.BaseQuery, state); ok {
		return reason, true
	}

	baseName := query.From.BaseTable
	if query.From.BaseAlias != "" {
		baseName = query.From.BaseAlias
	}
	leftTables := []string{baseName}
	for _, join := range query.From.Joins {
		if reason, ok := queryUnknownQualifiedColumnReason(join.TableQuery, state); ok {
			return reason, true
		}
		if reason, ok := exprUnknownQualifiedColumnReason(join.On, state); ok {
			return reason, true
		}
		if len(join.Using) > 0 {
			if reason, ok := usingColumnsValid(leftTables, join, state); ok {
				return reason, true
			}
		}
		name := join.Table
		if join.TableAlias != "" {
			name = join.TableAlias
		}
		if name != "" {
			leftTables = append(leftTables, name)
		}
	}

	for _, item := range query.Items {
		if reason, ok := exprUnknownQualifiedColumnReason(item.Expr, state); ok {
			return reason, true
		}
	}
	if reason, ok := exprUnknownQualifiedColumnReason(query.Where, state); ok {
		return reason, true
	}
	if reason, ok := exprUnknownQualifiedColumnReason(query.Having, state); ok {
		return reason, true
	}
	for _, expr := range query.GroupBy {
		if reason, ok := exprUnknownQualifiedColumnReason(expr, state); ok {
			return reason, true
		}
	}
	for _, ob := range query.OrderBy {
		if reason, ok := exprUnknownQualifiedColumnReason(ob.Expr, state); ok {
			return reason, true
		}
	}
	return "", false
}

func usingColumnsValid(leftTables []string, join generator.Join, state *schema.State) (string, bool) {
	if state == nil || len(join.Using) == 0 {
		return "", false
	}
	rightName := join.Table
	if join.TableAlias != "" {
		rightName = join.TableAlias
	}
	rightTable, rightKnown := state.TableByName(rightName)
	if !rightKnown {
		return "", false
	}
	for _, col := range join.Using {
		if _, ok := rightTable.ColumnByName(col); !ok {
			return "unknown_using_column", true
		}
		leftKnown := false
		leftFound := false
		for _, leftName := range leftTables {
			leftTable, ok := state.TableByName(leftName)
			if !ok {
				continue
			}
			leftKnown = true
			if _, ok := leftTable.ColumnByName(col); ok {
				leftFound = true
				break
			}
		}
		if leftKnown && !leftFound {
			return "unknown_using_column", true
		}
	}
	return "", false
}

func exprUnknownQualifiedColumnReason(expr generator.Expr, state *schema.State) (string, bool) {
	if expr == nil || state == nil {
		return "", false
	}
	switch e := expr.(type) {
	case generator.UnaryExpr:
		return exprUnknownQualifiedColumnReason(e.Expr, state)
	case generator.BinaryExpr:
		if reason, ok := exprUnknownQualifiedColumnReason(e.Left, state); ok {
			return reason, true
		}
		return exprUnknownQualifiedColumnReason(e.Right, state)
	case generator.FuncExpr:
		for _, arg := range e.Args {
			if reason, ok := exprUnknownQualifiedColumnReason(arg, state); ok {
				return reason, true
			}
		}
		return "", false
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if reason, ok := exprUnknownQualifiedColumnReason(w.When, state); ok {
				return reason, true
			}
			if reason, ok := exprUnknownQualifiedColumnReason(w.Then, state); ok {
				return reason, true
			}
		}
		return exprUnknownQualifiedColumnReason(e.Else, state)
	case generator.InExpr:
		if reason, ok := exprUnknownQualifiedColumnReason(e.Left, state); ok {
			return reason, true
		}
		for _, item := range e.List {
			if reason, ok := exprUnknownQualifiedColumnReason(item, state); ok {
				return reason, true
			}
		}
		return "", false
	case generator.SubqueryExpr:
		return queryUnknownQualifiedColumnReason(e.Query, state)
	case generator.ExistsExpr:
		return queryUnknownQualifiedColumnReason(e.Query, state)
	case generator.CompareSubqueryExpr:
		if reason, ok := exprUnknownQualifiedColumnReason(e.Left, state); ok {
			return reason, true
		}
		return queryUnknownQualifiedColumnReason(e.Query, state)
	case generator.WindowExpr:
		for _, arg := range e.Args {
			if reason, ok := exprUnknownQualifiedColumnReason(arg, state); ok {
				return reason, true
			}
		}
		for _, part := range e.PartitionBy {
			if reason, ok := exprUnknownQualifiedColumnReason(part, state); ok {
				return reason, true
			}
		}
		for _, ob := range e.OrderBy {
			if reason, ok := exprUnknownQualifiedColumnReason(ob.Expr, state); ok {
				return reason, true
			}
		}
	}
	for _, col := range expr.Columns() {
		if col.Table == "" || col.Name == "" {
			continue
		}
		tbl, ok := state.TableByName(col.Table)
		if !ok {
			continue
		}
		if _, ok := tbl.ColumnByName(col.Name); !ok {
			return "unknown_column", true
		}
	}
	return "", false
}
