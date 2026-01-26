package generator

import (
	"fmt"

	"shiro/internal/schema"
	"shiro/internal/util"
)

// GenerateSelectList builds a SELECT list for the given tables.
func (g *Generator) GenerateSelectList(tables []schema.Table) []SelectItem {
	count := g.Rand.Intn(SelectListMax) + 1
	items := make([]SelectItem, 0, count)
	for i := 0; i < count; i++ {
		expr := g.GenerateSelectExpr(tables, g.maxDepth)
		items = append(items, SelectItem{Expr: expr, Alias: fmt.Sprintf("c%d", i)})
	}
	return items
}

// GenerateSelectExpr builds a scalar or window expression for SELECT.
func (g *Generator) GenerateSelectExpr(tables []schema.Table, depth int) Expr {
	if g.Config.Features.WindowFuncs && util.Chance(g.Rand, g.Config.Weights.Features.WindowProb) {
		return g.GenerateWindowExpr(tables)
	}
	return g.GenerateScalarExpr(tables, depth, g.Config.Features.Subqueries)
}

// GenerateWindowExpr builds a window function expression.
func (g *Generator) GenerateWindowExpr(tables []schema.Table) Expr {
	funcs := []string{"ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "AVG"}
	name := funcs[g.Rand.Intn(len(funcs))]
	var args []Expr
	if name == "SUM" || name == "AVG" {
		args = []Expr{g.GenerateNumericExprPreferDecimalNoDouble(tables)}
		g.warnAggOnDouble(name, args[0])
	}
	partitionBy := []Expr{}
	if util.Chance(g.Rand, WindowPartitionProb) {
		col := g.randomColumn(tables)
		if col.Table != "" {
			partitionBy = append(partitionBy, ColumnExpr{Ref: col})
		}
	}
	orderCol := g.randomColumn(tables)
	orderExpr := Expr(LiteralExpr{Value: 1})
	if orderCol.Table != "" {
		orderExpr = ColumnExpr{Ref: orderCol}
	}
	orderBy := []OrderBy{{Expr: orderExpr, Desc: util.Chance(g.Rand, WindowOrderDescProb)}}
	return WindowExpr{
		Name:        name,
		Args:        args,
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
	}
}

// GenerateAggregateSelectList builds a SELECT list with aggregates and group keys.
func (g *Generator) GenerateAggregateSelectList(tables []schema.Table, groupBy []Expr) []SelectItem {
	items := make([]SelectItem, 0, 2+len(groupBy))
	for i, expr := range groupBy {
		items = append(items, SelectItem{Expr: expr, Alias: fmt.Sprintf("g%d", i)})
	}
	items = append(items, SelectItem{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"})
	sumArg := g.GenerateNumericExprPreferDecimalNoDouble(tables)
	g.warnAggOnDouble("SUM", sumArg)
	items = append(items, SelectItem{Expr: FuncExpr{Name: "SUM", Args: []Expr{sumArg}}, Alias: "sum1"})
	return items
}

// GenerateGroupBy selects a single grouping expression.
func (g *Generator) GenerateGroupBy(tables []schema.Table) []Expr {
	col := g.randomColumn(tables)
	if col.Table == "" {
		return nil
	}
	return []Expr{ColumnExpr{Ref: col}}
}

// GenerateOrderBy builds an ORDER BY list.
func (g *Generator) GenerateOrderBy(tables []schema.Table) []OrderBy {
	count := g.Rand.Intn(OrderByCountMax) + 1
	items := make([]OrderBy, 0, count)
	for i := 0; i < count; i++ {
		expr := g.GenerateScalarExpr(tables, g.maxDepth, false)
		if _, ok := expr.(LiteralExpr); ok {
			col := g.randomColumn(tables)
			if col.Table != "" {
				expr = ColumnExpr{Ref: col}
			}
		}
		items = append(items, OrderBy{Expr: expr, Desc: util.Chance(g.Rand, OrderByDescProb)})
	}
	return items
}

// GenerateOrderByFromItems uses SELECT-list expressions for ORDER BY.
func (g *Generator) GenerateOrderByFromItems(items []SelectItem) []OrderBy {
	if len(items) == 0 {
		return nil
	}
	count := 1
	if len(items) > 1 && util.Chance(g.Rand, OrderByFromItemsExtraProb) {
		count = 2
	}
	idxs := g.Rand.Perm(len(items))[:count]
	orders := make([]OrderBy, 0, count)
	for _, idx := range idxs {
		orders = append(orders, OrderBy{Expr: items[idx].Expr, Desc: util.Chance(g.Rand, OrderByDescProb)})
	}
	return orders
}

func (g *Generator) warnAggOnDouble(name string, arg Expr) {
	col, ok := arg.(ColumnExpr)
	if !ok {
		return
	}
	if col.Ref.Type != schema.TypeDouble {
		return
	}
	util.Warnf("aggregate %s on DOUBLE column %s.%s", name, col.Ref.Table, col.Ref.Name)
}

func (g *Generator) deterministicOrderBy(tables []schema.Table) []OrderBy {
	if len(tables) == 0 {
		return nil
	}
	base := tables[0]
	var col schema.Column
	found := false
	for _, c := range base.Columns {
		if c.Name == "id" {
			col = c
			found = true
			break
		}
	}
	if !found {
		if len(base.Columns) == 0 {
			return nil
		}
		col = base.Columns[0]
	}
	return []OrderBy{{
		Expr: ColumnExpr{Ref: ColumnRef{Table: base.Name, Name: col.Name, Type: col.Type}},
		Desc: false,
	}}
}
