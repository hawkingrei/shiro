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
		count := 1
		if util.Chance(g.Rand, WindowPartitionProb/2) {
			count = 2
		}
		cols := g.pickUniqueColumns(tables, count)
		for _, col := range cols {
			partitionBy = append(partitionBy, ColumnExpr{Ref: col})
		}
	}
	orderBy := make([]OrderBy, 0, 2)
	orderCount := 1
	if util.Chance(g.Rand, WindowOrderDescProb/2) {
		orderCount = 2
	}
	for _, col := range g.pickUniqueColumns(tables, orderCount) {
		orderBy = append(orderBy, OrderBy{Expr: ColumnExpr{Ref: col}, Desc: util.Chance(g.Rand, WindowOrderDescProb)})
	}
	if len(orderBy) == 0 {
		orderBy = []OrderBy{{Expr: LiteralExpr{Value: 1}, Desc: false}}
	}
	return WindowExpr{
		Name:        name,
		Args:        args,
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
	}
}

func (g *Generator) pickUniqueColumns(tables []schema.Table, count int) []ColumnRef {
	cols := g.uniqueColumns(tables)
	if len(cols) == 0 || count <= 0 {
		return nil
	}
	g.Rand.Shuffle(len(cols), func(i, j int) { cols[i], cols[j] = cols[j], cols[i] })
	if count > len(cols) {
		count = len(cols)
	}
	return cols[:count]
}

func (g *Generator) uniqueColumns(tables []schema.Table) []ColumnRef {
	cols := g.collectColumns(tables)
	if len(cols) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(cols))
	out := make([]ColumnRef, 0, len(cols))
	for _, col := range cols {
		if col.Table == "" || col.Name == "" {
			continue
		}
		key := col.Table + "." + col.Name
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, col)
	}
	return out
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
	cols := g.uniqueColumns(tables)
	if len(cols) == 0 {
		return nil
	}
	if len(cols) == 1 {
		return []Expr{ColumnExpr{Ref: cols[0]}}
	}
	idx1 := g.Rand.Intn(len(cols))
	idx2 := g.Rand.Intn(len(cols) - 1)
	if idx2 >= idx1 {
		idx2++
	}
	return []Expr{
		ColumnExpr{Ref: cols[idx1]},
		ColumnExpr{Ref: cols[idx2]},
	}
}

// wrapGroupByOrdinals converts GROUP BY expressions into ordinal positions.
func (g *Generator) wrapGroupByOrdinals(groupBy []Expr) []Expr {
	if len(groupBy) == 0 {
		return groupBy
	}
	out := make([]Expr, 0, len(groupBy))
	for idx, expr := range groupBy {
		out = append(out, GroupByOrdinalExpr{Ordinal: idx + 1, Expr: expr})
	}
	return out
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

func (g *Generator) orderByFromItemsStable(items []SelectItem) []OrderBy {
	if len(items) == 0 {
		return nil
	}
	orderBy := g.GenerateOrderByFromItems(items)
	if orderByDistinctColumns(orderBy) >= 2 {
		return orderBy
	}
	if len(items) >= 2 {
		return []OrderBy{
			{Expr: LiteralExpr{Value: 1}, Desc: util.Chance(g.Rand, OrderByDescProb)},
			{Expr: LiteralExpr{Value: 2}, Desc: util.Chance(g.Rand, OrderByDescProb)},
		}
	}
	return orderBy
}

func orderByDistinctColumns(orderBy []OrderBy) int {
	seen := make(map[string]struct{})
	for _, ob := range orderBy {
		for _, col := range ob.Expr.Columns() {
			if col.Table == "" || col.Name == "" {
				continue
			}
			seen[col.Table+"."+col.Name] = struct{}{}
		}
	}
	return len(seen)
}

func (g *Generator) ensureOrderByDistinctColumns(orderBy []OrderBy, tables []schema.Table) []OrderBy {
	if len(orderBy) == 0 {
		return orderBy
	}
	if orderByDistinctColumns(orderBy) >= 2 {
		return orderBy
	}
	cols := g.uniqueColumns(tables)
	if len(cols) < 2 {
		return orderBy
	}
	idx1 := g.Rand.Intn(len(cols))
	idx2 := g.Rand.Intn(len(cols) - 1)
	if idx2 >= idx1 {
		idx2++
	}
	return []OrderBy{
		{Expr: ColumnExpr{Ref: cols[idx1]}, Desc: util.Chance(g.Rand, OrderByDescProb)},
		{Expr: ColumnExpr{Ref: cols[idx2]}, Desc: util.Chance(g.Rand, OrderByDescProb)},
	}
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
	cols := g.uniqueColumns(tables)
	if len(cols) == 0 {
		return nil
	}
	if len(cols) == 1 {
		// Single-column tables cannot satisfy the "two distinct keys" policy.
		// Return the only column as the deterministic ordering fallback.
		return []OrderBy{{
			Expr: ColumnExpr{Ref: cols[0]},
			Desc: false,
		}}
	}
	base := tables[0]
	var primary ColumnRef
	found := false
	for _, c := range base.Columns {
		if c.Name == "id" {
			primary = ColumnRef{Table: base.Name, Name: c.Name, Type: c.Type}
			found = true
			break
		}
	}
	if !found {
		primary = cols[0]
	}
	secondary := cols[0]
	for _, col := range cols {
		if col.Table != primary.Table || col.Name != primary.Name {
			secondary = col
			break
		}
	}
	return []OrderBy{
		{Expr: ColumnExpr{Ref: primary}, Desc: false},
		{Expr: ColumnExpr{Ref: secondary}, Desc: false},
	}
}
