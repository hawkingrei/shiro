package generator

import (
	"fmt"

	"shiro/internal/schema"
	"shiro/internal/util"
)

const (
	// CrossJoinProb is the percentage chance to emit CROSS JOIN when multiple tables exist.
	CrossJoinProb = 3
	// ForceJoinFromSingleProb is the percentage chance to expand a single-table pick to two tables.
	ForceJoinFromSingleProb = 80
	// CTEExtraProb is the extra chance to allow CTE when multiple tables are present.
	CTEExtraProb = 50
	// CTECountMax is the maximum number of CTEs to generate.
	CTECountMax = 2
	// CTELimitMax is the maximum LIMIT value for CTE queries.
	CTELimitMax = 10
	// SelectListMax is the maximum number of SELECT items for regular queries.
	SelectListMax = 3
	// LimitMax is the maximum LIMIT value for regular queries.
	LimitMax = 20
	// WindowPartitionProb is the chance to add PARTITION BY in window functions.
	WindowPartitionProb = 50
	// WindowOrderDescProb is the chance to use DESC for window ORDER BY.
	WindowOrderDescProb = 50
	// OrderByDescProb is the chance to use DESC in ORDER BY.
	OrderByDescProb = 50
	// OrderByCountMax is the maximum number of ORDER BY items.
	OrderByCountMax = 2
	// OrderByFromItemsExtraProb is the chance to pick two items from SELECT list.
	OrderByFromItemsExtraProb = 40
	// PredicateSubqueryScale multiplies subquery weight for predicate generation.
	PredicateSubqueryScale = 5
	// PredicateExistsProb is the chance to use EXISTS in predicate subquery.
	PredicateExistsProb = 50
	// PredicateInListProb is the chance to use IN list instead of binary comparison.
	PredicateInListProb = 20
	// PredicateInListMax is the maximum IN list size.
	PredicateInListMax = 3
	// PredicateOrProb is the chance to use OR instead of AND.
	PredicateOrProb = 30
	// CorrelatedSubqProb is the chance to use a correlated subquery.
	CorrelatedSubqProb = 90
	// JoinCountToTwoProb is the chance to increase join count from 2 to 3.
	JoinCountToTwoProb = 60
	// JoinCountToThreeProb is the chance to increase join count from 3 to 4.
	JoinCountToThreeProb = 40
	// JoinCountToFourProb is the chance to increase join count from 4 to 5.
	JoinCountToFourProb = 30
	// ShuffleTablesProb is the chance to shuffle picked tables.
	ShuffleTablesProb = 80
	// UsingJoinProb is the chance to use USING when available.
	UsingJoinProb = 20
	// UsingColumnExtraProb is the chance to use two USING columns.
	UsingColumnExtraProb = 30
)

// GenerateSelectQuery builds a randomized SELECT query for current schema.
func (g *Generator) GenerateSelectQuery() *SelectQuery {
	baseTables := g.pickTables()
	if len(baseTables) == 0 {
		return nil
	}

	if query := g.generateTemplateQuery(baseTables); query != nil {
		queryFeatures := AnalyzeQueryFeatures(query)
		g.LastFeatures = &queryFeatures
		return query
	}

	query := &SelectQuery{}
	queryTables := append([]schema.Table{}, baseTables...)

	if g.Config.Features.CTE && (util.Chance(g.Rand, g.Config.Weights.Features.CTECount*10) || (len(baseTables) > 1 && util.Chance(g.Rand, CTEExtraProb))) {
		cteCount := g.Rand.Intn(CTECountMax) + 1
		for i := 0; i < cteCount; i++ {
			cteBase := baseTables[g.Rand.Intn(len(baseTables))]
			cteQuery := g.GenerateCTEQuery(cteBase)
			cteName := fmt.Sprintf("cte_%d", i)
			query.With = append(query.With, CTE{Name: cteName, Query: cteQuery})
			queryTables = append(queryTables, schema.Table{Name: cteName, Columns: cteBase.Columns})
		}
	}

	queryTables = g.maybeShuffleTables(queryTables)
	query.From = g.buildFromClause(queryTables)
	query.Items = g.GenerateSelectList(queryTables)

	if util.Chance(g.Rand, g.Config.Weights.Features.DistinctProb) && g.Config.Features.Distinct {
		query.Distinct = true
	}

	query.Where = g.GeneratePredicate(queryTables, g.maxDepth, g.Config.Features.Subqueries, g.maxSubqDepth)

	if g.Config.Features.Aggregates && util.Chance(g.Rand, g.aggProb()) {
		withGroupBy := g.Config.Features.GroupBy && util.Chance(g.Rand, g.Config.Weights.Features.GroupByProb)
		if withGroupBy {
			query.GroupBy = g.GenerateGroupBy(queryTables)
		}
		query.Items = g.GenerateAggregateSelectList(queryTables, len(query.GroupBy) > 0)
		if g.Config.Features.Having && len(query.GroupBy) > 0 && util.Chance(g.Rand, g.Config.Weights.Features.HavingProb) {
			query.Having = g.GenerateHavingPredicate(query.GroupBy, queryTables)
		}
	}

	if g.Config.Features.OrderBy && util.Chance(g.Rand, g.Config.Weights.Features.OrderByProb) {
		if query.Distinct {
			query.OrderBy = g.GenerateOrderByFromItems(query.Items)
		} else {
			query.OrderBy = g.GenerateOrderBy(queryTables)
		}
	}
	if g.Config.Features.Limit && util.Chance(g.Rand, g.Config.Weights.Features.LimitProb) {
		limit := g.Rand.Intn(LimitMax) + 1
		query.Limit = &limit
	}

	queryFeatures := AnalyzeQueryFeatures(query)
	g.LastFeatures = &queryFeatures
	return query
}

// GenerateCTEQuery builds a small SELECT query for a CTE.
func (g *Generator) GenerateCTEQuery(tbl schema.Table) *SelectQuery {
	query := &SelectQuery{}
	query.Items = g.GenerateCTESelectList(tbl)
	query.From = FromClause{BaseTable: tbl.Name}
	query.Where = g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth-1, false, g.maxSubqDepth)
	limit := g.Rand.Intn(CTELimitMax) + 1
	query.Limit = &limit
	query.OrderBy = g.GenerateCTEOrderBy(tbl)
	return query
}

// GenerateCTESelectList builds a small SELECT list for CTEs.
func (g *Generator) GenerateCTESelectList(tbl schema.Table) []SelectItem {
	if len(tbl.Columns) == 0 {
		return nil
	}
	count := min(3, len(tbl.Columns))
	perm := g.Rand.Perm(len(tbl.Columns))[:count]
	items := make([]SelectItem, 0, count)
	for i, idx := range perm {
		col := tbl.Columns[idx]
		items = append(items, SelectItem{Expr: ColumnExpr{Ref: ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}}, Alias: fmt.Sprintf("c%d", i)})
	}
	return items
}

// GenerateCTEOrderBy enforces a stable ORDER BY for CTEs with LIMIT.
func (g *Generator) GenerateCTEOrderBy(tbl schema.Table) []OrderBy {
	if len(tbl.Columns) == 0 {
		return nil
	}
	orderCol := tbl.Columns[0]
	for _, col := range tbl.Columns {
		if col.Name == "id" {
			orderCol = col
			break
		}
	}
	return []OrderBy{{Expr: ColumnExpr{Ref: ColumnRef{Table: tbl.Name, Name: orderCol.Name, Type: orderCol.Type}}, Desc: false}}
}

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

// GenerateAggregateSelectList builds a SELECT list with aggregates.
func (g *Generator) GenerateAggregateSelectList(tables []schema.Table, withGroupBy bool) []SelectItem {
	items := make([]SelectItem, 0, 3)
	items = append(items, SelectItem{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"})
	sumArg := g.GenerateNumericExprPreferDecimalNoDouble(tables)
	g.warnAggOnDouble("SUM", sumArg)
	items = append(items, SelectItem{Expr: FuncExpr{Name: "SUM", Args: []Expr{sumArg}}, Alias: "sum1"})
	if withGroupBy {
		items = append(items, SelectItem{Expr: g.GenerateScalarExpr(tables, g.maxDepth-1, false), Alias: "g1"})
	}
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

// GeneratePredicate builds a boolean predicate expression.
func (g *Generator) GeneratePredicate(tables []schema.Table, depth int, allowSubquery bool, subqDepth int) Expr {
	if allowSubquery && subqDepth > 0 && util.Chance(g.Rand, g.subqCount()*PredicateSubqueryScale) {
		sub := g.GenerateSubquery(tables, subqDepth-1)
		if sub != nil {
			if util.Chance(g.Rand, PredicateExistsProb) {
				expr := Expr(ExistsExpr{Query: sub})
				if g.Config.Features.NotExists && util.Chance(g.Rand, g.Config.Weights.Features.NotExistsProb) {
					return UnaryExpr{Op: "NOT", Expr: expr}
				}
				return expr
			}
			left := g.generateScalarExpr(tables, 0, false, 0)
			if !g.isNumericExpr(left) {
				left = g.GenerateNumericExpr(tables)
			}
			expr := Expr(InExpr{Left: left, List: []Expr{SubqueryExpr{Query: sub}}})
			if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
				return UnaryExpr{Op: "NOT", Expr: expr}
			}
			return expr
		}
	}
	if depth <= 0 {
		left, right := g.generateComparablePair(tables, allowSubquery, subqDepth)
		return BinaryExpr{Left: left, Op: g.pickComparison(), Right: right}
	}
	if util.Chance(g.Rand, PredicateInListProb) {
		leftExpr, colType := g.pickComparableExpr(tables)
		listSize := g.Rand.Intn(PredicateInListMax) + 1
		list := make([]Expr, 0, listSize)
		for i := 0; i < listSize; i++ {
			list = append(list, g.literalForColumn(schema.Column{Type: colType}))
		}
		expr := Expr(InExpr{Left: leftExpr, List: list})
		if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
			return UnaryExpr{Op: "NOT", Expr: expr}
		}
		return expr
	}
	choice := g.Rand.Intn(3)
	if choice == 0 {
		left, right := g.generateComparablePair(tables, allowSubquery, subqDepth)
		return BinaryExpr{Left: left, Op: g.pickComparison(), Right: right}
	}
	left := g.GeneratePredicate(tables, depth-1, allowSubquery, subqDepth)
	right := g.GeneratePredicate(tables, depth-1, allowSubquery, subqDepth)
	op := "AND"
	if util.Chance(g.Rand, PredicateOrProb) {
		op = "OR"
	}
	return BinaryExpr{Left: left, Op: op, Right: right}
}

// GenerateHavingPredicate builds a HAVING predicate from group-by expressions and aggregates.
func (g *Generator) GenerateHavingPredicate(groupBy []Expr, tables []schema.Table) Expr {
	candidates := make([]Expr, 0, len(groupBy)+2)
	candidates = append(candidates, groupBy...)
	candidates = append(candidates, FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}})
	sumArg := g.GenerateNumericExprNoDouble(tables)
	g.warnAggOnDouble("SUM", sumArg)
	candidates = append(candidates, FuncExpr{Name: "SUM", Args: []Expr{sumArg}})
	expr := candidates[g.Rand.Intn(len(candidates))]
	if colType, ok := g.exprType(expr); ok {
		return BinaryExpr{Left: expr, Op: g.pickComparison(), Right: g.literalForColumn(schema.Column{Type: colType})}
	}
	return BinaryExpr{Left: expr, Op: g.pickComparison(), Right: g.randomLiteralExpr()}
}

// GenerateScalarExpr builds a scalar expression with bounded depth.
func (g *Generator) GenerateScalarExpr(tables []schema.Table, depth int, allowSubquery bool) Expr {
	return g.generateScalarExpr(tables, depth, allowSubquery, g.maxSubqDepth)
}

// GenerateSubquery builds a COUNT-based subquery, optionally correlated.
func (g *Generator) GenerateSubquery(outerTables []schema.Table, subqDepth int) *SelectQuery {
	if len(g.State.Tables) == 0 {
		return nil
	}
	inner := g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
	query := &SelectQuery{
		Items: []SelectItem{
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
		},
		From: FromClause{BaseTable: inner.Name},
	}

	if g.Config.Features.CorrelatedSubq && len(outerTables) > 0 && util.Chance(g.Rand, CorrelatedSubqProb) {
		outerCol := g.randomColumn(outerTables)
		innerCol := g.pickColumnByType(inner, outerCol.Type)
		if outerCol.Table != "" && innerCol.Name != "" {
			query.Where = BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: inner.Name, Name: innerCol.Name, Type: innerCol.Type}},
				Op:    "=",
				Right: ColumnExpr{Ref: outerCol},
			}
			return query
		}
	}

	query.Where = g.GeneratePredicate([]schema.Table{inner}, 1, false, subqDepth)
	return query
}

func (g *Generator) pickTables() []schema.Table {
	if len(g.State.Tables) == 0 {
		return nil
	}
	maxTables := len(g.State.Tables)
	count := 1
	if g.Config.Features.Joins && maxTables > 1 {
		limit := min(maxTables, g.Config.MaxJoinTables)
		count = g.Rand.Intn(min(limit, g.joinCount()+1)) + 1
		if count == 1 && util.Chance(g.Rand, ForceJoinFromSingleProb) {
			count = min(2, limit)
		}
		if count == 2 && limit >= 3 && util.Chance(g.Rand, JoinCountToTwoProb) {
			count = 3
		}
		if count == 3 && limit >= 4 && util.Chance(g.Rand, JoinCountToThreeProb) {
			count = 4
		}
		if count == 4 && limit >= 5 && util.Chance(g.Rand, JoinCountToFourProb) {
			count = 5
		}
	}
	idxs := g.Rand.Perm(maxTables)[:count]
	picked := make([]schema.Table, 0, count)
	for _, idx := range idxs {
		picked = append(picked, g.State.Tables[idx])
	}
	return picked
}

func (g *Generator) maybeShuffleTables(tables []schema.Table) []schema.Table {
	if len(tables) < 2 || !g.Config.Features.Joins {
		return tables
	}
	if !util.Chance(g.Rand, ShuffleTablesProb) {
		return tables
	}
	g.Rand.Shuffle(len(tables), func(i, j int) { tables[i], tables[j] = tables[j], tables[i] })
	return tables
}

func (g *Generator) joinCount() int {
	if g.Adaptive != nil && g.Adaptive.JoinCount > 0 {
		return g.Adaptive.JoinCount
	}
	return g.Config.Weights.Features.JoinCount
}

func (g *Generator) subqCount() int {
	if g.Adaptive != nil && g.Adaptive.SubqCount >= 0 {
		return g.Adaptive.SubqCount
	}
	return g.Config.Weights.Features.SubqCount
}

func (g *Generator) aggProb() int {
	if g.Adaptive != nil && g.Adaptive.AggProb >= 0 {
		return g.Adaptive.AggProb
	}
	return g.Config.Weights.Features.AggProb
}

func (g *Generator) buildFromClause(tables []schema.Table) FromClause {
	if len(tables) == 0 {
		return FromClause{}
	}
	from := FromClause{BaseTable: tables[0].Name}
	if len(tables) == 1 || !g.Config.Features.Joins {
		return from
	}
	for i := 1; i < len(tables); i++ {
		joinType := JoinInner
		if util.Chance(g.Rand, CrossJoinProb) {
			joinType = JoinCross
		} else {
			switch g.Rand.Intn(3) {
			case 0:
				joinType = JoinInner
			case 1:
				joinType = JoinLeft
			case 2:
				joinType = JoinRight
			}
		}
		join := Join{Type: joinType, Table: tables[i].Name}
		if joinType != JoinCross {
			using := g.pickUsingColumns(tables[:i], tables[i])
			if len(using) > 0 && util.Chance(g.Rand, UsingJoinProb) {
				join.Using = using
			} else {
				join.On = g.joinCondition(tables[:i], tables[i])
			}
		}
		from.Joins = append(from.Joins, join)
	}
	return from
}

func (g *Generator) randomColumn(tables []schema.Table) ColumnRef {
	if len(tables) == 0 {
		return ColumnRef{}
	}
	bl := tables[g.Rand.Intn(len(tables))]
	if len(bl.Columns) == 0 {
		return ColumnRef{}
	}
	col := bl.Columns[g.Rand.Intn(len(bl.Columns))]
	return ColumnRef{Table: bl.Name, Name: col.Name, Type: col.Type}
}

// areTypesCompatibleForJoin checks if two column types are compatible for use in a join.
// Compatible types include:
// - Exact matches (always compatible)
// - Integer types: INT and BIGINT
// - Numeric types: all integer types (INT, BIGINT) with floating point types (FLOAT, DOUBLE, DECIMAL)
// - Date/time types: DATE, DATETIME, TIMESTAMP
func areTypesCompatibleForJoin(left, right schema.ColumnType) bool {
	// Exact match is always compatible
	if left == right {
		return true
	}

	// Integer types are compatible with each other
	if (left == schema.TypeInt || left == schema.TypeBigInt) &&
		(right == schema.TypeInt || right == schema.TypeBigInt) {
		return true
	}

	// Integer types are compatible with floating point types
	isInteger := left == schema.TypeInt || left == schema.TypeBigInt
	isFloat := right == schema.TypeFloat || right == schema.TypeDouble || right == schema.TypeDecimal
	if isInteger && isFloat {
		return true
	}
	isInteger = right == schema.TypeInt || right == schema.TypeBigInt
	isFloat = left == schema.TypeFloat || left == schema.TypeDouble || left == schema.TypeDecimal
	if isInteger && isFloat {
		return true
	}

	// Floating point types are compatible with each other
	if (left == schema.TypeFloat || left == schema.TypeDouble || left == schema.TypeDecimal) &&
		(right == schema.TypeFloat || right == schema.TypeDouble || right == schema.TypeDecimal) {
		return true
	}

	// Date/time types are compatible with each other
	if (left == schema.TypeDate || left == schema.TypeDatetime || left == schema.TypeTimestamp) &&
		(right == schema.TypeDate || right == schema.TypeDatetime || right == schema.TypeTimestamp) {
		return true
	}

	return false
}

func (g *Generator) pickUsingColumns(left []schema.Table, right schema.Table) []string {
	leftCounts := map[string]int{}
	leftTypes := map[string]schema.ColumnType{}
	for _, ltbl := range left {
		for _, lcol := range ltbl.Columns {
			leftCounts[lcol.Name]++
			if _, ok := leftTypes[lcol.Name]; !ok {
				leftTypes[lcol.Name] = lcol.Type
			}
		}
	}
	names := []string{}
	for _, ltbl := range left {
		for _, lcol := range ltbl.Columns {
			for _, rcol := range right.Columns {
				if lcol.Name == rcol.Name && areTypesCompatibleForJoin(lcol.Type, rcol.Type) && leftCounts[lcol.Name] == 1 && areTypesCompatibleForJoin(leftTypes[lcol.Name], lcol.Type) {
					names = append(names, lcol.Name)
				}
			}
		}
	}
	if len(names) == 0 {
		return nil
	}
	count := 1
	if len(names) > 1 && util.Chance(g.Rand, UsingColumnExtraProb) {
		count = 2
	}
	g.Rand.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	return names[:count]
}

func (g *Generator) joinCondition(left []schema.Table, right schema.Table) Expr {
	if l, r, ok := g.pickJoinColumnPair(left, right); ok {
		return BinaryExpr{Left: ColumnExpr{Ref: l}, Op: "=", Right: ColumnExpr{Ref: r}}
	}
	return g.trueExpr()
}

func (g *Generator) collectColumns(tables []schema.Table) []ColumnRef {
	cols := make([]ColumnRef, 0, 8)
	for _, tbl := range tables {
		for _, col := range tbl.Columns {
			cols = append(cols, ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type})
		}
	}
	return cols
}

func (g *Generator) pickJoinColumnPair(left []schema.Table, right schema.Table) (ColumnRef, ColumnRef, bool) {
	leftCols := g.collectColumns(left)
	if len(leftCols) == 0 || len(right.Columns) == 0 {
		return ColumnRef{}, ColumnRef{}, false
	}
	sameName := make([][2]ColumnRef, 0, 4)
	for _, l := range leftCols {
		for _, rcol := range right.Columns {
			if l.Name == rcol.Name && l.Type == rcol.Type {
				r := ColumnRef{Table: right.Name, Name: rcol.Name, Type: rcol.Type}
				sameName = append(sameName, [2]ColumnRef{l, r})
			}
		}
	}
	if len(sameName) > 0 {
		pair := sameName[g.Rand.Intn(len(sameName))]
		return pair[0], pair[1], true
	}
	rightByType := map[schema.ColumnType][]ColumnRef{}
	for _, rcol := range right.Columns {
		rightByType[rcol.Type] = append(rightByType[rcol.Type], ColumnRef{Table: right.Name, Name: rcol.Name, Type: rcol.Type})
	}
	sameType := make([][2]ColumnRef, 0, len(leftCols))
	for _, l := range leftCols {
		if rs, ok := rightByType[l.Type]; ok && len(rs) > 0 {
			r := rs[g.Rand.Intn(len(rs))]
			sameType = append(sameType, [2]ColumnRef{l, r})
		}
	}
	if len(sameType) == 0 {
		return ColumnRef{}, ColumnRef{}, false
	}
	pair := sameType[g.Rand.Intn(len(sameType))]
	return pair[0], pair[1], true
}

func (g *Generator) trueExpr() Expr {
	return BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}}
}
