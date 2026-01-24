package generator

import (
	"fmt"
	"math/rand"

	"shiro/internal/schema"
	"shiro/internal/util"
)

// (constants moved to constants.go)

// GenerateSelectQuery builds a randomized SELECT query for current schema.
func (g *Generator) GenerateSelectQuery() *SelectQuery {
	baseTables := g.pickTables()
	if len(baseTables) == 0 {
		return nil
	}
	g.resetPredicateStats()

	if query := g.generateTemplateQuery(baseTables); query != nil {
		queryFeatures := AnalyzeQueryFeatures(query)
		queryFeatures.PredicatePairsTotal = g.predicatePairsTotal
		queryFeatures.PredicatePairsJoin = g.predicatePairsJoin
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

	// Only emit LIMIT when paired with ORDER BY to keep top-N semantics.
	if g.Config.Features.OrderBy && util.Chance(g.Rand, g.Config.Weights.Features.OrderByProb) {
		if query.Distinct {
			query.OrderBy = g.GenerateOrderByFromItems(query.Items)
		} else {
			query.OrderBy = g.GenerateOrderBy(queryTables)
		}
		if g.Config.Features.Limit && util.Chance(g.Rand, g.Config.Weights.Features.LimitProb) {
			limit := g.Rand.Intn(LimitMax) + 1
			query.Limit = &limit
		}
	}

	queryFeatures := AnalyzeQueryFeatures(query)
	queryFeatures.PredicatePairsTotal = g.predicatePairsTotal
	queryFeatures.PredicatePairsJoin = g.predicatePairsJoin
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
				existsSub := g.GenerateExistsSubquery(tables, subqDepth-1)
				if existsSub != nil {
					sub = existsSub
				}
				expr := Expr(ExistsExpr{Query: sub})
				if g.Config.Features.NotExists && util.Chance(g.Rand, g.Config.Weights.Features.NotExistsProb) {
					return UnaryExpr{Op: "NOT", Expr: expr}
				}
				return expr
			}
			leftExpr, leftType, _ := g.pickComparableExprPreferJoinGraph(tables)
			typedSub := g.GenerateInSubquery(tables, leftType, subqDepth-1)
			if typedSub == nil {
				left, ok := g.pickNumericExprPreferJoinGraph(tables)
				if !ok {
					left = g.generateScalarExpr(tables, 0, false, 0)
					if !g.isNumericExpr(left) {
						left = g.GenerateNumericExpr(tables)
					}
				}
				expr := Expr(InExpr{Left: left, List: []Expr{SubqueryExpr{Query: sub}}})
				if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
					return UnaryExpr{Op: "NOT", Expr: expr}
				}
				return expr
			}
			expr := Expr(InExpr{Left: leftExpr, List: []Expr{SubqueryExpr{Query: typedSub}}})
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
		leftExpr, colType, _ := g.pickComparableExprPreferJoinGraph(tables)
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
	if len(outerTables) > 0 {
		if picked, ok := g.pickJoinableInnerTable(outerTables); ok {
			inner = picked
		}
	}
	query := &SelectQuery{
		Items: []SelectItem{
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
		},
		From: FromClause{BaseTable: inner.Name},
	}

	if g.Config.Features.CorrelatedSubq && len(outerTables) > 0 && util.Chance(g.Rand, CorrelatedSubqProb) {
		if outerCol, innerCol, ok := g.pickCorrelatedJoinPair(outerTables, inner); ok {
			query.Where = BinaryExpr{
				Left:  ColumnExpr{Ref: innerCol},
				Op:    "=",
				Right: ColumnExpr{Ref: outerCol},
			}
			if util.Chance(g.Rand, CorrelatedSubqExtraProb) {
				if extra, ok := g.pickCorrelatedPredicate(outerTables, inner); ok {
					query.Where = BinaryExpr{Left: query.Where, Op: "AND", Right: extra}
				}
			}
			return query
		}
		outerCol := g.randomColumn(outerTables)
		innerCol := g.pickColumnByType(inner, outerCol.Type)
		if outerCol.Table != "" && innerCol.Name != "" {
			query.Where = BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: inner.Name, Name: innerCol.Name, Type: innerCol.Type}},
				Op:    "=",
				Right: ColumnExpr{Ref: outerCol},
			}
			if util.Chance(g.Rand, CorrelatedSubqExtraProb) {
				if extra, ok := g.pickCorrelatedPredicate(outerTables, inner); ok {
					query.Where = BinaryExpr{Left: query.Where, Op: "AND", Right: extra}
				}
			}
			return query
		}
	}

	query.Where = g.GeneratePredicate([]schema.Table{inner}, 1, false, subqDepth)
	return query
}

func (g *Generator) GenerateInSubquery(outerTables []schema.Table, leftType schema.ColumnType, subqDepth int) *SelectQuery {
	if len(g.State.Tables) == 0 {
		return nil
	}
	inner := g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
	if picked, ok := g.pickInnerTableForType(outerTables, leftType); ok {
		inner = picked
	}
	innerCol := g.pickColumnByType(inner, leftType)
	if innerCol.Name == "" {
		innerCol, _ = g.pickCompatibleColumn(inner, leftType)
	}
	if innerCol.Name == "" {
		return nil
	}
	query := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: inner.Name, Name: innerCol.Name, Type: innerCol.Type}}, Alias: "c0"},
		},
		From: FromClause{BaseTable: inner.Name},
	}

	if g.Config.Features.CorrelatedSubq && len(outerTables) > 0 && util.Chance(g.Rand, CorrelatedSubqProb) {
		if outerCol, joinInnerCol, ok := g.pickCorrelatedJoinPair(outerTables, inner); ok {
			query.Where = BinaryExpr{
				Left:  ColumnExpr{Ref: joinInnerCol},
				Op:    "=",
				Right: ColumnExpr{Ref: outerCol},
			}
			if util.Chance(g.Rand, CorrelatedSubqExtraProb) {
				if extra, ok := g.pickCorrelatedPredicate(outerTables, inner); ok {
					query.Where = BinaryExpr{Left: query.Where, Op: "AND", Right: extra}
				}
			}
			return query
		}
		outerCol := g.randomColumn(outerTables)
		corrInner := g.pickColumnByType(inner, outerCol.Type)
		if outerCol.Table != "" && corrInner.Name != "" {
			query.Where = BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: inner.Name, Name: corrInner.Name, Type: corrInner.Type}},
				Op:    "=",
				Right: ColumnExpr{Ref: outerCol},
			}
			if util.Chance(g.Rand, CorrelatedSubqExtraProb) {
				if extra, ok := g.pickCorrelatedPredicate(outerTables, inner); ok {
					query.Where = BinaryExpr{Left: query.Where, Op: "AND", Right: extra}
				}
			}
			return query
		}
	}

	query.Where = g.GeneratePredicate([]schema.Table{inner}, 1, false, subqDepth)
	return query
}

func (g *Generator) GenerateExistsSubquery(outerTables []schema.Table, subqDepth int) *SelectQuery {
	if len(g.State.Tables) == 0 {
		return nil
	}
	inner := g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
	if len(outerTables) > 0 {
		if picked, ok := g.pickJoinableInnerTable(outerTables); ok {
			inner = picked
		}
	}
	item := SelectItem{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"}
	if len(outerTables) > 0 {
		outerCol := g.randomColumn(outerTables)
		if outerCol.Table != "" {
			if innerCol, ok := g.pickCompatibleColumn(inner, outerCol.Type); ok {
				item = SelectItem{
					Expr:  ColumnExpr{Ref: ColumnRef{Table: inner.Name, Name: innerCol.Name, Type: innerCol.Type}},
					Alias: "c0",
				}
			}
		}
	}
	query := &SelectQuery{
		Items: []SelectItem{item},
		From:  FromClause{BaseTable: inner.Name},
	}
	if g.Config.Features.CorrelatedSubq && len(outerTables) > 0 && util.Chance(g.Rand, CorrelatedSubqProb) {
		if outerCol, innerCol, ok := g.pickCorrelatedJoinPair(outerTables, inner); ok {
			query.Where = BinaryExpr{
				Left:  ColumnExpr{Ref: innerCol},
				Op:    "=",
				Right: ColumnExpr{Ref: outerCol},
			}
			if util.Chance(g.Rand, CorrelatedSubqExtraProb) {
				if extra, ok := g.pickCorrelatedPredicate(outerTables, inner); ok {
					query.Where = BinaryExpr{Left: query.Where, Op: "AND", Right: extra}
				}
			}
			return query
		}
	}
	query.Where = g.GeneratePredicate([]schema.Table{inner}, 1, false, subqDepth)
	return query
}

func (g *Generator) pickJoinableInnerTable(outerTables []schema.Table) (schema.Table, bool) {
	if len(outerTables) == 0 {
		return schema.Table{}, false
	}
	candidates := make([]schema.Table, 0, len(g.State.Tables))
	for _, tbl := range g.State.Tables {
		for _, outer := range outerTables {
			if tablesJoinable(outer, tbl) {
				candidates = append(candidates, tbl)
				break
			}
		}
	}
	if len(candidates) == 0 {
		return schema.Table{}, false
	}
	return candidates[g.Rand.Intn(len(candidates))], true
}

func (g *Generator) pickInnerTableForType(outerTables []schema.Table, colType schema.ColumnType) (schema.Table, bool) {
	candidates := make([]schema.Table, 0, len(g.State.Tables))
	joinable := make([]schema.Table, 0, len(g.State.Tables))
	for _, tbl := range g.State.Tables {
		if _, ok := g.pickCompatibleColumn(tbl, colType); !ok {
			continue
		}
		candidates = append(candidates, tbl)
		for _, outer := range outerTables {
			if tablesJoinable(outer, tbl) {
				joinable = append(joinable, tbl)
				break
			}
		}
	}
	if len(joinable) > 0 {
		return joinable[g.Rand.Intn(len(joinable))], true
	}
	if len(candidates) > 0 {
		return candidates[g.Rand.Intn(len(candidates))], true
	}
	return schema.Table{}, false
}

func (g *Generator) pickCompatibleColumn(tbl schema.Table, colType schema.ColumnType) (schema.Column, bool) {
	if colType == 0 {
		return schema.Column{}, false
	}
	candidates := make([]schema.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		if compatibleColumnType(col.Type, colType) {
			candidates = append(candidates, col)
		}
	}
	if len(candidates) == 0 {
		return schema.Column{}, false
	}
	return candidates[g.Rand.Intn(len(candidates))], true
}

func (g *Generator) pickCorrelatedPredicate(outerTables []schema.Table, inner schema.Table) (Expr, bool) {
	outerCol, innerCol, ok := g.pickCorrelatedJoinPair(outerTables, inner)
	if !ok {
		return nil, false
	}
	return BinaryExpr{
		Left:  ColumnExpr{Ref: innerCol},
		Op:    g.pickComparison(),
		Right: ColumnExpr{Ref: outerCol},
	}, true
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
		if count > 1 && util.Chance(g.Rand, JoinCountBiasProb) {
			biasMin := min(JoinCountBiasMin, limit)
			biasMax := min(JoinCountBiasMax, limit)
			if biasMin <= biasMax && limit >= biasMin {
				count = g.Rand.Intn(biasMax-biasMin+1) + biasMin
			}
		}
	}
	if count > 1 && g.Config.Features.Joins {
		if picked := g.pickJoinTables(count); len(picked) == count {
			return picked
		}
	}
	idxs := g.Rand.Perm(maxTables)[:count]
	picked := make([]schema.Table, 0, count)
	for _, idx := range idxs {
		picked = append(picked, g.State.Tables[idx])
	}
	return picked
}

func (g *Generator) pickJoinTables(count int) []schema.Table {
	if count <= 1 {
		return nil
	}
	tables := g.State.Tables
	if len(tables) < count {
		return nil
	}
	adj := buildJoinAdjacency(tables)
	if !hasJoinEdges(adj) {
		return nil
	}
	switch pickJoinShape(g.Rand) {
	case joinShapeStar:
		if idxs := pickStarJoinOrder(g.Rand, adj, count); len(idxs) == count {
			return mapJoinTables(tables, idxs)
		}
	case joinShapeSnowflake:
		if idxs := pickSnowflakeJoinOrder(g.Rand, adj, count); len(idxs) == count {
			return mapJoinTables(tables, idxs)
		}
	default:
		if idxs := pickChainJoinOrder(g.Rand, adj, count); len(idxs) == count {
			return mapJoinTables(tables, idxs)
		}
	}
	if idxs := pickChainJoinOrder(g.Rand, adj, count); len(idxs) == count {
		return mapJoinTables(tables, idxs)
	}
	return nil
}

type joinShape int

const (
	joinShapeChain joinShape = iota
	joinShapeStar
	joinShapeSnowflake
)

type columnPair struct {
	Left  ColumnRef
	Right ColumnRef
}

func pickJoinShape(r *rand.Rand) joinShape {
	roll := r.Intn(100)
	if roll < JoinShapeChainProb {
		return joinShapeChain
	}
	roll -= JoinShapeChainProb
	if roll < JoinShapeStarProb {
		return joinShapeStar
	}
	return joinShapeSnowflake
}

func buildJoinAdjacency(tables []schema.Table) [][]int {
	n := len(tables)
	adj := make([][]int, n)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if tablesJoinable(tables[i], tables[j]) {
				adj[i] = append(adj[i], j)
				adj[j] = append(adj[j], i)
			}
		}
	}
	return adj
}

func (g *Generator) collectJoinColumns(tbl schema.Table, useIndexPrefix bool) []ColumnRef {
	if useIndexPrefix {
		return g.collectIndexPrefixColumns([]schema.Table{tbl})
	}
	return g.collectColumns([]schema.Table{tbl})
}

func (g *Generator) collectJoinPairs(left schema.Table, right schema.Table, requireSameName bool, useIndexPrefix bool) []columnPair {
	leftCols := g.collectJoinColumns(left, useIndexPrefix)
	rightCols := g.collectJoinColumns(right, useIndexPrefix)
	if len(leftCols) == 0 || len(rightCols) == 0 {
		return nil
	}
	pairs := make([]columnPair, 0, 8)
	for _, l := range leftCols {
		for _, r := range rightCols {
			if requireSameName && l.Name != r.Name {
				continue
			}
			if !compatibleColumnType(l.Type, r.Type) {
				continue
			}
			pairs = append(pairs, columnPair{Left: l, Right: r})
		}
	}
	return pairs
}

func tablesJoinable(left schema.Table, right schema.Table) bool {
	for _, lcol := range left.Columns {
		for _, rcol := range right.Columns {
			if compatibleColumnType(lcol.Type, rcol.Type) {
				return true
			}
		}
	}
	return false
}

func hasJoinEdges(adj [][]int) bool {
	for _, edges := range adj {
		if len(edges) > 0 {
			return true
		}
	}
	return false
}

func pickChainJoinOrder(r *rand.Rand, adj [][]int, count int) []int {
	if count <= 0 || len(adj) == 0 {
		return nil
	}
	start := pickStartNode(r, adj)
	selected := []int{start}
	remaining := make(map[int]struct{}, len(adj)-1)
	for i := 0; i < len(adj); i++ {
		if i != start {
			remaining[i] = struct{}{}
		}
	}
	for len(selected) < count {
		last := selected[len(selected)-1]
		next := pickNeighborFromAnchors(r, adj, []int{last}, remaining)
		if next == -1 {
			next = pickNeighborFromAnchors(r, adj, selected, remaining)
		}
		if next == -1 {
			return nil
		}
		selected = append(selected, next)
		delete(remaining, next)
	}
	return selected
}

func pickStarJoinOrder(r *rand.Rand, adj [][]int, count int) []int {
	if count <= 0 || len(adj) == 0 {
		return nil
	}
	center := pickStartNode(r, adj)
	if len(adj[center]) == 0 {
		return nil
	}
	selected := []int{center}
	neighbors := append([]int(nil), adj[center]...)
	r.Shuffle(len(neighbors), func(i, j int) { neighbors[i], neighbors[j] = neighbors[j], neighbors[i] })
	for _, nb := range neighbors {
		if len(selected) >= count {
			break
		}
		selected = append(selected, nb)
	}
	if len(selected) != count {
		return nil
	}
	return selected
}

func pickSnowflakeJoinOrder(r *rand.Rand, adj [][]int, count int) []int {
	if count <= 0 || len(adj) == 0 {
		return nil
	}
	center := pickStartNode(r, adj)
	if len(adj[center]) == 0 {
		return nil
	}
	selected := []int{center}
	remaining := make(map[int]struct{}, len(adj)-1)
	for i := 0; i < len(adj); i++ {
		if i != center {
			remaining[i] = struct{}{}
		}
	}
	firstLevel := pickNeighbors(r, adj[center], remaining, min(2, count-1))
	for _, nb := range firstLevel {
		selected = append(selected, nb)
		delete(remaining, nb)
	}
	for len(selected) < count {
		next := pickNeighborFromAnchors(r, adj, firstLevel, remaining)
		if next == -1 {
			next = pickNeighborFromAnchors(r, adj, selected, remaining)
		}
		if next == -1 {
			return nil
		}
		selected = append(selected, next)
		delete(remaining, next)
	}
	return selected
}

func pickStartNode(r *rand.Rand, adj [][]int) int {
	best := 0
	bestDeg := -1
	for i, edges := range adj {
		if len(edges) > bestDeg {
			bestDeg = len(edges)
			best = i
		}
	}
	if bestDeg <= 0 {
		return r.Intn(len(adj))
	}
	if util.Chance(r, 60) {
		return best
	}
	return r.Intn(len(adj))
}

func pickNeighbors(r *rand.Rand, neighbors []int, remaining map[int]struct{}, count int) []int {
	candidates := make([]int, 0, len(neighbors))
	for _, nb := range neighbors {
		if _, ok := remaining[nb]; ok {
			candidates = append(candidates, nb)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	r.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	if len(candidates) < count {
		count = len(candidates)
	}
	return candidates[:count]
}

func pickNeighborFromAnchors(r *rand.Rand, adj [][]int, anchors []int, remaining map[int]struct{}) int {
	candidates := map[int]struct{}{}
	for _, anchor := range anchors {
		for _, nb := range adj[anchor] {
			if _, ok := remaining[nb]; ok {
				candidates[nb] = struct{}{}
			}
		}
	}
	if len(candidates) == 0 {
		return -1
	}
	list := make([]int, 0, len(candidates))
	for nb := range candidates {
		list = append(list, nb)
	}
	return list[r.Intn(len(list))]
}

func mapJoinTables(tables []schema.Table, idxs []int) []schema.Table {
	picked := make([]schema.Table, 0, len(idxs))
	for _, idx := range idxs {
		picked = append(picked, tables[idx])
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

func (g *Generator) indexPrefixProb() int {
	if g.Adaptive != nil && g.Adaptive.IndexPrefixProb >= 0 {
		return g.Adaptive.IndexPrefixProb
	}
	return g.Config.Weights.Features.IndexPrefixProb
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

func (g *Generator) pickUsingColumns(left []schema.Table, right schema.Table) []string {
	useIndexPrefix := util.Chance(g.Rand, g.indexPrefixProb())
	// USING requires same column names; we only relax type matching by category (number/string/time/bool).
	leftCounts := map[string]int{}
	leftTypes := map[string]schema.ColumnType{}
	for _, ltbl := range left {
		for _, lcol := range g.collectJoinColumns(ltbl, useIndexPrefix) {
			leftCounts[lcol.Name]++
			if _, ok := leftTypes[lcol.Name]; !ok {
				leftTypes[lcol.Name] = lcol.Type
			}
		}
	}
	names := []string{}
	for _, ltbl := range left {
		pairs := g.collectJoinPairs(ltbl, right, true, useIndexPrefix)
		for _, pair := range pairs {
			if leftCounts[pair.Left.Name] != 1 {
				continue
			}
			if !compatibleColumnType(leftTypes[pair.Left.Name], pair.Left.Type) {
				continue
			}
			names = append(names, pair.Left.Name)
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

func (g *Generator) collectIndexPrefixColumns(tables []schema.Table) []ColumnRef {
	cols := make([]ColumnRef, 0, 8)
	seen := map[string]struct{}{}
	for _, tbl := range tables {
		for _, col := range tbl.Columns {
			if !col.HasIndex {
				continue
			}
			key := tbl.Name + "." + col.Name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			cols = append(cols, ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type})
		}
		for _, idx := range tbl.Indexes {
			if len(idx.Columns) == 0 {
				continue
			}
			name := idx.Columns[0]
			col, ok := tbl.ColumnByName(name)
			if !ok {
				continue
			}
			key := tbl.Name + "." + col.Name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			cols = append(cols, ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type})
		}
	}
	return cols
}

func (g *Generator) pickJoinColumnPair(left []schema.Table, right schema.Table) (leftCol ColumnRef, rightCol ColumnRef, ok bool) {
	if util.Chance(g.Rand, g.indexPrefixProb()) {
		for _, ltbl := range left {
			pairs := g.collectJoinPairs(ltbl, right, true, true)
			if len(pairs) > 0 {
				pair := pairs[g.Rand.Intn(len(pairs))]
				leftCol, rightCol, ok = pair.Left, pair.Right, true
				return
			}
		}
		for _, ltbl := range left {
			pairs := g.collectJoinPairs(ltbl, right, false, true)
			if len(pairs) > 0 {
				pair := pairs[g.Rand.Intn(len(pairs))]
				leftCol, rightCol, ok = pair.Left, pair.Right, true
				return
			}
		}
	}
	for _, ltbl := range left {
		pairs := g.collectJoinPairs(ltbl, right, true, false)
		if len(pairs) > 0 {
			pair := pairs[g.Rand.Intn(len(pairs))]
			leftCol, rightCol, ok = pair.Left, pair.Right, true
			return
		}
	}
	for _, ltbl := range left {
		pairs := g.collectJoinPairs(ltbl, right, false, false)
		if len(pairs) > 0 {
			pair := pairs[g.Rand.Intn(len(pairs))]
			leftCol, rightCol, ok = pair.Left, pair.Right, true
			return
		}
	}
	return
}

func (g *Generator) pickCorrelatedJoinPair(outerTables []schema.Table, inner schema.Table) (outerCol ColumnRef, innerCol ColumnRef, ok bool) {
	if len(outerTables) == 0 {
		return ColumnRef{}, ColumnRef{}, false
	}
	outerOrder := g.Rand.Perm(len(outerTables))
	tryPick := func(requireSameName bool, useIndexPrefix bool) (ColumnRef, ColumnRef, bool) {
		for _, idx := range outerOrder {
			pairs := g.collectJoinPairs(outerTables[idx], inner, requireSameName, useIndexPrefix)
			if len(pairs) == 0 {
				continue
			}
			pair := pairs[g.Rand.Intn(len(pairs))]
			return pair.Left, pair.Right, true
		}
		return ColumnRef{}, ColumnRef{}, false
	}

	preferIndexPrefix := util.Chance(g.Rand, g.indexPrefixProb())
	prefixFirst := []bool{true, false}
	if !preferIndexPrefix {
		prefixFirst = []bool{false, true}
	}
	for _, useIndexPrefix := range prefixFirst {
		if outerCol, innerCol, ok = tryPick(true, useIndexPrefix); ok {
			return outerCol, innerCol, true
		}
		if outerCol, innerCol, ok = tryPick(false, useIndexPrefix); ok {
			return outerCol, innerCol, true
		}
	}
	return ColumnRef{}, ColumnRef{}, false
}

func (g *Generator) trueExpr() Expr {
	return BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}}
}

func tableHasIndexPrefixColumn(tbl schema.Table, name string) bool {
	for _, idx := range tbl.Indexes {
		if len(idx.Columns) == 0 {
			continue
		}
		if idx.Columns[0] == name {
			return true
		}
	}
	return false
}

// compatibleColumnType and typeCategory are defined in type_compat.go.
