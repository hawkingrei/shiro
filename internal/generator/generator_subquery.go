package generator

import (
	"shiro/internal/schema"
	"shiro/internal/util"
)

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
			g.applySubqueryOrderLimit(query, inner)
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
			g.applySubqueryOrderLimit(query, inner)
			return query
		}
	}

	allowNested := subqDepth > 0 && util.Chance(g.Rand, SubqueryNestProb)
	query.Where = g.GeneratePredicate([]schema.Table{inner}, 1, allowNested, subqDepth)
	g.applySubqueryOrderLimit(query, inner)
	return query
}

func (g *Generator) generateInSubquery(outerTables []schema.Table, leftType schema.ColumnType, subqDepth int) *SelectQuery {
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
			g.applySubqueryOrderLimit(query, inner)
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
			g.applySubqueryOrderLimit(query, inner)
			return query
		}
	}

	allowNested := subqDepth > 0 && util.Chance(g.Rand, SubqueryNestProb)
	query.Where = g.GeneratePredicate([]schema.Table{inner}, 1, allowNested, subqDepth)
	g.applySubqueryOrderLimit(query, inner)
	return query
}

func (g *Generator) generateExistsSubquery(outerTables []schema.Table, subqDepth int) *SelectQuery {
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
			g.applySubqueryOrderLimit(query, inner)
			return query
		}
	}
	allowNested := subqDepth > 0 && util.Chance(g.Rand, SubqueryNestProb)
	query.Where = g.GeneratePredicate([]schema.Table{inner}, 1, allowNested, subqDepth)
	g.applySubqueryOrderLimit(query, inner)
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

func (g *Generator) applySubqueryOrderLimit(query *SelectQuery, inner schema.Table) {
	if query == nil || !g.Config.Features.OrderBy || !g.Config.Features.Limit {
		return
	}
	if !util.Chance(g.Rand, SubqueryLimitProb) {
		return
	}
	limit := g.Rand.Intn(CTELimitMax) + 1
	query.Limit = &limit
	if util.Chance(g.Rand, SubqueryOrderProb) {
		query.OrderBy = g.GenerateOrderBy([]schema.Table{inner})
	}
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
