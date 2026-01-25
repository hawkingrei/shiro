package generator

import (
	"fmt"

	"shiro/internal/schema"
	"shiro/internal/util"
)

// GenerateSelectQuery builds a randomized SELECT query for current schema.
func (g *Generator) GenerateSelectQuery() *SelectQuery {
	baseTables := g.pickTables()
	if len(baseTables) == 0 {
		return nil
	}
	g.resetPredicateStats()

	if !g.Config.TQS.Enabled {
		if query := g.generateTemplateQuery(baseTables); query != nil {
			if hasCrossOrTrueJoin(query.From) && len(query.OrderBy) == 0 {
				query.OrderBy = g.deterministicOrderBy(baseTables)
			}
			queryFeatures := AnalyzeQueryFeatures(query)
			queryFeatures.PredicatePairsTotal = g.predicatePairsTotal
			queryFeatures.PredicatePairsJoin = g.predicatePairsJoin
			g.LastFeatures = &queryFeatures
			return query
		}
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

	if !g.Config.Features.DSG {
		queryTables = g.maybeShuffleTables(queryTables)
	}
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
	if hasCrossOrTrueJoin(query.From) && len(query.OrderBy) == 0 {
		query.OrderBy = g.deterministicOrderBy(queryTables)
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
