package generator

import (
	"fmt"
	"strings"

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
	allowSubquery := g.Config.Features.Subqueries && !g.disallowScalarSubq
	subqueryDisallowReason := ""
	if !allowSubquery {
		if g.subqueryConstraintDisallow {
			subqueryDisallowReason = "constraint:subquery"
		} else if !g.Config.Features.Subqueries {
			subqueryDisallowReason = "config:subqueries_off"
		} else if g.disallowScalarSubq {
			subqueryDisallowReason = "scalar_subquery_off"
		} else {
			subqueryDisallowReason = "subquery_disabled"
		}
	}

	if !g.Config.TQS.Enabled {
		if query := g.generateTemplateQuery(baseTables); query != nil {
			if hasCrossOrTrueJoin(query.From) && len(query.OrderBy) == 0 {
				query.OrderBy = g.ensureDeterministicOrderBy(query, baseTables)
			}
			if !g.validateQueryScope(query) {
				return nil
			}
			g.setLastFeatures(query, allowSubquery, subqueryDisallowReason)
			return query
		}
	}

	query := &SelectQuery{}
	queryTables := append([]schema.Table{}, baseTables...)

	if g.Config.Features.CTE && (util.Chance(g.Rand, g.Config.Weights.Features.CTECount*10) || (len(baseTables) > 1 && util.Chance(g.Rand, CTEExtraProb))) {
		cteCount := g.Rand.Intn(CTECountMax) + 1
		cteTables := make([]schema.Table, 0, cteCount)
		for i := 0; i < cteCount; i++ {
			cteBase := baseTables[g.Rand.Intn(len(baseTables))]
			if len(cteTables) > 0 && util.Chance(g.Rand, 30) {
				cteBase = cteTables[g.Rand.Intn(len(cteTables))]
			}
			cteQuery := g.GenerateCTEQuery(cteBase)
			cteName := fmt.Sprintf("cte_%d", i)
			cteCols := g.columnsFromSelectItems(cteQuery.Items)
			if len(cteCols) == 0 {
				cteCols = cteBase.Columns
			}
			query.With = append(query.With, CTE{Name: cteName, Query: cteQuery})
			cteTable := schema.Table{Name: cteName, Columns: cteCols}
			queryTables = append(queryTables, cteTable)
			cteTables = append(cteTables, cteTable)
		}
	}

	if !g.Config.Features.DSG {
		queryTables = g.maybeShuffleTables(queryTables)
	}
	queryTables = g.positionCTETables(queryTables, query.With)
	query.From = g.buildFromClause(queryTables)
	query.Items = g.GenerateSelectList(queryTables)

	if util.Chance(g.Rand, g.Config.Weights.Features.DistinctProb) && g.Config.Features.Distinct {
		query.Distinct = true
	}

	switch g.predicateMode {
	case PredicateModeNone:
		query.Where = nil
	case PredicateModeSimple:
		query.Where = g.GenerateSimplePredicate(queryTables, min(2, g.maxDepth))
	case PredicateModeSimpleColumns:
		query.Where = g.GenerateSimplePredicateColumns(queryTables, min(2, g.maxDepth))
	default:
		query.Where = g.GeneratePredicate(queryTables, g.maxDepth, allowSubquery, g.maxSubqDepth)
	}

	if g.Config.Features.Aggregates && util.Chance(g.Rand, g.aggProb()) {
		withGroupBy := g.Config.Features.GroupBy && util.Chance(g.Rand, g.Config.Weights.Features.GroupByProb)
		if withGroupBy {
			query.GroupBy = g.GenerateGroupBy(queryTables)
		}
		query.Items = g.GenerateAggregateSelectList(queryTables, query.GroupBy)
		if g.Config.Features.Having && len(query.GroupBy) > 0 && util.Chance(g.Rand, g.Config.Weights.Features.HavingProb) {
			query.Having = g.GenerateHavingPredicate(query.GroupBy, queryTables)
		}
		if len(query.GroupBy) > 0 && util.Chance(g.Rand, g.groupByOrdinalProb()) {
			query.GroupBy = g.wrapGroupByOrdinals(query.GroupBy)
		}
	}

	// Only emit LIMIT when paired with ORDER BY to keep top-N semantics.
	if g.Config.Features.OrderBy && util.Chance(g.Rand, g.Config.Weights.Features.OrderByProb) {
		query.OrderBy = g.orderByForQuery(query, queryTables)
		if g.Config.Features.Limit && util.Chance(g.Rand, g.Config.Weights.Features.LimitProb) {
			limit := g.Rand.Intn(LimitMax) + 1
			query.Limit = &limit
		}
	}
	if hasCrossOrTrueJoin(query.From) && len(query.OrderBy) == 0 {
		query.OrderBy = g.ensureDeterministicOrderBy(query, queryTables)
	}

	if !g.validateQueryScope(query) {
		return nil
	}
	g.setLastFeatures(query, allowSubquery, subqueryDisallowReason)
	return query
}

// (constraints-based builder is implemented in select_query_builder.go)

func (g *Generator) setLastFeatures(query *SelectQuery, allowSubquery bool, subqueryDisallowReason string) {
	if query == nil {
		return
	}
	queryFeatures := AnalyzeQueryFeatures(query)
	queryFeatures.ViewCount = g.countViewsInQuery(query)
	queryFeatures.PredicatePairsTotal = g.predicatePairsTotal
	queryFeatures.PredicatePairsJoin = g.predicatePairsJoin
	queryFeatures.SubqueryAllowed = allowSubquery
	queryFeatures.SubqueryDisallowReason = subqueryDisallowReason
	queryFeatures.SubqueryAttempts = g.subqueryAttempts
	queryFeatures.SubqueryBuilt = g.subqueryBuilt
	queryFeatures.SubqueryFailed = g.subqueryFailed
	g.LastFeatures = &queryFeatures
}

func (g *Generator) ensureDeterministicOrderBy(query *SelectQuery, tables []schema.Table) []OrderBy {
	if query == nil {
		return nil
	}
	if g.queryRequiresSelectOrder(query) {
		if ob := g.orderByFromItemsStable(query.Items); len(ob) > 0 {
			return ob
		}
	}
	return g.ensureOrderByDistinctColumns(g.deterministicOrderBy(tables), tables)
}

func (g *Generator) orderByForQuery(query *SelectQuery, tables []schema.Table) []OrderBy {
	if query == nil {
		return nil
	}
	if g.queryRequiresSelectOrder(query) {
		return g.orderByFromItemsStable(query.Items)
	}
	return g.ensureOrderByDistinctColumns(g.GenerateOrderBy(tables), tables)
}

func (g *Generator) queryRequiresSelectOrder(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	if query.Distinct || len(query.GroupBy) > 0 {
		return true
	}
	for _, item := range query.Items {
		if ExprHasAggregate(item.Expr) {
			return true
		}
	}
	return false
}

func (g *Generator) countViewsInQuery(query *SelectQuery) int {
	if g == nil || g.State == nil || query == nil {
		return 0
	}
	names := []string{query.From.BaseTable}
	for _, join := range query.From.Joins {
		names = append(names, join.Table)
	}
	count := 0
	for _, name := range names {
		if name == "" {
			continue
		}
		if tbl, ok := g.State.TableByName(name); ok && tbl.IsView {
			count++
		}
	}
	return count
}

func (g *Generator) positionCTETables(tables []schema.Table, with []CTE) []schema.Table {
	if len(tables) == 0 || len(with) == 0 {
		return tables
	}
	cteNames := make(map[string]struct{}, len(with))
	for _, cte := range with {
		cteNames[cte.Name] = struct{}{}
	}
	cteIdx := make([]int, 0, len(with))
	nonCTEIdx := make([]int, 0, len(tables))
	for i, tbl := range tables {
		if _, ok := cteNames[tbl.Name]; ok {
			cteIdx = append(cteIdx, i)
		} else {
			nonCTEIdx = append(nonCTEIdx, i)
		}
	}
	if len(cteIdx) == 0 {
		return tables
	}
	if util.Chance(g.Rand, 50) {
		swapIdx := cteIdx[g.Rand.Intn(len(cteIdx))]
		tables[0], tables[swapIdx] = tables[swapIdx], tables[0]
		return tables
	}
	if len(nonCTEIdx) == 0 {
		return tables
	}
	if _, ok := cteNames[tables[0].Name]; ok {
		swapIdx := nonCTEIdx[g.Rand.Intn(len(nonCTEIdx))]
		tables[0], tables[swapIdx] = tables[swapIdx], tables[0]
	}
	return tables
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

func (g *Generator) columnsFromSelectItems(items []SelectItem) []schema.Column {
	if len(items) == 0 {
		return nil
	}
	items = ensureUniqueAliases(items)
	cols := make([]schema.Column, 0, len(items))
	for i, item := range items {
		name := strings.TrimSpace(item.Alias)
		if name == "" {
			name = fmt.Sprintf("c%d", i)
		}
		colType, ok := g.exprType(item.Expr)
		if !ok {
			colType = schema.TypeVarchar
		}
		cols = append(cols, schema.Column{Name: name, Type: colType})
	}
	return cols
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
