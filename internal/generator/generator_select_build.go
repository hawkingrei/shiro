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
		maxCTE := g.cteCountMax()
		if maxCTE <= 0 {
			maxCTE = CTECountMax
		}
		cteCount := g.Rand.Intn(maxCTE) + 1
		cteTables := make([]schema.Table, 0, cteCount)
		for i := 0; i < cteCount; i++ {
			cteName := fmt.Sprintf("cte_%d", i)
			cteBase := baseTables[g.Rand.Intn(len(baseTables))]
			if len(cteTables) > 0 && util.Chance(g.Rand, 30) {
				cteBase = cteTables[g.Rand.Intn(len(cteTables))]
			}
			cteQuery := g.GenerateCTEQuery(cteBase)
			if i == 0 && g.Config.Features.RecursiveCTE && util.Chance(g.Rand, RecursiveCTEProb) {
				if rq := g.GenerateRecursiveCTEQuery(cteBase, cteName); rq != nil {
					cteQuery = rq
					query.WithRecursive = true
				}
			}
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
	derivedTables := g.buildDerivedTableMap(queryTables)
	query.From = g.buildFromClause(queryTables, derivedTables)
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
		if g.applyGroupByExtension(query) {
			g.maybeAppendGroupingSelectItem(query)
		}
	}

	if g.Config.Features.FullJoinEmulation {
		g.maybeEmulateFullJoin(query)
	}
	g.attachNamedWindows(query)

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
	g.maybeAttachSetOperations(query, queryTables)
	// Current set-operation modeling does not track expression-level ORDER BY/LIMIT.
	// Keep this normalized so we do not accidentally bind ORDER/LIMIT to one branch.
	clearSetOperationOrderLimit(query)

	if !g.validateQueryScope(query) {
		return nil
	}
	g.setLastFeatures(query, allowSubquery, subqueryDisallowReason)
	return query
}

func (g *Generator) attachNamedWindows(query *SelectQuery) {
	if query == nil || !g.Config.Features.WindowFuncs || !util.Chance(g.Rand, NamedWindowProb) {
		return
	}
	for idx, item := range query.Items {
		win, ok := item.Expr.(WindowExpr)
		if !ok {
			continue
		}
		if win.WindowName != "" {
			continue
		}
		name := fmt.Sprintf("w%d", g.Rand.Intn(4))
		query.WindowDefs = append(query.WindowDefs, WindowDef{
			Name:        name,
			PartitionBy: append([]Expr{}, win.PartitionBy...),
			OrderBy:     append([]OrderBy{}, win.OrderBy...),
			Frame:       win.Frame,
		})
		win.WindowName = name
		win.PartitionBy = nil
		win.OrderBy = nil
		win.Frame = nil
		query.Items[idx].Expr = win
		return
	}
}

// (constraints-based builder is implemented in select_query_builder.go)

func (g *Generator) buildDerivedTableMap(tables []schema.Table) map[string]*SelectQuery {
	if !g.Config.Features.DerivedTables || len(tables) == 0 {
		return nil
	}
	derived := make(map[string]*SelectQuery, len(tables))
	for _, tbl := range tables {
		if !util.Chance(g.Rand, DerivedTableProb) {
			continue
		}
		subq := g.buildDerivedTableQuery(tbl)
		if subq == nil {
			continue
		}
		derived[tbl.Name] = subq
	}
	if len(derived) == 0 {
		return nil
	}
	return derived
}

func (g *Generator) buildDerivedTableQuery(tbl schema.Table) *SelectQuery {
	if tbl.Name == "" || len(tbl.Columns) == 0 {
		return nil
	}
	items := make([]SelectItem, 0, len(tbl.Columns))
	for i, col := range tbl.Columns {
		alias := col.Name
		if alias == "" {
			alias = fmt.Sprintf("c%d", i)
		}
		items = append(items, SelectItem{
			Expr:  ColumnExpr{Ref: ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}},
			Alias: alias,
		})
	}
	return &SelectQuery{
		Items: items,
		From:  FromClause{BaseTable: tbl.Name},
	}
}

func (g *Generator) maybeAttachSetOperations(query *SelectQuery, tables []schema.Table) {
	if !g.Config.Features.SetOperations || query == nil || len(query.Items) == 0 {
		return
	}
	if !util.Chance(g.Rand, SetOperationProb) {
		return
	}
	opCount := 1
	if util.Chance(g.Rand, SetOperationChainProb) {
		opCount++
	}
	ops := make([]SetOperation, 0, opCount)
	for i := 0; i < opCount; i++ {
		rhs := g.buildSetOperationQuery(query.Items, tables)
		if rhs == nil {
			break
		}
		opType := g.pickSetOperationType()
		ops = append(ops, SetOperation{
			Type:  opType,
			All:   g.pickSetOperationAll(opType),
			Query: rhs,
		})
	}
	if len(ops) == 0 {
		return
	}
	query.SetOps = append(query.SetOps, ops...)
	clearSetOperationOrderLimit(query)
}

func clearSetOperationOrderLimit(query *SelectQuery) {
	clearSetOperationOrderLimitInSetExpr(query, false)
}

func clearSetOperationOrderLimitInSetExpr(query *SelectQuery, inSetExpr bool) {
	if query == nil {
		return
	}
	currentInSetExpr := inSetExpr || len(query.SetOps) > 0
	if currentInSetExpr {
		query.OrderBy = nil
		query.Limit = nil
	}
	for i := range query.With {
		clearSetOperationOrderLimitInSetExpr(query.With[i].Query, false)
	}
	if query.From.BaseQuery != nil {
		clearSetOperationOrderLimitInSetExpr(query.From.BaseQuery, false)
	}
	for i := range query.From.Joins {
		if query.From.Joins[i].TableQuery != nil {
			clearSetOperationOrderLimitInSetExpr(query.From.Joins[i].TableQuery, false)
		}
	}
	for _, op := range query.SetOps {
		clearSetOperationOrderLimitInSetExpr(op.Query, true)
	}
}

func (g *Generator) buildSetOperationQuery(baseItems []SelectItem, tables []schema.Table) *SelectQuery {
	if len(baseItems) == 0 {
		return nil
	}
	rhsTables := append([]schema.Table{}, tables...)
	if len(rhsTables) == 0 {
		rhsTables = g.pickTables()
	}
	if len(rhsTables) == 0 {
		return nil
	}
	if !g.Config.Features.DSG {
		rhsTables = g.maybeShuffleTables(rhsTables)
	}
	if len(rhsTables) > 1 && util.Chance(g.Rand, 40) {
		rhsTables = rhsTables[:g.Rand.Intn(len(rhsTables))+1]
	}
	derived := g.buildDerivedTableMap(rhsTables)
	query := &SelectQuery{
		Items: g.buildSetOperationItems(baseItems, rhsTables),
		From:  g.buildFromClause(rhsTables, derived),
	}
	if len(query.Items) == 0 {
		return nil
	}
	allowSubquery := g.Config.Features.Subqueries && !g.disallowScalarSubq
	if util.Chance(g.Rand, 45) {
		query.Where = g.GeneratePredicate(rhsTables, min(2, g.maxDepth), allowSubquery, g.maxSubqDepth)
	}
	if g.Config.Features.Aggregates && util.Chance(g.Rand, g.aggProb()/2) {
		withGroupBy := g.Config.Features.GroupBy && util.Chance(g.Rand, g.Config.Weights.Features.GroupByProb)
		if withGroupBy {
			query.GroupBy = g.GenerateGroupBy(rhsTables)
			g.applyGroupByExtension(query)
		}
		query.Items = g.GenerateAggregateSelectList(rhsTables, query.GroupBy)
		if len(query.Items) != len(baseItems) {
			query.Items = g.buildSetOperationItems(baseItems, rhsTables)
		}
		if g.Config.Features.Having && len(query.GroupBy) > 0 && util.Chance(g.Rand, g.Config.Weights.Features.HavingProb/2) {
			query.Having = g.GenerateHavingPredicate(query.GroupBy, rhsTables)
		}
	}
	return query
}

func (g *Generator) buildSetOperationItems(baseItems []SelectItem, tables []schema.Table) []SelectItem {
	items := make([]SelectItem, 0, len(baseItems))
	for i, base := range baseItems {
		alias := base.Alias
		if alias == "" {
			alias = fmt.Sprintf("c%d", i)
		}
		expr, colType, ok := g.pickComparableExprPreferJoinGraph(tables)
		if t, hasType := g.exprType(base.Expr); hasType {
			colType = t
			ok = true
		}
		if ok {
			if col, found := g.pickCompatibleColumnRef(tables, colType); found {
				expr = ColumnExpr{Ref: col}
			} else {
				expr = g.literalForColumn(schema.Column{Type: colType})
			}
		}
		items = append(items, SelectItem{Expr: expr, Alias: alias})
	}
	return items
}

func (g *Generator) pickCompatibleColumnRef(tables []schema.Table, colType schema.ColumnType) (ColumnRef, bool) {
	if colType == 0 {
		return ColumnRef{}, false
	}
	cols := g.collectColumns(tables)
	if len(cols) == 0 {
		return ColumnRef{}, false
	}
	candidates := make([]ColumnRef, 0, len(cols))
	for _, col := range cols {
		if compatibleColumnType(col.Type, colType) {
			candidates = append(candidates, col)
		}
	}
	if len(candidates) == 0 {
		return ColumnRef{}, false
	}
	return candidates[g.Rand.Intn(len(candidates))], true
}

func (g *Generator) pickSetOperationType() SetOperationType {
	switch g.Rand.Intn(3) {
	case 0:
		return SetOperationUnion
	case 1:
		return SetOperationIntersect
	default:
		return SetOperationExcept
	}
}

func (g *Generator) pickSetOperationAll(opType SetOperationType) bool {
	switch opType {
	case SetOperationUnion:
		return util.Chance(g.Rand, 40)
	default:
		// TiDB does not support INTERSECT ALL / EXCEPT ALL.
		return false
	}
}

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
	queryFeatures.FullJoinEmulationAttempted = g.fullJoinEmulationAttempted
	queryFeatures.FullJoinEmulationRejectReason = g.fullJoinEmulationReject
	g.LastFeatures = &queryFeatures
	g.setQueryAnalysisWithFeatures(query, queryFeatures)
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

func (g *Generator) maybeAppendGroupingSelectItem(query *SelectQuery) {
	if query == nil || len(query.GroupBy) == 0 {
		return
	}
	groupExpr := query.GroupBy[0]
	if ord, ok := groupExpr.(GroupByOrdinalExpr); ok && ord.Expr != nil {
		groupExpr = ord.Expr
	}
	query.Items = append(query.Items, SelectItem{
		Expr:  FuncExpr{Name: "GROUPING", Args: []Expr{groupExpr}},
		Alias: "grp_flag",
	})
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

// GenerateRecursiveCTEQuery builds a simple numeric recursive CTE body.
func (g *Generator) GenerateRecursiveCTEQuery(tbl schema.Table, cteName string) *SelectQuery {
	if cteName == "" {
		return nil
	}
	seedCol, ok := pickFirstNumericColumn(tbl)
	if !ok {
		return nil
	}
	seedRef := ColumnRef{Table: tbl.Name, Name: seedCol.Name, Type: seedCol.Type}
	one := 1
	seed := &SelectQuery{
		Items: []SelectItem{{
			Expr:  ColumnExpr{Ref: seedRef},
			Alias: "c0",
		}},
		From: FromClause{BaseTable: tbl.Name},
		OrderBy: []OrderBy{{
			Expr: ColumnExpr{Ref: seedRef},
		}},
		Limit: &one,
	}
	recursiveRef := ColumnRef{Table: cteName, Name: "c0", Type: seedCol.Type}
	recursive := &SelectQuery{
		Items: []SelectItem{{
			Expr:  BinaryExpr{Left: ColumnExpr{Ref: recursiveRef}, Op: "+", Right: LiteralExpr{Value: 1}},
			Alias: "c0",
		}},
		From:  FromClause{BaseTable: cteName},
		Where: BinaryExpr{Left: ColumnExpr{Ref: recursiveRef}, Op: "<", Right: LiteralExpr{Value: 3}},
	}
	seed.SetOps = []SetOperation{{
		Type:  SetOperationUnion,
		All:   true,
		Query: recursive,
	}}
	return seed
}

func pickFirstNumericColumn(tbl schema.Table) (schema.Column, bool) {
	for _, col := range tbl.Columns {
		switch col.Type {
		case schema.TypeInt, schema.TypeBigInt, schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
			return col, true
		}
	}
	return schema.Column{}, false
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
