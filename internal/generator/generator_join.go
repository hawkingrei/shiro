package generator

import (
	"math/rand"
	"reflect"
	"strconv"
	"strings"

	"shiro/internal/schema"
	"shiro/internal/util"
)

type joinShape int

const (
	joinShapeChain joinShape = iota
	joinShapeStar
	joinShapeSnowflake
)

type groupedAggregateLateralMode int

const (
	groupedAggregateLateralModeWhere groupedAggregateLateralMode = iota
	groupedAggregateLateralModeOuterFilteredWhere
	groupedAggregateLateralModeMultiFilteredWhere
	groupedAggregateLateralModeOuterCorrelatedGroupKey
	groupedAggregateLateralModeCaseCorrelatedGroupKey
	groupedAggregateLateralModeNestedCaseCorrelatedGroupKey
	groupedAggregateLateralModeWrappedNestedCaseCorrelatedGroupKey
	groupedAggregateLateralModeGroupKeyHaving
	groupedAggregateLateralModeAggregateValueHaving
)

type columnPair struct {
	Left  ColumnRef
	Right ColumnRef
}

type mergedColumnCandidate struct {
	Natural bool
	Column  ColumnRef
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
	var cols []ColumnRef
	if useIndexPrefix {
		cols = g.collectIndexPrefixColumns([]schema.Table{tbl})
	} else {
		cols = g.collectColumns([]schema.Table{tbl})
	}
	if !g.Config.Features.DSG {
		return cols
	}
	filtered := make([]ColumnRef, 0, len(cols))
	for _, col := range cols {
		if strings.HasPrefix(col.Name, "k") {
			filtered = append(filtered, col)
		}
	}
	if len(filtered) > 0 {
		return filtered
	}
	if useIndexPrefix {
		allCols := g.collectColumns([]schema.Table{tbl})
		filtered = filtered[:0]
		for _, col := range allCols {
			if strings.HasPrefix(col.Name, "k") {
				filtered = append(filtered, col)
			}
		}
		if len(filtered) > 0 {
			return filtered
		}
	}
	return cols
}

func (g *Generator) collectJoinPairs(left schema.Table, right schema.Table, requireSameName bool, useIndexPrefix bool) []columnPair {
	leftCols := g.collectJoinColumns(left, useIndexPrefix)
	rightCols := g.collectJoinColumns(right, useIndexPrefix)
	if len(leftCols) == 0 || len(rightCols) == 0 {
		return nil
	}
	if g.Config.Features.DSG {
		requireSameName = true
	}
	pairs := make([]columnPair, 0, 8)
	for _, l := range leftCols {
		for _, r := range rightCols {
			if requireSameName && l.Name != r.Name {
				continue
			}
			if g.Config.Features.DSG {
				if !dsgAllowedJoinKey(left.Name, l.Name) || !dsgAllowedJoinKey(right.Name, r.Name) {
					continue
				}
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

func dsgAllowedJoinKey(tableName, colName string) bool {
	if tableName == "t0" {
		return strings.HasPrefix(colName, "k")
	}
	if !strings.HasPrefix(tableName, "t") || len(tableName) < 2 {
		return false
	}
	idx := 0
	for i := 1; i < len(tableName); i++ {
		ch := tableName[i]
		if ch < '0' || ch > '9' {
			return false
		}
		idx = idx*10 + int(ch-'0')
	}
	if idx <= 0 {
		return false
	}
	if colName == "k0" {
		return true
	}
	return colName == "k"+strconv.Itoa(idx-1)
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

func (g *Generator) groupByOrdinalProb() int {
	if g.Adaptive != nil && g.Adaptive.GroupByOrdProb >= 0 {
		return g.Adaptive.GroupByOrdProb
	}
	return GroupByOrdinalBaseProb
}

func (g *Generator) joinUsingProb() int {
	if g.Config.Oracles.JoinUsingProb >= 0 {
		return g.Config.Oracles.JoinUsingProb
	}
	return UsingJoinProb
}

func (g *Generator) pickSupportedLateralJoinType() (JoinType, bool) {
	if g.joinTypeOverride != nil {
		switch *g.joinTypeOverride {
		case JoinCross, JoinInner:
			return *g.joinTypeOverride, true
		default:
			return "", false
		}
	}
	if util.Chance(g.Rand, 50) {
		return JoinCross, true
	}
	return JoinInner, true
}

func (g *Generator) buildLateralDerivedTableQuery(outerTables []schema.Table, inner schema.Table) *SelectQuery {
	query := g.buildDerivedTableQuery(inner)
	if query == nil {
		return nil
	}
	if outerCol, innerCol, ok := g.pickCorrelatedJoinPair(outerTables, inner); ok {
		query.Where = BinaryExpr{
			Left:  ColumnExpr{Ref: innerCol},
			Op:    "=",
			Right: ColumnExpr{Ref: outerCol},
		}
	} else if util.Chance(g.Rand, 50) {
		query.Where = g.GenerateSimplePredicateColumns([]schema.Table{inner}, min(2, g.maxDepth))
	}
	if g.Config.Features.OrderBy && g.Config.Features.Limit && util.Chance(g.Rand, LateralJoinOrderLimitProb) {
		query.OrderBy = g.orderByForQuery(query, []schema.Table{inner})
		if len(query.OrderBy) > 0 {
			limit := g.Rand.Intn(LateralJoinLimitMax) + 1
			query.Limit = &limit
			query.OrderBy = g.ensureLimitOrderByTieBreaker(query, []schema.Table{inner})
		}
	}
	return query
}

func (g *Generator) buildCorrelatedOrderLimitLateralQuery(outerTables []schema.Table, inner schema.Table) *SelectQuery {
	if g == nil || !g.Config.Features.OrderBy || !g.Config.Features.Limit {
		return nil
	}
	outerCol, innerCol, ok := g.pickCorrelatedJoinPair(outerTables, inner)
	if !ok {
		return nil
	}
	query := g.buildDerivedTableQuery(inner)
	if query == nil {
		return nil
	}
	query.Where = BinaryExpr{
		Left:  ColumnExpr{Ref: innerCol},
		Op:    "=",
		Right: ColumnExpr{Ref: outerCol},
	}
	query.OrderBy = g.orderByForQuery(query, []schema.Table{inner})
	if len(query.OrderBy) == 0 {
		return nil
	}
	limit := g.Rand.Intn(LateralJoinLimitMax) + 1
	query.Limit = &limit
	query.OrderBy = g.ensureLimitOrderByTieBreaker(query, []schema.Table{inner})
	return query
}

func (g *Generator) buildGroupedAggregateLateralHookQuery(tables []schema.Table) *SelectQuery {
	if g == nil || !g.Config.Features.Joins || !g.Config.Features.LateralJoins || !g.Config.Features.Aggregates || !g.Config.Features.GroupBy || g.Config.Features.DSG || len(tables) < 3 {
		return nil
	}
	if !util.Chance(g.Rand, LateralJoinGroupedAggregateProb) {
		return nil
	}
	variantOrder := []groupedAggregateLateralMode{
		groupedAggregateLateralModeWhere,
		groupedAggregateLateralModeOuterFilteredWhere,
		groupedAggregateLateralModeMultiFilteredWhere,
		groupedAggregateLateralModeOuterCorrelatedGroupKey,
		groupedAggregateLateralModeCaseCorrelatedGroupKey,
		groupedAggregateLateralModeNestedCaseCorrelatedGroupKey,
		groupedAggregateLateralModeWrappedNestedCaseCorrelatedGroupKey,
	}
	if g.Config.Features.Having {
		variantOrder = append(variantOrder,
			groupedAggregateLateralModeGroupKeyHaving,
			groupedAggregateLateralModeAggregateValueHaving,
		)
		g.Rand.Shuffle(len(variantOrder), func(i, j int) {
			variantOrder[i], variantOrder[j] = variantOrder[j], variantOrder[i]
		})
	}
	for _, mode := range variantOrder {
		for i := 0; i < len(tables); i++ {
			for j := 0; j < len(tables); j++ {
				if j == i {
					continue
				}
				for k := 0; k < len(tables); k++ {
					if k == i || k == j {
						continue
					}
					if query := g.buildGroupedAggregateLateralHookQueryForTables(tables[i], tables[j], tables[k], mode); query != nil {
						return query
					}
				}
			}
		}
	}
	return nil
}

func (g *Generator) buildGroupedAggregateLateralHookQueryForTables(base schema.Table, sibling schema.Table, inner schema.Table, mode groupedAggregateLateralMode) *SelectQuery {
	if g == nil {
		return nil
	}
	joinType, ok := g.pickSupportedLateralJoinType()
	if !ok {
		return nil
	}
	baseJoinCol, siblingJoinCol, ok := g.pickJoinColumnPair([]schema.Table{base}, sibling)
	if !ok {
		return nil
	}
	baseOuterCol, baseInnerCol, ok := g.pickJoinColumnPair([]schema.Table{base}, inner)
	if !ok {
		return nil
	}
	siblingOuterCol, groupInnerCol, ok := g.pickJoinColumnPair([]schema.Table{sibling}, inner)
	if !ok {
		return nil
	}
	sumRef, hasSum := g.pickAggregateValueColumnRef(inner, baseInnerCol.Name, groupInnerCol.Name)

	groupExpr := Expr(ColumnExpr{Ref: groupInnerCol})
	groupExprType := groupInnerCol.Type
	if mode == groupedAggregateLateralModeOuterCorrelatedGroupKey || mode == groupedAggregateLateralModeCaseCorrelatedGroupKey || mode == groupedAggregateLateralModeNestedCaseCorrelatedGroupKey || mode == groupedAggregateLateralModeWrappedNestedCaseCorrelatedGroupKey {
		if !g.isNumericType(groupInnerCol.Type) || !g.isNumericType(siblingOuterCol.Type) || !compatibleColumnType(groupInnerCol.Type, siblingOuterCol.Type) {
			return nil
		}
		switch mode {
		case groupedAggregateLateralModeOuterCorrelatedGroupKey:
			groupExpr = BinaryExpr{
				Left:  ColumnExpr{Ref: groupInnerCol},
				Op:    "+",
				Right: ColumnExpr{Ref: siblingOuterCol},
			}
		case groupedAggregateLateralModeCaseCorrelatedGroupKey:
			groupExpr = CaseExpr{
				Whens: []CaseWhen{
					{
						When: BinaryExpr{
							Left:  ColumnExpr{Ref: groupInnerCol},
							Op:    ">=",
							Right: ColumnExpr{Ref: siblingOuterCol},
						},
						Then: ColumnExpr{Ref: groupInnerCol},
					},
				},
				Else: ColumnExpr{Ref: siblingOuterCol},
			}
		case groupedAggregateLateralModeNestedCaseCorrelatedGroupKey:
			filterInnerRef, ok := g.pickGroupedAggregateFilterInnerRef(inner, sumRef, hasSum, baseInnerCol.Name)
			if !ok || !g.isNumericType(filterInnerRef.Type) || !compatibleColumnType(filterInnerRef.Type, siblingOuterCol.Type) {
				return nil
			}
			innerCase := CaseExpr{
				Whens: []CaseWhen{
					{
						When: BinaryExpr{
							Left:  ColumnExpr{Ref: filterInnerRef},
							Op:    ">=",
							Right: ColumnExpr{Ref: siblingOuterCol},
						},
						Then: ColumnExpr{Ref: groupInnerCol},
					},
				},
				Else: ColumnExpr{Ref: siblingOuterCol},
			}
			outerElse := CaseExpr{
				Whens: []CaseWhen{
					{
						When: BinaryExpr{
							Left:  ColumnExpr{Ref: filterInnerRef},
							Op:    ">=",
							Right: ColumnExpr{Ref: siblingOuterCol},
						},
						Then: ColumnExpr{Ref: siblingOuterCol},
					},
				},
				Else: ColumnExpr{Ref: groupInnerCol},
			}
			groupExpr = CaseExpr{
				Whens: []CaseWhen{
					{
						When: BinaryExpr{
							Left:  ColumnExpr{Ref: groupInnerCol},
							Op:    ">=",
							Right: ColumnExpr{Ref: siblingOuterCol},
						},
						Then: innerCase,
					},
				},
				Else: outerElse,
			}
		case groupedAggregateLateralModeWrappedNestedCaseCorrelatedGroupKey:
			filterInnerRef, ok := g.pickGroupedAggregateFilterInnerRef(inner, sumRef, hasSum, baseInnerCol.Name)
			if !ok || !g.isNumericType(filterInnerRef.Type) || !compatibleColumnType(filterInnerRef.Type, siblingOuterCol.Type) {
				return nil
			}
			diffInnerOuter := BinaryExpr{
				Left:  ColumnExpr{Ref: groupInnerCol},
				Op:    "-",
				Right: ColumnExpr{Ref: siblingOuterCol},
			}
			diffOuterInner := BinaryExpr{
				Left:  ColumnExpr{Ref: siblingOuterCol},
				Op:    "-",
				Right: ColumnExpr{Ref: groupInnerCol},
			}
			innerCase := CaseExpr{
				Whens: []CaseWhen{
					{
						When: BinaryExpr{
							Left:  ColumnExpr{Ref: filterInnerRef},
							Op:    ">=",
							Right: ColumnExpr{Ref: siblingOuterCol},
						},
						Then: diffInnerOuter,
					},
				},
				Else: diffOuterInner,
			}
			outerElse := CaseExpr{
				Whens: []CaseWhen{
					{
						When: BinaryExpr{
							Left:  ColumnExpr{Ref: filterInnerRef},
							Op:    ">=",
							Right: ColumnExpr{Ref: siblingOuterCol},
						},
						Then: diffOuterInner,
					},
				},
				Else: diffInnerOuter,
			}
			groupExpr = FuncExpr{
				Name: "ABS",
				Args: []Expr{
					CaseExpr{
						Whens: []CaseWhen{
							{
								When: BinaryExpr{
									Left:  ColumnExpr{Ref: groupInnerCol},
									Op:    ">=",
									Right: ColumnExpr{Ref: siblingOuterCol},
								},
								Then: innerCase,
							},
						},
						Else: outerElse,
					},
				},
			}
		}
	}

	lateralItems := make([]SelectItem, 0, 3)
	lateralItems = append(lateralItems,
		SelectItem{
			Expr:  groupExpr,
			Alias: "g0",
		},
		SelectItem{
			Expr:  FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}},
			Alias: "cnt",
		},
	)
	if hasSum {
		sumExpr := ColumnExpr{Ref: sumRef}
		g.warnAggOnDouble("SUM", sumExpr)
		lateralItems = append(lateralItems, SelectItem{
			Expr:  FuncExpr{Name: "SUM", Args: []Expr{sumExpr}},
			Alias: "sum1",
		})
	}

	where := Expr(BinaryExpr{
		Left:  ColumnExpr{Ref: baseInnerCol},
		Op:    "=",
		Right: ColumnExpr{Ref: baseOuterCol},
	})
	switch mode {
	case groupedAggregateLateralModeWhere:
		where = BinaryExpr{
			Left: where,
			Op:   "AND",
			Right: BinaryExpr{
				Left:  ColumnExpr{Ref: groupInnerCol},
				Op:    "=",
				Right: ColumnExpr{Ref: siblingOuterCol},
			},
		}
	case groupedAggregateLateralModeOuterFilteredWhere:
		filterOuterRef, ok := g.pickGroupedAggregateOuterNumericRef(base, sibling, baseJoinCol.Name, siblingJoinCol.Name)
		if !ok {
			return nil
		}
		filterInnerRef, ok := g.pickGroupedAggregateFilterInnerRef(inner, sumRef, hasSum, baseInnerCol.Name, groupInnerCol.Name)
		if !ok || !compatibleColumnType(filterInnerRef.Type, filterOuterRef.Type) {
			return nil
		}
		where = BinaryExpr{
			Left: where,
			Op:   "AND",
			Right: BinaryExpr{
				Left:  ColumnExpr{Ref: filterInnerRef},
				Op:    ">=",
				Right: ColumnExpr{Ref: filterOuterRef},
			},
		}
	case groupedAggregateLateralModeMultiFilteredWhere:
		filterOuterRef, ok := g.pickGroupedAggregateOuterNumericRef(base, sibling, baseJoinCol.Name, siblingJoinCol.Name)
		if !ok {
			return nil
		}
		filterInnerRef, ok := g.pickGroupedAggregateFilterInnerRef(inner, sumRef, hasSum, baseInnerCol.Name, groupInnerCol.Name)
		if !ok || !compatibleColumnType(filterInnerRef.Type, filterOuterRef.Type) {
			return nil
		}
		where = BinaryExpr{
			Left: BinaryExpr{
				Left: where,
				Op:   "AND",
				Right: BinaryExpr{
					Left:  ColumnExpr{Ref: groupInnerCol},
					Op:    "=",
					Right: ColumnExpr{Ref: siblingOuterCol},
				},
			},
			Op: "AND",
			Right: BinaryExpr{
				Left:  ColumnExpr{Ref: filterInnerRef},
				Op:    ">=",
				Right: ColumnExpr{Ref: filterOuterRef},
			},
		}
	case groupedAggregateLateralModeOuterCorrelatedGroupKey:
		// Keep the base equality anchor in WHERE and move sibling dependency into the grouped key itself.
	case groupedAggregateLateralModeCaseCorrelatedGroupKey:
		// Keep the base equality anchor in WHERE and route sibling dependency through a non-linear grouped key expression.
	case groupedAggregateLateralModeNestedCaseCorrelatedGroupKey:
		// Keep the base equality anchor in WHERE and route sibling dependency through nested CASE branches in the grouped key.
	case groupedAggregateLateralModeWrappedNestedCaseCorrelatedGroupKey:
		// Keep the base equality anchor in WHERE and route sibling dependency through nested CASE branches wrapped by a value-domain-changing function.
	}

	lateralQuery := &SelectQuery{
		Items:   lateralItems,
		From:    FromClause{BaseTable: inner.Name},
		Where:   where,
		GroupBy: []Expr{groupExpr},
	}
	switch mode {
	case groupedAggregateLateralModeGroupKeyHaving:
		lateralQuery.Having = BinaryExpr{
			Left:  ColumnExpr{Ref: groupInnerCol},
			Op:    "=",
			Right: ColumnExpr{Ref: siblingOuterCol},
		}
	case groupedAggregateLateralModeAggregateValueHaving:
		havingOuterRef, ok := g.pickGroupedAggregateOuterNumericRef(base, sibling, baseJoinCol.Name, siblingJoinCol.Name)
		if !ok {
			return nil
		}
		havingAgg, ok := g.pickGroupedAggregateHavingExpr(sumRef, hasSum)
		if !ok {
			return nil
		}
		lateralQuery.Having = BinaryExpr{
			Left:  havingAgg,
			Op:    ">=",
			Right: ColumnExpr{Ref: havingOuterRef},
		}
	}

	lateralJoin := Join{
		Type:       joinType,
		Lateral:    true,
		Table:      "dt",
		TableAlias: "dt",
		TableQuery: lateralQuery,
	}
	if joinType == JoinInner {
		lateralJoin.On = g.trueExpr()
	}

	items := make([]SelectItem, 0, 5)
	items = append(items,
		SelectItem{
			Expr:  ColumnExpr{Ref: baseOuterCol},
			Alias: baseOuterCol.Table + "_" + baseOuterCol.Name,
		},
		SelectItem{
			Expr:  ColumnExpr{Ref: siblingOuterCol},
			Alias: siblingOuterCol.Table + "_" + siblingOuterCol.Name,
		},
		SelectItem{
			Expr: ColumnExpr{Ref: ColumnRef{
				Table: "dt",
				Name:  "g0",
				Type:  groupExprType,
			}},
			Alias: "lateral_g0",
		},
		SelectItem{
			Expr: ColumnExpr{Ref: ColumnRef{
				Table: "dt",
				Name:  "cnt",
				Type:  schema.TypeBigInt,
			}},
			Alias: "lateral_cnt",
		},
	)
	if hasSum {
		items = append(items, SelectItem{
			Expr: ColumnExpr{Ref: ColumnRef{
				Table: "dt",
				Name:  "sum1",
				Type:  sumRef.Type,
			}},
			Alias: "lateral_sum1",
		})
	}

	query := &SelectQuery{
		Items: items,
		From: FromClause{
			BaseTable: base.Name,
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: sibling.Name,
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: baseJoinCol},
						Op:    "=",
						Right: ColumnExpr{Ref: siblingJoinCol},
					},
				},
				lateralJoin,
			},
		},
	}
	query.OrderBy = g.orderByFromItemsStable(query.Items)
	return query
}

func (g *Generator) buildGroupedOutputAliasLateralHookQuery(tables []schema.Table) *SelectQuery {
	if g == nil || !g.Config.Features.Joins || !g.Config.Features.LateralJoins || !g.Config.Features.Aggregates || !g.Config.Features.GroupBy || g.Config.Features.DSG || len(tables) < 3 {
		return nil
	}
	if !util.Chance(g.Rand, LateralJoinGroupedOutputAliasProb) {
		return nil
	}
	shapeOrder := []bool{false}
	if g.Config.Features.NaturalJoins {
		if util.Chance(g.Rand, 50) {
			shapeOrder = []bool{true, false}
		} else {
			shapeOrder = []bool{false, true}
		}
	}
	for _, natural := range shapeOrder {
		for i := 0; i < len(tables); i++ {
			for j := 0; j < len(tables); j++ {
				if j == i {
					continue
				}
				for k := 0; k < len(tables); k++ {
					if k == i || k == j {
						continue
					}
					if query := g.buildGroupedOutputAliasLateralHookQueryForTables(tables[i], tables[j], tables[k], natural); query != nil {
						return query
					}
				}
			}
		}
	}
	return nil
}

func (g *Generator) buildProjectedOrderLimitLateralHookQuery(tables []schema.Table) *SelectQuery {
	if g == nil || !g.Config.Features.Joins || !g.Config.Features.LateralJoins || !g.Config.Features.OrderBy || !g.Config.Features.Limit || g.Config.Features.DSG || len(tables) < 3 {
		return nil
	}
	if !util.Chance(g.Rand, LateralJoinProjectedOrderLimitProb) {
		return nil
	}
	shapeOrder := []bool{false}
	if g.Config.Features.NaturalJoins {
		if util.Chance(g.Rand, 50) {
			shapeOrder = []bool{true, false}
		} else {
			shapeOrder = []bool{false, true}
		}
	}
	for _, natural := range shapeOrder {
		for i := 0; i < len(tables); i++ {
			for j := 0; j < len(tables); j++ {
				if j == i {
					continue
				}
				for k := 0; k < len(tables); k++ {
					if k == i || k == j {
						continue
					}
					if query := g.buildProjectedOrderLimitLateralHookQueryForTables(tables[i], tables[j], tables[k], natural); query != nil {
						return query
					}
				}
			}
		}
	}
	return nil
}

func (g *Generator) buildMultiOuterProjectedOrderLimitLateralHookQuery(tables []schema.Table) *SelectQuery {
	if g == nil || !g.Config.Features.Joins || !g.Config.Features.LateralJoins || !g.Config.Features.OrderBy || !g.Config.Features.Limit || g.Config.Features.DSG || len(tables) < 3 {
		return nil
	}
	if !util.Chance(g.Rand, LateralJoinMultiOuterOrderLimitProb) {
		return nil
	}
	for i := 0; i < len(tables); i++ {
		for j := 0; j < len(tables); j++ {
			if j == i {
				continue
			}
			for k := 0; k < len(tables); k++ {
				if k == i || k == j {
					continue
				}
				if query := g.buildMultiOuterProjectedOrderLimitLateralHookQueryForTables(tables[i], tables[j], tables[k]); query != nil {
					return query
				}
			}
		}
	}
	return nil
}

func (g *Generator) buildScalarSubqueryProjectedOrderLimitLateralHookQuery(tables []schema.Table) *SelectQuery {
	if g == nil || !g.Config.Features.Joins || !g.Config.Features.LateralJoins || !g.Config.Features.OrderBy || !g.Config.Features.Limit || g.Config.Features.DSG || len(tables) < 3 {
		return nil
	}
	if !g.allowScalarSubquery() || !util.Chance(g.Rand, LateralJoinScalarSubqueryOrderLimitProb) {
		return nil
	}
	for i := 0; i < len(tables); i++ {
		for j := 0; j < len(tables); j++ {
			if j == i {
				continue
			}
			for k := 0; k < len(tables); k++ {
				if k == i || k == j {
					continue
				}
				if query := g.buildScalarSubqueryProjectedOrderLimitLateralHookQueryForTables(tables[i], tables[j], tables[k]); query != nil {
					return query
				}
			}
		}
	}
	return nil
}

func (g *Generator) buildScalarSubqueryProjectedOrderLimitLateralHookQueryForTables(base schema.Table, sibling schema.Table, inner schema.Table) *SelectQuery {
	if g == nil {
		return nil
	}
	joinType, ok := g.pickSupportedLateralJoinType()
	if !ok {
		return nil
	}
	baseJoinCol, siblingJoinCol, ok := g.pickJoinColumnPair([]schema.Table{base}, sibling)
	if !ok {
		return nil
	}
	baseOuterCol, baseInnerCol, ok := g.pickJoinColumnPair([]schema.Table{base}, inner)
	if !ok {
		return nil
	}
	siblingOuterCol, innerScoreCol, ok := g.pickJoinColumnPair([]schema.Table{sibling}, inner)
	if !ok || !g.isNumericType(siblingOuterCol.Type) || !g.isNumericType(innerScoreCol.Type) || !compatibleColumnType(siblingOuterCol.Type, innerScoreCol.Type) {
		return nil
	}
	scalarSourceCol, ok := g.pickCompatibleColumn(inner, innerScoreCol.Type)
	if !ok || !g.isNumericType(scalarSourceCol.Type) {
		return nil
	}
	baseSignalCol, ok := g.pickCompatibleColumn(base, innerScoreCol.Type)
	if !ok || !g.isNumericType(baseSignalCol.Type) {
		return nil
	}
	baseSignalRef := ColumnRef{Table: base.Name, Name: baseSignalCol.Name, Type: baseSignalCol.Type}
	innerScoreRef := ColumnRef{Table: inner.Name, Name: innerScoreCol.Name, Type: innerScoreCol.Type}
	subqueryLimit := 1
	buildScalarSubquery := func() *SelectQuery {
		scalarBaseRef := ColumnRef{Table: "sq", Name: baseInnerCol.Name, Type: baseInnerCol.Type}
		scalarCompareRef := ColumnRef{Table: "sq", Name: innerScoreCol.Name, Type: innerScoreCol.Type}
		scalarSourceRef := ColumnRef{Table: "sq", Name: scalarSourceCol.Name, Type: scalarSourceCol.Type}
		return &SelectQuery{
			Items: []SelectItem{
				{
					Expr:  ColumnExpr{Ref: scalarSourceRef},
					Alias: "sv0",
				},
			},
			From: FromClause{BaseTable: inner.Name, BaseAlias: "sq"},
			Where: BinaryExpr{
				Left: BinaryExpr{
					Left:  ColumnExpr{Ref: scalarBaseRef},
					Op:    "=",
					Right: ColumnExpr{Ref: baseOuterCol},
				},
				Op: "AND",
				Right: BinaryExpr{
					Left:  ColumnExpr{Ref: scalarCompareRef},
					Op:    "<>",
					Right: ColumnExpr{Ref: siblingOuterCol},
				},
			},
			OrderBy: []OrderBy{
				{
					Expr: FuncExpr{
						Name: "ABS",
						Args: []Expr{BinaryExpr{
							Left:  ColumnExpr{Ref: scalarCompareRef},
							Op:    "-",
							Right: ColumnExpr{Ref: siblingOuterCol},
						}},
					},
				},
				{Expr: ColumnExpr{Ref: scalarSourceRef}, Desc: true},
				{Expr: ColumnExpr{Ref: baseSignalRef}},
			},
			Limit: &subqueryLimit,
		}
	}
	scoreExpr := FuncExpr{
		Name: "ABS",
		Args: []Expr{BinaryExpr{
			Left:  ColumnExpr{Ref: innerScoreRef},
			Op:    "-",
			Right: SubqueryExpr{Query: buildScalarSubquery()},
		}},
	}
	tieExpr := SubqueryExpr{Query: buildScalarSubquery()}
	scoreAliasRef := ColumnRef{Name: "score0", Type: innerScoreCol.Type}
	tieAliasRef := ColumnRef{Name: "tie0", Type: scalarSourceCol.Type}
	limit := 1
	lateralQuery := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  scoreExpr,
				Alias: "score0",
			},
			{
				Expr:  tieExpr,
				Alias: "tie0",
			},
		},
		From: FromClause{BaseTable: inner.Name},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: baseInnerCol},
			Op:    "=",
			Right: ColumnExpr{Ref: baseOuterCol},
		},
		OrderBy: []OrderBy{
			{Expr: ColumnExpr{Ref: scoreAliasRef}},
			{Expr: ColumnExpr{Ref: tieAliasRef}, Desc: true},
			{Expr: ColumnExpr{Ref: baseSignalRef}},
		},
		Limit: &limit,
	}
	lateralJoin := Join{
		Type:       joinType,
		Lateral:    true,
		Table:      "dt",
		TableAlias: "dt",
		TableQuery: lateralQuery,
	}
	if joinType == JoinInner {
		lateralJoin.On = g.trueExpr()
	}
	query := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: baseOuterCol},
				Alias: baseOuterCol.Table + "_" + baseOuterCol.Name,
			},
			{
				Expr:  ColumnExpr{Ref: siblingOuterCol},
				Alias: siblingOuterCol.Table + "_" + siblingOuterCol.Name,
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  "score0",
					Type:  scoreAliasRef.Type,
				}},
				Alias: "lateral_score0",
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  "tie0",
					Type:  tieAliasRef.Type,
				}},
				Alias: "lateral_tie0",
			},
		},
		From: FromClause{
			BaseTable: base.Name,
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: sibling.Name,
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: baseJoinCol},
						Op:    "=",
						Right: ColumnExpr{Ref: siblingJoinCol},
					},
				},
				lateralJoin,
			},
		},
	}
	query.Where = BinaryExpr{
		Left: ColumnExpr{Ref: ColumnRef{
			Table: "dt",
			Name:  "tie0",
			Type:  tieAliasRef.Type,
		}},
		Op:    ">=",
		Right: ColumnExpr{Ref: siblingOuterCol},
	}
	query.OrderBy = []OrderBy{
		{
			Expr: FuncExpr{
				Name: "ABS",
				Args: []Expr{BinaryExpr{
					Left: ColumnExpr{Ref: ColumnRef{
						Table: "dt",
						Name:  "tie0",
						Type:  tieAliasRef.Type,
					}},
					Op:    "-",
					Right: ColumnExpr{Ref: siblingOuterCol},
				}},
			},
		},
		{
			Expr: ColumnExpr{Ref: ColumnRef{
				Table: "dt",
				Name:  "score0",
				Type:  scoreAliasRef.Type,
			}},
		},
		{Expr: ColumnExpr{Ref: baseOuterCol}},
	}
	return query
}

func (g *Generator) buildMultiOuterProjectedOrderLimitLateralHookQueryForTables(base schema.Table, sibling schema.Table, inner schema.Table) *SelectQuery {
	if g == nil {
		return nil
	}
	joinType, ok := g.pickSupportedLateralJoinType()
	if !ok {
		return nil
	}
	baseJoinCol, siblingJoinCol, ok := g.pickJoinColumnPair([]schema.Table{base}, sibling)
	if !ok {
		return nil
	}
	baseOuterCol, baseInnerCol, ok := g.pickJoinColumnPair([]schema.Table{base}, inner)
	if !ok {
		return nil
	}
	siblingOuterCol, scoreInnerCol, ok := g.pickJoinColumnPair([]schema.Table{sibling}, inner)
	if !ok || !g.isNumericType(siblingOuterCol.Type) || !g.isNumericType(scoreInnerCol.Type) || !compatibleColumnType(siblingOuterCol.Type, scoreInnerCol.Type) {
		return nil
	}
	baseSignalCol, ok := g.pickCompatibleColumn(base, siblingOuterCol.Type)
	if !ok || !g.isNumericType(baseSignalCol.Type) {
		return nil
	}
	baseSignalRef := ColumnRef{Table: base.Name, Name: baseSignalCol.Name, Type: baseSignalCol.Type}
	tieInnerCol, ok := g.pickCompatibleColumn(inner, baseSignalRef.Type)
	if !ok || !g.isNumericType(tieInnerCol.Type) {
		return nil
	}
	tieInnerRef := ColumnRef{Table: inner.Name, Name: tieInnerCol.Name, Type: tieInnerCol.Type}
	positiveTieExpr := BinaryExpr{
		Left:  ColumnExpr{Ref: tieInnerRef},
		Op:    ">=",
		Right: ColumnExpr{Ref: baseSignalRef},
	}
	scoreExpr := FuncExpr{
		Name: "ABS",
		Args: []Expr{CaseExpr{
			Whens: []CaseWhen{
				{
					When: BinaryExpr{
						Left:  ColumnExpr{Ref: scoreInnerCol},
						Op:    ">=",
						Right: ColumnExpr{Ref: siblingOuterCol},
					},
					Then: CaseExpr{
						Whens: []CaseWhen{
							{
								When: positiveTieExpr,
								Then: BinaryExpr{
									Left:  ColumnExpr{Ref: scoreInnerCol},
									Op:    "-",
									Right: ColumnExpr{Ref: siblingOuterCol},
								},
							},
						},
						Else: ColumnExpr{Ref: baseSignalRef},
					},
				},
			},
			Else: CaseExpr{
				Whens: []CaseWhen{
					{
						When: positiveTieExpr,
						Then: BinaryExpr{
							Left:  ColumnExpr{Ref: siblingOuterCol},
							Op:    "-",
							Right: ColumnExpr{Ref: scoreInnerCol},
						},
					},
				},
				Else: ColumnExpr{Ref: tieInnerRef},
			},
		}},
	}
	tieExpr := FuncExpr{
		Name: "ABS",
		Args: []Expr{CaseExpr{
			Whens: []CaseWhen{
				{
					When: positiveTieExpr,
					Then: BinaryExpr{
						Left:  ColumnExpr{Ref: tieInnerRef},
						Op:    "+",
						Right: ColumnExpr{Ref: baseSignalRef},
					},
				},
			},
			Else: BinaryExpr{
				Left:  ColumnExpr{Ref: siblingOuterCol},
				Op:    "-",
				Right: ColumnExpr{Ref: tieInnerRef},
			},
		}},
	}
	scoreAliasRef := ColumnRef{Name: "score0", Type: siblingOuterCol.Type}
	tieAliasRef := ColumnRef{Name: "tie0", Type: baseSignalRef.Type}
	limit := 1
	lateralQuery := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  scoreExpr,
				Alias: "score0",
			},
			{
				Expr:  tieExpr,
				Alias: "tie0",
			},
		},
		From: FromClause{BaseTable: inner.Name},
		Where: BinaryExpr{
			Left: BinaryExpr{
				Left:  ColumnExpr{Ref: baseInnerCol},
				Op:    "=",
				Right: ColumnExpr{Ref: baseOuterCol},
			},
			Op: "AND",
			Right: BinaryExpr{
				Left:  ColumnExpr{Ref: scoreInnerCol},
				Op:    "<>",
				Right: ColumnExpr{Ref: siblingOuterCol},
			},
		},
		OrderBy: []OrderBy{
			{Expr: ColumnExpr{Ref: scoreAliasRef}},
			{Expr: ColumnExpr{Ref: tieAliasRef}, Desc: true},
			{Expr: ColumnExpr{Ref: siblingOuterCol}},
		},
		Limit: &limit,
	}
	lateralJoin := Join{
		Type:       joinType,
		Lateral:    true,
		Table:      "dt",
		TableAlias: "dt",
		TableQuery: lateralQuery,
	}
	if joinType == JoinInner {
		lateralJoin.On = g.trueExpr()
	}
	query := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: baseOuterCol},
				Alias: baseOuterCol.Table + "_" + baseOuterCol.Name,
			},
			{
				Expr:  ColumnExpr{Ref: siblingOuterCol},
				Alias: siblingOuterCol.Table + "_" + siblingOuterCol.Name,
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  "score0",
					Type:  scoreAliasRef.Type,
				}},
				Alias: "lateral_score0",
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  "tie0",
					Type:  tieAliasRef.Type,
				}},
				Alias: "lateral_tie0",
			},
		},
		From: FromClause{
			BaseTable: base.Name,
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: sibling.Name,
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: baseJoinCol},
						Op:    "=",
						Right: ColumnExpr{Ref: siblingJoinCol},
					},
				},
				lateralJoin,
			},
		},
	}
	query.OrderBy = g.orderByFromItemsStable(query.Items)
	return query
}

func (g *Generator) buildProjectedOrderLimitLateralHookQueryForTables(base schema.Table, sibling schema.Table, inner schema.Table, natural bool) *SelectQuery {
	if g == nil {
		return nil
	}
	joinType, ok := g.pickSupportedLateralJoinType()
	if !ok {
		return nil
	}
	candidates := g.collectMergedColumnCandidates([]schema.Table{base}, sibling, natural)
	if len(candidates) == 0 {
		return nil
	}
	type hookedPair struct {
		merged mergedColumnCandidate
		inner  schema.Column
	}
	pairs := make([]hookedPair, 0, len(candidates))
	for _, candidate := range candidates {
		if !g.isNumericType(candidate.Column.Type) {
			continue
		}
		innerCol, ok := g.pickCompatibleColumn(inner, candidate.Column.Type)
		if !ok || !g.isNumericType(innerCol.Type) {
			continue
		}
		pairs = append(pairs, hookedPair{merged: candidate, inner: innerCol})
	}
	if len(pairs) == 0 {
		return nil
	}
	pick := pairs[g.Rand.Intn(len(pairs))]
	mergedRef := ColumnRef{Name: pick.merged.Column.Name, Type: pick.merged.Column.Type}
	innerRef := ColumnRef{Table: inner.Name, Name: pick.inner.Name, Type: pick.inner.Type}
	positiveInnerExpr := BinaryExpr{
		Left:  ColumnExpr{Ref: innerRef},
		Op:    ">=",
		Right: LiteralExpr{Value: 0},
	}
	scoreExpr := FuncExpr{
		Name: "ABS",
		Args: []Expr{CaseExpr{
			Whens: []CaseWhen{
				{
					When: BinaryExpr{
						Left:  ColumnExpr{Ref: innerRef},
						Op:    ">=",
						Right: ColumnExpr{Ref: mergedRef},
					},
					Then: CaseExpr{
						Whens: []CaseWhen{
							{
								When: positiveInnerExpr,
								Then: BinaryExpr{
									Left:  ColumnExpr{Ref: innerRef},
									Op:    "-",
									Right: ColumnExpr{Ref: mergedRef},
								},
							},
						},
						Else: ColumnExpr{Ref: mergedRef},
					},
				},
			},
			Else: CaseExpr{
				Whens: []CaseWhen{
					{
						When: positiveInnerExpr,
						Then: BinaryExpr{
							Left:  ColumnExpr{Ref: mergedRef},
							Op:    "-",
							Right: ColumnExpr{Ref: innerRef},
						},
					},
				},
				Else: ColumnExpr{Ref: innerRef},
			},
		}},
	}
	tieExpr := FuncExpr{
		Name: "ABS",
		Args: []Expr{CaseExpr{
			Whens: []CaseWhen{
				{
					When: positiveInnerExpr,
					Then: BinaryExpr{
						Left:  ColumnExpr{Ref: innerRef},
						Op:    "+",
						Right: ColumnExpr{Ref: mergedRef},
					},
				},
			},
			Else: BinaryExpr{
				Left:  ColumnExpr{Ref: mergedRef},
				Op:    "-",
				Right: ColumnExpr{Ref: innerRef},
			},
		}},
	}
	scoreAliasRef := ColumnRef{Name: "score0", Type: pick.inner.Type}
	tieAliasRef := ColumnRef{Name: "tie0", Type: pick.inner.Type}
	limit := 1
	lateralQuery := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  scoreExpr,
				Alias: "score0",
			},
			{
				Expr:  tieExpr,
				Alias: "tie0",
			},
		},
		From: FromClause{BaseTable: inner.Name},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: innerRef},
			Op:    "<>",
			Right: ColumnExpr{Ref: mergedRef},
		},
		OrderBy: []OrderBy{
			{Expr: ColumnExpr{Ref: scoreAliasRef}},
			{Expr: ColumnExpr{Ref: tieAliasRef}, Desc: true},
			{Expr: ColumnExpr{Ref: mergedRef}},
		},
		Limit: &limit,
	}
	joins := make([]Join, 0, 2)
	joins = append(joins, Join{
		Type:  JoinInner,
		Table: sibling.Name,
	})
	if pick.merged.Natural {
		joins[0].Natural = true
	} else {
		joins[0].Using = []string{pick.merged.Column.Name}
	}
	lateralJoin := Join{
		Type:       joinType,
		Lateral:    true,
		Table:      "dt",
		TableAlias: "dt",
		TableQuery: lateralQuery,
	}
	if joinType == JoinInner {
		lateralJoin.On = g.trueExpr()
	}
	joins = append(joins, lateralJoin)
	query := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: mergedRef},
				Alias: "merged_" + pick.merged.Column.Name,
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  "score0",
					Type:  pick.inner.Type,
				}},
				Alias: "lateral_score0",
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  "tie0",
					Type:  pick.inner.Type,
				}},
				Alias: "lateral_tie0",
			},
		},
		From: FromClause{
			BaseTable: base.Name,
			Joins:     joins,
		},
	}
	query.OrderBy = g.orderByFromItemsStable(query.Items)
	return query
}

func (g *Generator) buildGroupedOutputOrderLimitLateralHookQuery(tables []schema.Table) *SelectQuery {
	if g == nil || !g.Config.Features.Joins || !g.Config.Features.LateralJoins || !g.Config.Features.Aggregates || !g.Config.Features.GroupBy || !g.Config.Features.OrderBy || !g.Config.Features.Limit || g.Config.Features.DSG || len(tables) < 3 {
		return nil
	}
	if !util.Chance(g.Rand, LateralJoinGroupedOutputOrderLimitProb) {
		return nil
	}
	shapeOrder := []bool{false}
	if g.Config.Features.NaturalJoins {
		if util.Chance(g.Rand, 50) {
			shapeOrder = []bool{true, false}
		} else {
			shapeOrder = []bool{false, true}
		}
	}
	for _, natural := range shapeOrder {
		for i := 0; i < len(tables); i++ {
			for j := 0; j < len(tables); j++ {
				if j == i {
					continue
				}
				for k := 0; k < len(tables); k++ {
					if k == i || k == j {
						continue
					}
					if query := g.buildGroupedOutputOrderLimitLateralHookQueryForTables(tables[i], tables[j], tables[k], natural); query != nil {
						return query
					}
				}
			}
		}
	}
	return nil
}

func (g *Generator) buildGroupedOutputOrderLimitLateralHookQueryForTables(base schema.Table, sibling schema.Table, inner schema.Table, natural bool) *SelectQuery {
	if g == nil {
		return nil
	}
	joinType, ok := g.pickSupportedLateralJoinType()
	if !ok {
		return nil
	}
	candidates := g.collectMergedColumnCandidates([]schema.Table{base}, sibling, natural)
	if len(candidates) == 0 {
		return nil
	}
	type hookedPair struct {
		merged mergedColumnCandidate
		inner  schema.Column
	}
	pairs := make([]hookedPair, 0, len(candidates))
	for _, candidate := range candidates {
		if !g.isNumericType(candidate.Column.Type) {
			continue
		}
		innerCol, ok := g.pickCompatibleColumn(inner, candidate.Column.Type)
		if !ok || !g.isNumericType(innerCol.Type) {
			continue
		}
		pairs = append(pairs, hookedPair{merged: candidate, inner: innerCol})
	}
	if len(pairs) == 0 {
		return nil
	}
	pick := pairs[g.Rand.Intn(len(pairs))]
	mergedRef := ColumnRef{Name: pick.merged.Column.Name, Type: pick.merged.Column.Type}
	innerRef := ColumnRef{Table: inner.Name, Name: pick.inner.Name, Type: pick.inner.Type}
	groupAliasRef := ColumnRef{Name: "g0", Type: pick.inner.Type}
	countAliasRef := ColumnRef{Name: "cnt", Type: schema.TypeBigInt}
	countExpr := FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}
	havingExpr := BinaryExpr{
		Left: FuncExpr{
			Name: "ABS",
			Args: []Expr{CaseExpr{
				Whens: []CaseWhen{
					{
						When: BinaryExpr{
							Left:  ColumnExpr{Ref: innerRef},
							Op:    ">=",
							Right: ColumnExpr{Ref: mergedRef},
						},
						Then: BinaryExpr{
							Left:  countExpr,
							Op:    "-",
							Right: ColumnExpr{Ref: mergedRef},
						},
					},
				},
				Else: BinaryExpr{
					Left: BinaryExpr{
						Left:  countExpr,
						Op:    "+",
						Right: ColumnExpr{Ref: innerRef},
					},
					Op:    "-",
					Right: ColumnExpr{Ref: mergedRef},
				},
			}},
		},
		Op:    ">=",
		Right: LiteralExpr{Value: 1},
	}
	groupExpr := ColumnExpr{Ref: innerRef}
	limit := 1
	lateralQuery := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  groupExpr,
				Alias: "g0",
			},
			{
				Expr:  countExpr,
				Alias: "cnt",
			},
		},
		From: FromClause{BaseTable: inner.Name},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: innerRef},
			Op:    "<>",
			Right: ColumnExpr{Ref: mergedRef},
		},
		GroupBy: []Expr{groupExpr},
		Having:  havingExpr,
		OrderBy: []OrderBy{
			{Expr: ColumnExpr{Ref: groupAliasRef}},
			{Expr: ColumnExpr{Ref: countAliasRef}, Desc: true},
			{Expr: ColumnExpr{Ref: mergedRef}},
		},
		Limit: &limit,
	}
	joins := make([]Join, 0, 2)
	joins = append(joins, Join{
		Type:  JoinInner,
		Table: sibling.Name,
	})
	if pick.merged.Natural {
		joins[0].Natural = true
	} else {
		joins[0].Using = []string{pick.merged.Column.Name}
	}
	lateralJoin := Join{
		Type:       joinType,
		Lateral:    true,
		Table:      "dt",
		TableAlias: "dt",
		TableQuery: lateralQuery,
	}
	if joinType == JoinInner {
		lateralJoin.On = g.trueExpr()
	}
	joins = append(joins, lateralJoin)
	query := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: mergedRef},
				Alias: "merged_" + pick.merged.Column.Name,
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  "g0",
					Type:  pick.inner.Type,
				}},
				Alias: "lateral_g0",
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  "cnt",
					Type:  schema.TypeBigInt,
				}},
				Alias: "lateral_cnt",
			},
		},
		From: FromClause{
			BaseTable: base.Name,
			Joins:     joins,
		},
	}
	query.OrderBy = g.orderByFromItemsStable(query.Items)
	return query
}

func (g *Generator) buildGroupedOutputAliasLateralHookQueryForTables(base schema.Table, sibling schema.Table, inner schema.Table, natural bool) *SelectQuery {
	if g == nil {
		return nil
	}
	joinType, ok := g.pickSupportedLateralJoinType()
	if !ok {
		return nil
	}
	candidates := g.collectMergedColumnCandidates([]schema.Table{base}, sibling, natural)
	if len(candidates) == 0 {
		return nil
	}
	type hookedPair struct {
		merged mergedColumnCandidate
		inner  schema.Column
	}
	pairs := make([]hookedPair, 0, len(candidates))
	for _, candidate := range candidates {
		innerCol, ok := g.pickCompatibleColumn(inner, candidate.Column.Type)
		if !ok {
			continue
		}
		pairs = append(pairs, hookedPair{merged: candidate, inner: innerCol})
	}
	if len(pairs) == 0 {
		return nil
	}
	pick := pairs[g.Rand.Intn(len(pairs))]
	mergedRef := ColumnRef{Name: pick.merged.Column.Name, Type: pick.merged.Column.Type}
	innerRef := ColumnRef{Table: inner.Name, Name: pick.inner.Name, Type: pick.inner.Type}
	aggQuery := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: innerRef},
				Alias: "g0",
			},
			{
				Expr:  FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}},
				Alias: "cnt",
			},
		},
		From:    FromClause{BaseTable: inner.Name},
		GroupBy: []Expr{ColumnExpr{Ref: innerRef}},
	}
	groupAliasRef := ColumnRef{Table: "agg", Name: "g0", Type: pick.inner.Type}
	countAliasRef := ColumnRef{Table: "agg", Name: "cnt", Type: schema.TypeBigInt}
	lateralQuery := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: groupAliasRef},
				Alias: "g0",
			},
			{
				Expr:  ColumnExpr{Ref: countAliasRef},
				Alias: "cnt",
			},
		},
		From: FromClause{
			BaseTable: "agg",
			BaseAlias: "agg",
			BaseQuery: aggQuery,
		},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: groupAliasRef},
			Op:    "=",
			Right: ColumnExpr{Ref: mergedRef},
		},
		OrderBy: []OrderBy{{Expr: ColumnExpr{Ref: groupAliasRef}}},
	}
	joins := make([]Join, 0, 2)
	joins = append(joins, Join{
		Type:  JoinInner,
		Table: sibling.Name,
	})
	if pick.merged.Natural {
		joins[0].Natural = true
	} else {
		joins[0].Using = []string{pick.merged.Column.Name}
	}
	lateralJoin := Join{
		Type:       joinType,
		Lateral:    true,
		Table:      "dt",
		TableAlias: "dt",
		TableQuery: lateralQuery,
	}
	if joinType == JoinInner {
		lateralJoin.On = g.trueExpr()
	}
	joins = append(joins, lateralJoin)
	query := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: mergedRef},
				Alias: "merged_" + pick.merged.Column.Name,
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  "g0",
					Type:  pick.inner.Type,
				}},
				Alias: "lateral_g0",
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  "cnt",
					Type:  schema.TypeBigInt,
				}},
				Alias: "lateral_cnt",
			},
		},
		From: FromClause{
			BaseTable: base.Name,
			Joins:     joins,
		},
	}
	query.OrderBy = g.orderByFromItemsStable(query.Items)
	return query
}

func (g *Generator) buildCorrelatedAggregateLateralHookQuery(tables []schema.Table) *SelectQuery {
	if g == nil || !g.Config.Features.Joins || !g.Config.Features.LateralJoins || !g.Config.Features.Aggregates || g.Config.Features.DSG || len(tables) < 3 {
		return nil
	}
	if !util.Chance(g.Rand, LateralJoinAggregateProb) {
		return nil
	}
	for i := 0; i < len(tables); i++ {
		for j := 0; j < len(tables); j++ {
			if j == i {
				continue
			}
			for k := 0; k < len(tables); k++ {
				if k == i || k == j {
					continue
				}
				if query := g.buildCorrelatedAggregateLateralHookQueryForTables(tables[i], tables[j], tables[k]); query != nil {
					return query
				}
			}
		}
	}
	return nil
}

func (g *Generator) buildCorrelatedAggregateLateralHookQueryForTables(base schema.Table, sibling schema.Table, inner schema.Table) *SelectQuery {
	if g == nil {
		return nil
	}
	joinType, ok := g.pickSupportedLateralJoinType()
	if !ok {
		return nil
	}
	baseJoinCol, siblingJoinCol, ok := g.pickJoinColumnPair([]schema.Table{base}, sibling)
	if !ok {
		return nil
	}
	baseOuterCol, baseInnerCol, ok := g.pickJoinColumnPair([]schema.Table{base}, inner)
	if !ok {
		return nil
	}
	siblingOuterCol, siblingInnerCol, ok := g.pickJoinColumnPair([]schema.Table{sibling}, inner)
	if !ok {
		return nil
	}

	lateralItems := make([]SelectItem, 0, 2)
	lateralItems = append(lateralItems, SelectItem{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"})
	sumRef, hasSum := g.pickAggregateValueColumnRef(inner, baseInnerCol.Name, siblingInnerCol.Name)
	if hasSum {
		sumExpr := ColumnExpr{Ref: sumRef}
		g.warnAggOnDouble("SUM", sumExpr)
		lateralItems = append(lateralItems, SelectItem{
			Expr:  FuncExpr{Name: "SUM", Args: []Expr{sumExpr}},
			Alias: "sum1",
		})
	}

	lateralQuery := &SelectQuery{
		Items: lateralItems,
		From:  FromClause{BaseTable: inner.Name},
		Where: BinaryExpr{
			Left: BinaryExpr{
				Left:  ColumnExpr{Ref: baseInnerCol},
				Op:    "=",
				Right: ColumnExpr{Ref: baseOuterCol},
			},
			Op: "AND",
			Right: BinaryExpr{
				Left:  ColumnExpr{Ref: siblingInnerCol},
				Op:    "=",
				Right: ColumnExpr{Ref: siblingOuterCol},
			},
		},
	}

	lateralJoin := Join{
		Type:       joinType,
		Lateral:    true,
		Table:      "dt",
		TableAlias: "dt",
		TableQuery: lateralQuery,
	}
	if joinType == JoinInner {
		lateralJoin.On = g.trueExpr()
	}

	items := make([]SelectItem, 0, 4)
	items = append(items,
		SelectItem{
			Expr:  ColumnExpr{Ref: baseOuterCol},
			Alias: baseOuterCol.Table + "_" + baseOuterCol.Name,
		},
		SelectItem{
			Expr:  ColumnExpr{Ref: siblingOuterCol},
			Alias: siblingOuterCol.Table + "_" + siblingOuterCol.Name,
		},
		SelectItem{
			Expr: ColumnExpr{Ref: ColumnRef{
				Table: "dt",
				Name:  "cnt",
				Type:  schema.TypeBigInt,
			}},
			Alias: "lateral_cnt",
		},
	)
	if hasSum {
		items = append(items, SelectItem{
			Expr: ColumnExpr{Ref: ColumnRef{
				Table: "dt",
				Name:  "sum1",
				Type:  sumRef.Type,
			}},
			Alias: "lateral_sum1",
		})
	}

	query := &SelectQuery{
		Items: items,
		From: FromClause{
			BaseTable: base.Name,
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: sibling.Name,
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: baseJoinCol},
						Op:    "=",
						Right: ColumnExpr{Ref: siblingJoinCol},
					},
				},
				lateralJoin,
			},
		},
	}
	query.OrderBy = g.orderByFromItemsStable(query.Items)
	return query
}

func (g *Generator) pickAggregateValueColumnRef(tbl schema.Table, excludeNames ...string) (ColumnRef, bool) {
	if g == nil || len(tbl.Columns) == 0 {
		return ColumnRef{}, false
	}
	exclude := make(map[string]struct{}, len(excludeNames))
	for _, name := range excludeNames {
		if name == "" {
			continue
		}
		exclude[name] = struct{}{}
	}
	preferred := make([]schema.Column, 0, len(tbl.Columns))
	fallback := make([]schema.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		if _, skip := exclude[col.Name]; skip {
			continue
		}
		if !g.isNumericType(col.Type) {
			continue
		}
		fallback = append(fallback, col)
		if col.Type != schema.TypeDouble {
			preferred = append(preferred, col)
		}
	}
	if len(preferred) > 0 {
		col := preferred[g.Rand.Intn(len(preferred))]
		return ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}, true
	}
	if len(fallback) > 0 {
		col := fallback[g.Rand.Intn(len(fallback))]
		return ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}, true
	}
	return ColumnRef{}, false
}

func (g *Generator) pickGroupedAggregateOuterNumericRef(base schema.Table, sibling schema.Table, excludeNames ...string) (ColumnRef, bool) {
	if ref, ok := g.pickAggregateValueColumnRef(sibling, excludeNames...); ok {
		return ref, true
	}
	return g.pickAggregateValueColumnRef(base, excludeNames...)
}

func (g *Generator) pickGroupedAggregateFilterInnerRef(tbl schema.Table, sumRef ColumnRef, hasSum bool, excludeNames ...string) (ColumnRef, bool) {
	if hasSum {
		return sumRef, true
	}
	return g.pickAggregateValueColumnRef(tbl, excludeNames...)
}

func (g *Generator) pickGroupedAggregateHavingExpr(sumRef ColumnRef, hasSum bool) (Expr, bool) {
	if hasSum && util.Chance(g.Rand, 50) {
		sumExpr := ColumnExpr{Ref: sumRef}
		g.warnAggOnDouble("SUM", sumExpr)
		return FuncExpr{Name: "SUM", Args: []Expr{sumExpr}}, true
	}
	return FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, true
}

func (g *Generator) buildMergedColumnVisibilityLateralHookQuery(tables []schema.Table) *SelectQuery {
	if g == nil || !g.Config.Features.Joins || !g.Config.Features.LateralJoins || g.Config.Features.DSG || len(tables) < 3 {
		return nil
	}
	if !util.Chance(g.Rand, LateralJoinMergedVisibilityProb) {
		return nil
	}
	shapeOrder := []bool{false}
	if g.Config.Features.NaturalJoins {
		if util.Chance(g.Rand, 50) {
			shapeOrder = []bool{true, false}
		} else {
			shapeOrder = []bool{false, true}
		}
	}
	for _, natural := range shapeOrder {
		for i := 0; i < len(tables); i++ {
			for j := 0; j < len(tables); j++ {
				if j == i {
					continue
				}
				for k := 0; k < len(tables); k++ {
					if k == i || k == j {
						continue
					}
					if query := g.buildMergedColumnVisibilityLateralHookQueryForTables(tables[i], tables[j], tables[k], natural); query != nil {
						return query
					}
				}
			}
		}
	}
	return nil
}

func (g *Generator) buildMergedColumnVisibilityLateralHookQueryForTables(base schema.Table, sibling schema.Table, inner schema.Table, natural bool) *SelectQuery {
	if g == nil {
		return nil
	}
	joinType, ok := g.pickSupportedLateralJoinType()
	if !ok {
		return nil
	}
	candidates := g.collectMergedColumnCandidates([]schema.Table{base}, sibling, natural)
	if len(candidates) == 0 {
		return nil
	}
	type hookedPair struct {
		merged mergedColumnCandidate
		inner  schema.Column
	}
	pairs := make([]hookedPair, 0, len(candidates))
	for _, candidate := range candidates {
		innerCol, ok := g.pickCompatibleColumn(inner, candidate.Column.Type)
		if !ok {
			continue
		}
		pairs = append(pairs, hookedPair{merged: candidate, inner: innerCol})
	}
	if len(pairs) == 0 {
		return nil
	}
	pick := pairs[g.Rand.Intn(len(pairs))]
	innerRef := ColumnRef{Table: inner.Name, Name: pick.inner.Name, Type: pick.inner.Type}
	mergedRef := ColumnRef{Name: pick.merged.Column.Name, Type: pick.merged.Column.Type}
	lateralQuery := &SelectQuery{
		Items: []SelectItem{{
			Expr:  ColumnExpr{Ref: innerRef},
			Alias: pick.merged.Column.Name,
		}},
		From: FromClause{BaseTable: inner.Name},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: innerRef},
			Op:    "=",
			Right: ColumnExpr{Ref: mergedRef},
		},
	}
	joins := make([]Join, 0, 2)
	joins = append(joins, Join{
		Type:  JoinInner,
		Table: sibling.Name,
	})
	if pick.merged.Natural {
		joins[0].Natural = true
	} else {
		joins[0].Using = []string{pick.merged.Column.Name}
	}
	lateralJoin := Join{
		Type:       joinType,
		Lateral:    true,
		Table:      "dt",
		TableAlias: "dt",
		TableQuery: lateralQuery,
	}
	if joinType == JoinInner {
		lateralJoin.On = g.trueExpr()
	}
	joins = append(joins, lateralJoin)
	query := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: mergedRef},
				Alias: "merged_" + pick.merged.Column.Name,
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{
					Table: "dt",
					Name:  pick.merged.Column.Name,
					Type:  pick.inner.Type,
				}},
				Alias: "lateral_" + pick.merged.Column.Name,
			},
		},
		From: FromClause{
			BaseTable: base.Name,
			Joins:     joins,
		},
	}
	query.OrderBy = g.orderByFromItemsStable(query.Items)
	return query
}

func (g *Generator) collectMergedColumnCandidates(left []schema.Table, right schema.Table, natural bool) []mergedColumnCandidate {
	if natural {
		if !g.Config.Features.NaturalJoins || !g.naturalJoinAllowed(left, right) {
			return nil
		}
		cols := g.collectNaturalMergedColumns(left, right)
		out := make([]mergedColumnCandidate, 0, len(cols))
		for _, col := range cols {
			out = append(out, mergedColumnCandidate{
				Natural: true,
				Column:  col,
			})
		}
		return out
	}
	names := g.collectUsingColumnNamesWithMode(left, right, false)
	if len(names) == 0 {
		return nil
	}
	leftCols := uniqueColumnsByName(g.collectColumns(left))
	out := make([]mergedColumnCandidate, 0, len(names))
	for _, name := range names {
		col, ok := leftCols[name]
		if !ok {
			continue
		}
		out = append(out, mergedColumnCandidate{
			Column: ColumnRef{Name: name, Type: col.Type},
		})
	}
	return out
}

func (g *Generator) collectNaturalMergedColumns(left []schema.Table, right schema.Table) []ColumnRef {
	leftCols := uniqueColumnsByName(g.collectColumns(left))
	if len(leftCols) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]ColumnRef, 0, len(right.Columns))
	for _, col := range right.Columns {
		leftCol, ok := leftCols[col.Name]
		if !ok || !compatibleColumnType(leftCol.Type, col.Type) {
			continue
		}
		if _, dup := seen[col.Name]; dup {
			continue
		}
		seen[col.Name] = struct{}{}
		out = append(out, ColumnRef{Name: col.Name, Type: leftCol.Type})
	}
	return out
}

func uniqueColumnsByName(cols []ColumnRef) map[string]ColumnRef {
	if len(cols) == 0 {
		return nil
	}
	counts := make(map[string]int, len(cols))
	first := make(map[string]ColumnRef, len(cols))
	for _, col := range cols {
		if col.Name == "" {
			continue
		}
		counts[col.Name]++
		if _, ok := first[col.Name]; !ok {
			first[col.Name] = col
		}
	}
	out := make(map[string]ColumnRef, len(first))
	for name, count := range counts {
		if count == 1 {
			out[name] = first[name]
		}
	}
	return out
}

func (g *Generator) buildLateralJoin(outerTables []schema.Table, inner schema.Table) (Join, bool) {
	if g == nil || !g.Config.Features.LateralJoins || g.Config.Features.DSG || len(outerTables) == 0 {
		return Join{}, false
	}
	if !util.Chance(g.Rand, LateralJoinProb) {
		return Join{}, false
	}
	joinType, ok := g.pickSupportedLateralJoinType()
	if !ok {
		return Join{}, false
	}
	query := g.buildCorrelatedOrderLimitLateralQuery(outerTables, inner)
	if query == nil {
		query = g.buildLateralDerivedTableQuery(outerTables, inner)
	}
	if query == nil {
		return Join{}, false
	}
	join := Join{
		Type:       joinType,
		Lateral:    true,
		Table:      inner.Name,
		TableQuery: query,
		TableAlias: inner.Name,
	}
	if joinType == JoinInner {
		join.On = g.trueExpr()
	}
	return join, true
}

func (g *Generator) buildFromClause(tables []schema.Table, derived map[string]*SelectQuery) FromClause {
	if len(tables) == 0 {
		return FromClause{}
	}
	from := FromClause{BaseTable: tables[0].Name}
	if subq, ok := derived[from.BaseTable]; ok && subq != nil {
		from.BaseQuery = subq
		from.BaseAlias = from.BaseTable
	}
	if len(tables) == 1 || !g.Config.Features.Joins {
		return from
	}
	for i := 1; i < len(tables); i++ {
		if lateralJoin, ok := g.buildLateralJoin(tables[:i], tables[i]); ok {
			from.Joins = append(from.Joins, lateralJoin)
			continue
		}
		joinType := JoinInner
		if g.joinTypeOverride != nil {
			joinType = *g.joinTypeOverride
		} else if rareChance(g.Rand, crossJoinRareDenom) {
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
		if subq, ok := derived[join.Table]; ok && subq != nil {
			join.TableQuery = subq
			join.TableAlias = join.Table
		}
		if joinType != JoinCross {
			using := g.pickUsingColumns(tables[:i], tables[i])
			naturalOK := g.naturalJoinAllowed(tables[:i], tables[i])
			if g.Config.Features.NaturalJoins && naturalOK && len(using) > 0 && util.Chance(g.Rand, NaturalJoinProb) {
				join.Natural = true
				join.Using = using
			} else if len(using) > 0 && util.Chance(g.Rand, g.joinUsingProb()) {
				join.Using = using
			} else {
				join.On = g.joinCondition(tables[:i], tables[i])
			}
		}
		from.Joins = append(from.Joins, join)
	}
	if g.Config.TQS.Enabled && g.TQSWalker != nil {
		if g.Config.Features.DSG {
			for _, join := range from.Joins {
				if join.Table == "" {
					continue
				}
				g.TQSWalker.RecordPath([]string{from.BaseTable, join.Table})
			}
		} else {
			path := make([]string, 0, 1+len(from.Joins))
			path = append(path, from.BaseTable)
			for _, join := range from.Joins {
				path = append(path, join.Table)
			}
			g.TQSWalker.RecordPath(path)
		}
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
	names := g.collectUsingColumnNamesWithMode(left, right, util.Chance(g.Rand, g.indexPrefixProb()))
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

func (g *Generator) collectUsingColumnNamesWithMode(left []schema.Table, right schema.Table, useIndexPrefix bool) []string {
	// USING requires same column names; we only relax type matching by category (number/string/time/bool).
	leftCounts := map[string]int{}
	leftTypes := map[string]schema.ColumnType{}
	leftAllCounts := map[string]int{}
	for _, col := range g.collectColumns(left) {
		leftAllCounts[col.Name]++
	}
	for _, ltbl := range left {
		for _, lcol := range g.collectJoinColumns(ltbl, useIndexPrefix) {
			leftCounts[lcol.Name]++
			if _, ok := leftTypes[lcol.Name]; !ok {
				leftTypes[lcol.Name] = lcol.Type
			}
		}
	}
	names := []string{}
	seen := map[string]struct{}{}
	for _, ltbl := range left {
		pairs := g.collectJoinPairs(ltbl, right, true, useIndexPrefix)
		for _, pair := range pairs {
			if leftCounts[pair.Left.Name] != 1 {
				continue
			}
			if leftAllCounts[pair.Left.Name] != 1 {
				continue
			}
			if !compatibleColumnType(leftTypes[pair.Left.Name], pair.Left.Type) {
				continue
			}
			if _, ok := seen[pair.Left.Name]; ok {
				continue
			}
			seen[pair.Left.Name] = struct{}{}
			names = append(names, pair.Left.Name)
		}
	}
	return names
}

func (g *Generator) naturalJoinAllowed(left []schema.Table, right schema.Table) bool {
	if g == nil || len(left) == 0 || len(right.Columns) == 0 {
		return false
	}
	leftCols := g.collectColumns(left)
	if len(leftCols) == 0 {
		return false
	}
	counts := make(map[string]int, len(leftCols))
	for _, col := range leftCols {
		counts[col.Name]++
	}
	for _, col := range right.Columns {
		if counts[col.Name] > 1 {
			return false
		}
	}
	return true
}

func (g *Generator) joinCondition(left []schema.Table, right schema.Table) Expr {
	if l, r, ok := g.pickJoinColumnPair(left, right); ok {
		eq := BinaryExpr{Left: ColumnExpr{Ref: l}, Op: "=", Right: ColumnExpr{Ref: r}}
		policy := strings.ToLower(strings.TrimSpace(g.Config.Oracles.JoinOnPolicy))
		if policy == "complex" {
			tables := append([]schema.Table{}, left...)
			tables = append(tables, right)
			extra := g.GeneratePredicate(tables, 1, false, 0)
			if extra != nil {
				return BinaryExpr{Left: eq, Op: "AND", Right: extra}
			}
		}
		return eq
	}
	if names := g.pickUsingColumns(left, right); len(names) > 0 {
		if expr := joinConditionFromUsing(left, right, names); expr != nil {
			return expr
		}
	}
	policy := strings.ToLower(strings.TrimSpace(g.Config.Oracles.JoinOnPolicy))
	if policy == "simple" && g.Config.Oracles.JoinUsingProb >= 100 {
		if l, r, ok := g.pickLooseJoinColumnPair(left, right); ok {
			return BinaryExpr{Left: ColumnExpr{Ref: l}, Op: "<=>", Right: ColumnExpr{Ref: r}}
		}
	}
	return g.falseExpr()
}

func joinConditionFromUsing(left []schema.Table, right schema.Table, names []string) Expr {
	if len(names) == 0 {
		return nil
	}
	name := names[0]
	leftTable := findTableWithColumn(left, name)
	if leftTable == "" {
		return nil
	}
	return BinaryExpr{
		Left:  ColumnExpr{Ref: ColumnRef{Table: leftTable, Name: name}},
		Op:    "=",
		Right: ColumnExpr{Ref: ColumnRef{Table: right.Name, Name: name}},
	}
}

func findTableWithColumn(tables []schema.Table, column string) string {
	if column == "" {
		return ""
	}
	for _, tbl := range tables {
		if _, ok := tbl.ColumnByName(column); ok {
			return tbl.Name
		}
	}
	return ""
}

func (g *Generator) collectColumns(tables []schema.Table) []ColumnRef {
	cols := make([]ColumnRef, 0, 8)
	for _, tbl := range tables {
		// Preserve CTE/derived columns as-is; no schema lookup needed.
		if len(tbl.Columns) == 0 {
			continue
		}
		if g.State == nil {
			for _, col := range tbl.Columns {
				cols = append(cols, ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type})
			}
			continue
		}
		if st, ok := g.State.TableByName(tbl.Name); ok {
			for _, col := range tbl.Columns {
				if _, ok := st.ColumnByName(col.Name); ok {
					cols = append(cols, ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type})
				}
			}
			continue
		}
		for _, col := range tbl.Columns {
			cols = append(cols, ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type})
		}
	}
	return cols
}

func (g *Generator) collectIndexPrefixColumns(tables []schema.Table) []ColumnRef {
	cols := make([]ColumnRef, 0, 8)
	seen := map[string]struct{}{}
	addColIfNew := func(tblName string, col schema.Column) {
		key := tblName + "." + col.Name
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		cols = append(cols, ColumnRef{Table: tblName, Name: col.Name, Type: col.Type})
	}
	for _, tbl := range tables {
		for _, col := range tbl.Columns {
			if !col.HasIndex {
				continue
			}
			addColIfNew(tbl.Name, col)
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
			addColIfNew(tbl.Name, col)
		}
	}
	return cols
}

func (g *Generator) pickJoinColumnPair(left []schema.Table, right schema.Table) (leftCol ColumnRef, rightCol ColumnRef, ok bool) {
	if g.Config.Features.DSG {
		leftTables := left
		for _, tbl := range left {
			if tbl.Name == "t0" {
				leftTables = []schema.Table{tbl}
				break
			}
		}
		preferIndexPrefix := util.Chance(g.Rand, g.indexPrefixProb())
		prefixFirst := []bool{true, false}
		if !preferIndexPrefix {
			prefixFirst = []bool{false, true}
		}
		for _, useIndexPrefix := range prefixFirst {
			for _, ltbl := range leftTables {
				pairs := g.collectJoinPairs(ltbl, right, true, useIndexPrefix)
				if len(pairs) > 0 {
					pair := pairs[g.Rand.Intn(len(pairs))]
					leftCol, rightCol, ok = pair.Left, pair.Right, true
					return
				}
			}
			for _, ltbl := range leftTables {
				pairs := g.collectJoinPairs(ltbl, right, false, useIndexPrefix)
				if len(pairs) > 0 {
					pair := pairs[g.Rand.Intn(len(pairs))]
					leftCol, rightCol, ok = pair.Left, pair.Right, true
					return
				}
			}
		}
		return
	}
	tryPick := func(requireSameName bool, useIndexPrefix bool) bool {
		for _, ltbl := range left {
			pairs := g.collectJoinPairs(ltbl, right, requireSameName, useIndexPrefix)
			if len(pairs) == 0 {
				continue
			}
			pair := pairs[g.Rand.Intn(len(pairs))]
			leftCol, rightCol, ok = pair.Left, pair.Right, true
			return true
		}
		return false
	}
	if util.Chance(g.Rand, g.indexPrefixProb()) {
		if tryPick(true, true) {
			return
		}
		if tryPick(false, true) {
			return
		}
	}
	if tryPick(true, false) {
		return
	}
	if tryPick(false, false) {
		return
	}
	return
}

func (g *Generator) pickLooseJoinColumnPair(left []schema.Table, right schema.Table) (leftCol ColumnRef, rightCol ColumnRef, ok bool) {
	leftCols := g.collectColumns(left)
	rightCols := g.collectColumns([]schema.Table{right})
	if len(leftCols) == 0 || len(rightCols) == 0 {
		return ColumnRef{}, ColumnRef{}, false
	}
	pairs := make([]columnPair, 0, 8)
	for _, l := range leftCols {
		for _, r := range rightCols {
			if compatibleColumnType(l.Type, r.Type) {
				pairs = append(pairs, columnPair{Left: l, Right: r})
			}
		}
	}
	if len(pairs) == 0 {
		leftCol = leftCols[g.Rand.Intn(len(leftCols))]
		rightCol = rightCols[g.Rand.Intn(len(rightCols))]
		return leftCol, rightCol, true
	}
	pair := pairs[g.Rand.Intn(len(pairs))]
	return pair.Left, pair.Right, true
}

func (g *Generator) pickCorrelatedJoinPair(outerTables []schema.Table, inner schema.Table) (outerCol ColumnRef, innerCol ColumnRef, ok bool) {
	if len(outerTables) == 0 {
		return ColumnRef{}, ColumnRef{}, false
	}
	if g.Config.Features.DSG {
		anchors := outerTables
		for _, tbl := range outerTables {
			if tbl.Name == "t0" {
				anchors = []schema.Table{tbl}
				break
			}
		}
		preferIndexPrefix := util.Chance(g.Rand, g.indexPrefixProb())
		prefixFirst := []bool{true, false}
		if !preferIndexPrefix {
			prefixFirst = []bool{false, true}
		}
		for _, useIndexPrefix := range prefixFirst {
			for _, tbl := range anchors {
				pairs := g.collectJoinPairs(tbl, inner, true, useIndexPrefix)
				if len(pairs) == 0 {
					continue
				}
				pair := pairs[g.Rand.Intn(len(pairs))]
				return pair.Left, pair.Right, true
			}
			for _, tbl := range anchors {
				pairs := g.collectJoinPairs(tbl, inner, false, useIndexPrefix)
				if len(pairs) == 0 {
					continue
				}
				pair := pairs[g.Rand.Intn(len(pairs))]
				return pair.Left, pair.Right, true
			}
		}
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

func hasCrossOrTrueJoin(from FromClause) bool {
	for _, join := range from.Joins {
		if join.Type == JoinCross {
			return true
		}
		if join.Type == JoinInner && isLiteralEqualityTrue(join.On) {
			return true
		}
	}
	return false
}

func isLiteralEqualityTrue(expr Expr) bool {
	bin, ok := expr.(BinaryExpr)
	if !ok || bin.Op != "=" {
		return false
	}
	left, lok := bin.Left.(LiteralExpr)
	right, rok := bin.Right.(LiteralExpr)
	if !lok || !rok {
		return false
	}
	return reflect.DeepEqual(left.Value, right.Value)
}

func (g *Generator) maybeEmulateFullJoin(query *SelectQuery) {
	if query == nil || len(query.SetOps) > 0 || len(query.From.Joins) != 1 {
		return
	}
	if !util.Chance(g.Rand, FullJoinEmulationProb) {
		return
	}
	g.fullJoinEmulationAttempted = true
	if ok, reason := g.applyFullJoinEmulationWithReason(query); !ok {
		g.fullJoinEmulationReject = reason
		return
	}
	g.fullJoinEmulationReject = ""
}

func (g *Generator) applyFullJoinEmulation(query *SelectQuery) bool {
	ok, _ := g.applyFullJoinEmulationWithReason(query)
	return ok
}

func (g *Generator) applyFullJoinEmulationWithReason(query *SelectQuery) (bool, string) {
	if query == nil || len(query.SetOps) > 0 || len(query.From.Joins) != 1 {
		return false, "invalid_shape"
	}
	base := query.From.baseName()
	if base == "" {
		return false, "base_table_missing"
	}
	join := query.From.Joins[0]
	if join.Type == JoinCross {
		return false, "cross_join"
	}
	nullFilter := fullJoinRightAntiFilter(base, join)
	if nullFilter == nil {
		return false, "anti_filter_missing"
	}
	left := query.Clone()
	right := query.Clone()
	left.SetOps = nil
	right.SetOps = nil
	left.From.Joins[0].Type = JoinLeft
	right.From.Joins[0].Type = JoinRight
	if right.Where == nil {
		right.Where = nullFilter
	} else {
		right.Where = BinaryExpr{Left: right.Where, Op: "AND", Right: nullFilter}
	}
	*query = *left
	query.SetOps = []SetOperation{{
		Type:  SetOperationUnion,
		All:   true,
		Query: right,
	}}
	query.FullJoinEmulation = true
	clearSetOperationOrderLimit(query)
	return true, ""
}

func fullJoinRightAntiFilter(baseTable string, join Join) Expr {
	if baseTable == "" {
		return nil
	}
	if len(join.Using) > 0 {
		usingCol := join.Using[0]
		if usingCol == "" {
			return nil
		}
		// USING/NATURAL join outputs merged columns; keep this unqualified to satisfy
		// USING scope visibility rules.
		return BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Name: usingCol}},
			Op:    "IS",
			Right: LiteralExpr{Value: nil},
		}
	}
	if join.On == nil {
		return nil
	}
	key, ok := pickBaseJoinKey(join.On, baseTable)
	if !ok {
		return nil
	}
	return BinaryExpr{
		Left:  ColumnExpr{Ref: ColumnRef{Table: baseTable, Name: key}},
		Op:    "IS",
		Right: LiteralExpr{Value: nil},
	}
}

func pickBaseJoinKey(expr Expr, baseTable string) (string, bool) {
	switch e := expr.(type) {
	case BinaryExpr:
		switch e.Op {
		case "AND":
			if key, ok := pickBaseJoinKey(e.Left, baseTable); ok {
				return key, true
			}
			return pickBaseJoinKey(e.Right, baseTable)
		case "=", "<=>":
			left, lok := exprColumnRef(e.Left)
			right, rok := exprColumnRef(e.Right)
			if !lok || !rok {
				return "", false
			}
			if left.Table == baseTable && right.Table != baseTable {
				return left.Name, true
			}
			if right.Table == baseTable && left.Table != baseTable {
				return right.Name, true
			}
			return "", false
		default:
			return "", false
		}
	default:
		return "", false
	}
}

func exprColumnRef(expr Expr) (ColumnRef, bool) {
	switch e := expr.(type) {
	case ColumnExpr:
		return e.Ref, true
	case UnaryExpr:
		if e.Op == "+" {
			return exprColumnRef(e.Expr)
		}
	}
	return ColumnRef{}, false
}

const (
	crossJoinRareDenom = 10_000
)

func rareChance(r *rand.Rand, denom int) bool {
	if r == nil || denom <= 1 {
		return denom <= 1
	}
	return r.Intn(denom) == 0
}

// compatibleColumnType and typeCategory are defined in type_compat.go.
