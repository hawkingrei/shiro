package generator

import "shiro/internal/util"

func (g *Generator) applyGroupByExtension(query *SelectQuery) bool {
	if g == nil || query == nil || len(query.GroupBy) == 0 {
		return false
	}
	resetGroupByExtensions(query)
	// Keep CUBE/GROUPING SETS off for ordinal GROUP BY because many dialects
	// only accept expression lists in these clauses.
	hasOrdinal := groupByHasOrdinal(query.GroupBy)
	if g.Config.Features.GroupByGroupingSets && !hasOrdinal && util.Chance(g.Rand, GroupByGroupingSetsProb) {
		if sets := buildGroupingSets(query.GroupBy); len(sets) > 0 {
			query.GroupByGroupingSets = sets
			return true
		}
	}
	if g.Config.Features.GroupByCube && !hasOrdinal && util.Chance(g.Rand, GroupByCubeProb) {
		query.GroupByWithCube = true
		return true
	}
	if g.Config.Features.GroupByRollup && util.Chance(g.Rand, GroupByRollupProb) {
		query.GroupByWithRollup = true
		return true
	}
	return false
}

func resetGroupByExtensions(query *SelectQuery) {
	if query == nil {
		return
	}
	query.GroupByWithRollup = false
	query.GroupByWithCube = false
	query.GroupByGroupingSets = nil
}

func groupByHasOrdinal(groupBy []Expr) bool {
	for _, expr := range groupBy {
		if _, ok := expr.(GroupByOrdinalExpr); ok {
			return true
		}
	}
	return false
}

func buildGroupingSets(groupBy []Expr) []GroupingSet {
	base := unwrapGroupByExprs(groupBy)
	if len(base) == 0 {
		return nil
	}
	if len(base) == 1 {
		return []GroupingSet{
			cloneGroupingSet(base),
			{},
		}
	}
	sets := []GroupingSet{
		cloneGroupingSet(base),
		cloneGroupingSet(base[:1]),
		cloneGroupingSet(base[len(base)-1:]),
		{},
	}
	return sets
}

func unwrapGroupByExprs(groupBy []Expr) []Expr {
	out := make([]Expr, 0, len(groupBy))
	for _, expr := range groupBy {
		if ord, ok := expr.(GroupByOrdinalExpr); ok && ord.Expr != nil {
			out = append(out, ord.Expr)
			continue
		}
		out = append(out, expr)
	}
	return out
}

func cloneGroupingSet(exprs []Expr) GroupingSet {
	if len(exprs) == 0 {
		return GroupingSet{}
	}
	out := make(GroupingSet, len(exprs))
	copy(out, exprs)
	return out
}
