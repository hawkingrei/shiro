package generator

import (
	"math/rand"

	"shiro/internal/schema"
	"shiro/internal/util"
)

func (g *Generator) pickTables() []schema.Table {
	if len(g.State.Tables) == 0 {
		return nil
	}
	_, viewTables := schema.SplitTablesByView(g.State.Tables)
	maxTables := len(g.State.Tables)
	count := 1
	if g.Config.Features.Joins && maxTables > 1 {
		limit := min(maxTables, g.Config.MaxJoinTables)
		if g.Config.TQS.Enabled {
			count = g.pickTQSJoinCount(limit)
		} else {
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
		count = g.maybePreferFullJoinCandidate(count, limit)
	}
	if g.minJoinTables > 0 && count < g.minJoinTables {
		if maxTables >= g.minJoinTables {
			count = g.minJoinTables
		} else {
			count = maxTables
		}
	}
	if count > 1 && g.Config.Features.Joins {
		if picked := g.pickJoinTables(count); len(picked) == count {
			if !g.Config.Features.DSG && len(viewTables) > 0 && util.Chance(g.Rand, ViewJoinReplaceProb) {
				return replaceWithJoinableView(g.Rand, picked, viewTables)
			}
			return picked
		}
	}
	if count == 1 && len(viewTables) > 0 && util.Chance(g.Rand, ViewPickProb) {
		return []schema.Table{viewTables[g.Rand.Intn(len(viewTables))]}
	}
	idxs := g.Rand.Perm(maxTables)[:count]
	picked := make([]schema.Table, 0, count)
	for _, idx := range idxs {
		picked = append(picked, g.State.Tables[idx])
	}
	return picked
}

func (g *Generator) maybePreferFullJoinCandidate(count int, limit int) int {
	return g.maybePreferFullJoinCandidateWithProb(count, limit, FullJoinCandidateProb)
}

func (g *Generator) maybePreferFullJoinCandidateWithProb(count int, limit int, prob int) int {
	if g == nil {
		return count
	}
	if !g.Config.Features.Joins || !g.Config.Features.FullJoinEmulation {
		return count
	}
	if count <= 2 || limit < 2 {
		return count
	}
	if util.Chance(g.Rand, prob) {
		return 2
	}
	return count
}

func (g *Generator) pickTQSJoinCount(limit int) int {
	if limit <= 1 {
		return 1
	}
	minLen := g.Config.TQS.WalkMin
	maxLen := g.Config.TQS.WalkMax
	if maxLen > 0 && minLen <= 0 {
		minLen = 1
	}
	if minLen > 0 && maxLen > 0 {
		if minLen > maxLen {
			minLen, maxLen = maxLen, minLen
		}
		if minLen > limit {
			return limit
		}
		if maxLen > limit {
			maxLen = limit
		}
		return g.Rand.Intn(maxLen-minLen+1) + minLen
	}
	if g.Config.TQS.WalkLength > 0 {
		return min(limit, g.Config.TQS.WalkLength)
	}
	return min(limit, g.joinCount()+1)
}

func (g *Generator) pickJoinTables(count int) []schema.Table {
	if count <= 1 {
		return nil
	}
	if g.Config.Features.DSG {
		return g.pickDSGJoinTables(count)
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

func (g *Generator) pickDSGJoinTables(count int) []schema.Table {
	if count <= 0 || len(g.State.Tables) == 0 {
		return nil
	}
	var base schema.Table
	baseOK := false
	dims := make([]schema.Table, 0, len(g.State.Tables)-1)
	for _, tbl := range g.State.Tables {
		if tbl.IsView {
			continue
		}
		if tbl.Name == "t0" {
			base = tbl
			baseOK = true
			continue
		}
		dims = append(dims, tbl)
	}
	if !baseOK {
		return nil
	}
	if count == 1 || len(dims) == 0 {
		return []schema.Table{base}
	}
	maxCount := 1 + len(dims)
	if count > maxCount {
		count = maxCount
	}
	if g.Config.TQS.Enabled && g.TQSWalker != nil {
		path := g.TQSWalker.WalkTables(g.Rand, count, g.Config.TQS.Gamma)
		if len(path) >= 2 {
			picked := mapTablesByName(g.State.Tables, path)
			picked = ensureBaseFirst(picked, base.Name)
			if len(picked) < count {
				picked = appendMissingTables(picked, dims, count)
			}
			if len(picked) >= 2 {
				return picked
			}
		}
	}
	perm := g.Rand.Perm(len(dims))
	picked := make([]schema.Table, 0, count)
	picked = append(picked, base)
	for i := 0; i < count-1; i++ {
		picked = append(picked, dims[perm[i]])
	}
	return picked
}

func replaceWithJoinableView(r *rand.Rand, picked []schema.Table, views []schema.Table) []schema.Table {
	if len(picked) == 0 || len(views) == 0 {
		return picked
	}
	viewIdxs := randPerm(r, len(views))
	for _, vIdx := range viewIdxs {
		view := views[vIdx]
		for i := range picked {
			if view.Name == picked[i].Name {
				continue
			}
			if !joinableWithAny(view, picked, i) {
				continue
			}
			out := append([]schema.Table{}, picked...)
			out[i] = view
			return out
		}
	}
	return picked
}

func joinableWithAny(tbl schema.Table, picked []schema.Table, skip int) bool {
	for i := range picked {
		if i == skip {
			continue
		}
		if tablesJoinable(tbl, picked[i]) {
			return true
		}
	}
	return false
}

func randPerm(r *rand.Rand, n int) []int {
	if n <= 0 {
		return nil
	}
	if n == 1 {
		return []int{0}
	}
	return r.Perm(n)
}

func mapTablesByName(tables []schema.Table, names []string) []schema.Table {
	if len(names) == 0 {
		return nil
	}
	tableMap := make(map[string]schema.Table, len(tables))
	for _, tbl := range tables {
		tableMap[tbl.Name] = tbl
	}
	out := make([]schema.Table, 0, len(names))
	for _, name := range names {
		if tbl, ok := tableMap[name]; ok {
			out = append(out, tbl)
		}
	}
	return out
}

func ensureBaseFirst(tables []schema.Table, base string) []schema.Table {
	if len(tables) == 0 || base == "" {
		return tables
	}
	if tables[0].Name == base {
		return tables
	}
	for i, tbl := range tables {
		if tbl.Name == base {
			out := make([]schema.Table, 0, len(tables))
			out = append(out, tbl)
			out = append(out, append(tables[:i], tables[i+1:]...)...)
			return out
		}
	}
	return tables
}

func appendMissingTables(picked []schema.Table, candidates []schema.Table, target int) []schema.Table {
	if len(picked) >= target {
		return picked
	}
	seen := make(map[string]struct{}, len(picked))
	for _, tbl := range picked {
		seen[tbl.Name] = struct{}{}
	}
	for _, tbl := range candidates {
		if len(picked) >= target {
			break
		}
		if _, ok := seen[tbl.Name]; ok {
			continue
		}
		picked = append(picked, tbl)
		seen[tbl.Name] = struct{}{}
	}
	return picked
}
