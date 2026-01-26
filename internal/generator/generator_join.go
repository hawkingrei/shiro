package generator

import (
	"math/rand"
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
	return cols
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
	return g.falseExpr()
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
	}
	return false
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
