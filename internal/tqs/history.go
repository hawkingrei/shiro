package tqs

import (
	"math/rand"
	"sort"
	"strings"

	"shiro/internal/schema"
)

// History tracks join-edge coverage for TQS random walks.
type History struct {
	base string
	adj  map[string][]string
	edge map[string]int64
}

// NewHistory builds a history graph from the current schema.
func NewHistory(state *schema.State, base string) *History {
	h := &History{
		base: base,
		adj:  make(map[string][]string),
		edge: make(map[string]int64),
	}
	if state == nil || len(state.Tables) == 0 {
		return h
	}
	baseName := base
	if baseName == "" {
		baseName = "t0"
	}
	for _, tbl := range state.Tables {
		h.adj[tbl.Name] = nil
	}
	for _, tbl := range state.Tables {
		if tbl.Name == baseName {
			continue
		}
		if !shareKeyColumn(state, baseName, tbl.Name) {
			continue
		}
		h.addEdge(baseName, tbl.Name)
	}
	return h
}

// WalkTables performs a random walk over the history graph.
// length is the desired number of tables in the join chain.
func (h *History) WalkTables(r *rand.Rand, length int, gamma float64) []string {
	if r == nil {
		return nil
	}
	if length <= 1 {
		return []string{h.baseName()}
	}
	base := h.baseName()
	visited := map[string]struct{}{base: {}}
	path := []string{base}
	for len(path) < length {
		cur := path[len(path)-1]
		candidates := h.unvisitedNeighbors(cur, visited)
		if len(candidates) == 0 && cur != base {
			cur = base
			candidates = h.unvisitedNeighbors(base, visited)
		}
		if len(candidates) == 0 {
			break
		}
		next := h.pickNext(r, cur, candidates, gamma)
		path = append(path, next)
		visited[next] = struct{}{}
	}
	return path
}

// RecordPath updates edge coverage using a join path.
func (h *History) RecordPath(path []string) {
	if len(path) < 2 {
		return
	}
	for i := 1; i < len(path); i++ {
		key := edgeKey(path[i-1], path[i])
		h.edge[key]++
	}
}

func (h *History) baseName() string {
	if h.base == "" {
		return "t0"
	}
	return h.base
}

func (h *History) addEdge(a, b string) {
	if a == "" || b == "" || a == b {
		return
	}
	h.adj[a] = appendIfMissing(h.adj[a], b)
	h.adj[b] = appendIfMissing(h.adj[b], a)
}

func (h *History) unvisitedNeighbors(cur string, visited map[string]struct{}) []string {
	neighbors := h.adj[cur]
	if len(neighbors) == 0 {
		return nil
	}
	out := make([]string, 0, len(neighbors))
	for _, n := range neighbors {
		if _, ok := visited[n]; ok {
			continue
		}
		out = append(out, n)
	}
	return out
}

func (h *History) pickNext(r *rand.Rand, cur string, candidates []string, gamma float64) string {
	if len(candidates) == 1 {
		return candidates[0]
	}
	if gamma < 0 {
		gamma = 0
	} else if gamma > 1 {
		gamma = 1
	}
	if r.Float64() < gamma {
		return candidates[r.Intn(len(candidates))]
	}
	type cand struct {
		name  string
		count int64
	}
	stats := make([]cand, 0, len(candidates))
	for _, c := range candidates {
		stats = append(stats, cand{name: c, count: h.edge[edgeKey(cur, c)]})
	}
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].count == stats[j].count {
			return stats[i].name < stats[j].name
		}
		return stats[i].count < stats[j].count
	})
	minCount := stats[0].count
	pool := make([]string, 0, len(stats))
	for _, s := range stats {
		if s.count != minCount {
			break
		}
		pool = append(pool, s.name)
	}
	return pool[r.Intn(len(pool))]
}

func edgeKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "->" + b
}

func appendIfMissing(list []string, val string) []string {
	for _, v := range list {
		if v == val {
			return list
		}
	}
	return append(list, val)
}

func shareKeyColumn(state *schema.State, left, right string) bool {
	lt, ok := state.TableByName(left)
	if !ok {
		return false
	}
	rt, ok := state.TableByName(right)
	if !ok {
		return false
	}
	leftKeys := map[string]struct{}{}
	for _, col := range lt.Columns {
		if strings.HasPrefix(col.Name, "k") {
			leftKeys[col.Name] = struct{}{}
		}
	}
	if len(leftKeys) == 0 {
		return false
	}
	for _, col := range rt.Columns {
		if _, ok := leftKeys[col.Name]; ok {
			return true
		}
	}
	return false
}
