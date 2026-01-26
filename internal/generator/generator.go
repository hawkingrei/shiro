package generator

import (
	"fmt"
	"math/rand"
	"time"

	"shiro/internal/config"
	"shiro/internal/schema"
)

// Generator creates SQL statements based on schema state.
type Generator struct {
	Rand                *rand.Rand
	Config              config.Config
	State               *schema.State
	Adaptive            *AdaptiveWeights
	Template            *TemplateWeights
	LastFeatures        *QueryFeatures
	Seed                int64
	Truth               any
	TQSWalker           TQSWalker
	tableSeq            int
	viewSeq             int
	indexSeq            int
	constraintSeq       int
	maxDepth            int
	maxSubqDepth        int
	predicatePairsTotal int64
	predicatePairsJoin  int64
	joinTypeOverride    *JoinType
	minJoinTables       int
	predicateMode       PredicateMode
	disallowScalarSubq  bool
}

// PredicateMode controls predicate generation.
type PredicateMode int

// PredicateMode values define predicate generation constraints.
const (
	// PredicateModeDefault uses standard predicate generation.
	PredicateModeDefault PredicateMode = iota
	// PredicateModeNone disables predicate generation.
	PredicateModeNone
	// PredicateModeSimple uses AND-combined comparisons.
	PredicateModeSimple
	// PredicateModeSimpleColumns uses AND-combined column comparisons only.
	PredicateModeSimpleColumns
)

// PreparedQuery holds a prepared statement and args.
type PreparedQuery struct {
	SQL      string
	Args     []any
	ArgTypes []schema.ColumnType
}

// (constants moved to constants.go)

// New constructs a Generator with a seed.
func New(cfg config.Config, state *schema.State, seed int64) *Generator {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Generator{
		Rand:         rand.New(rand.NewSource(seed)),
		Config:       cfg,
		State:        state,
		Seed:         seed,
		maxDepth:     3,
		maxSubqDepth: 2,
	}
}

// SetAdaptiveWeights overrides feature weights for adaptive sampling.
func (g *Generator) SetAdaptiveWeights(weights AdaptiveWeights) {
	g.Adaptive = &weights
}

// ClearAdaptiveWeights disables adaptive sampling overrides.
func (g *Generator) ClearAdaptiveWeights() {
	g.Adaptive = nil
}

// SetTemplateWeights overrides template sampling weights.
func (g *Generator) SetTemplateWeights(weights TemplateWeights) {
	g.Template = &weights
}

// ClearTemplateWeights disables template sampling overrides.
func (g *Generator) ClearTemplateWeights() {
	g.Template = nil
}

// SetPredicateMode overrides predicate generation behavior.
func (g *Generator) SetPredicateMode(mode PredicateMode) {
	g.predicateMode = mode
}

// PredicateMode returns the current predicate mode override.
func (g *Generator) PredicateMode() PredicateMode {
	return g.predicateMode
}

// SetJoinTypeOverride forces all joins to use the given join type.
func (g *Generator) SetJoinTypeOverride(joinType JoinType) {
	g.joinTypeOverride = &joinType
}

// ClearJoinTypeOverride removes join type overrides.
func (g *Generator) ClearJoinTypeOverride() {
	g.joinTypeOverride = nil
}

// JoinTypeOverride returns the current join override if set.
func (g *Generator) JoinTypeOverride() (JoinType, bool) {
	if g.joinTypeOverride == nil {
		return "", false
	}
	return *g.joinTypeOverride, true
}

// SetMinJoinTables enforces a minimum join table count in selection.
func (g *Generator) SetMinJoinTables(count int) {
	g.minJoinTables = count
}

// MinJoinTables returns the current minimum join table override.
func (g *Generator) MinJoinTables() int {
	return g.minJoinTables
}

// ClearMinJoinTables removes the join table count override.
func (g *Generator) ClearMinJoinTables() {
	g.minJoinTables = 0
}

// SetDisallowScalarSubquery blocks generating scalar subqueries in expressions.
func (g *Generator) SetDisallowScalarSubquery(disallow bool) {
	g.disallowScalarSubq = disallow
}

// DisallowScalarSubquery reports whether scalar subqueries are disabled.
func (g *Generator) DisallowScalarSubquery() bool {
	return g.disallowScalarSubq
}

// SetTruth stores the RowID bitmap truth for TQS evaluation.
func (g *Generator) SetTruth(truth any) {
	g.Truth = truth
}

// TQSWalker provides random-walk join paths for TQS.
type TQSWalker interface {
	WalkTables(r *rand.Rand, length int, gamma float64) []string
	RecordPath(path []string)
}

// SetTQSWalker wires a TQS history graph for random-walk joins.
func (g *Generator) SetTQSWalker(history TQSWalker) {
	g.TQSWalker = history
}

func (g *Generator) resetPredicateStats() {
	g.predicatePairsTotal = 0
	g.predicatePairsJoin = 0
}

func (g *Generator) trackPredicatePair(fromJoinGraph bool) {
	g.predicatePairsTotal++
	if fromJoinGraph {
		g.predicatePairsJoin++
	}
}

// NextTableName returns a unique table name.
func (g *Generator) NextTableName() string {
	name := fmt.Sprintf("t%d", g.tableSeq)
	g.tableSeq++
	return name
}

// NextViewName returns a unique view name.
func (g *Generator) NextViewName() string {
	name := fmt.Sprintf("v%d", g.viewSeq)
	g.viewSeq++
	return name
}

// NextConstraintName returns a unique constraint name with a prefix.
func (g *Generator) NextConstraintName(prefix string) string {
	name := fmt.Sprintf("%s_%d", prefix, g.constraintSeq)
	g.constraintSeq++
	return name
}
