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
	Rand          *rand.Rand
	Config        config.Config
	State         *schema.State
	Adaptive      *AdaptiveWeights
	Template      *TemplateWeights
	LastFeatures  *QueryFeatures
	Seed          int64
	tableSeq      int
	viewSeq       int
	indexSeq      int
	constraintSeq int
	maxDepth      int
	maxSubqDepth  int
}

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
