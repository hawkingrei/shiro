package oracle

import (
	"context"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

// Result captures an oracle execution outcome.
type Result struct {
	OK       bool
	Oracle   string
	SQL      []string
	Expected string
	Actual   string
	Details  map[string]any
	Metrics  map[string]int64
	Truth    *GroundTruthMetrics
	Err      error
}

// GroundTruthMetrics carries optional truth-check signals for reporting.
type GroundTruthMetrics struct {
	Enabled  bool
	Mismatch bool
	JoinSig  string
	RowCount int
}

// Oracle defines a SQL oracle contract.
type Oracle interface {
	Name() string
	Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result
}
