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
	Err      error
}

// Oracle defines a SQL oracle contract.
type Oracle interface {
	Name() string
	Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result
}
