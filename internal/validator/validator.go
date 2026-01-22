package validator

import (
	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // Register TiDB parser driver.
)

// Validator wraps the TiDB parser for SQL validation.
type Validator struct {
	parser *parser.Parser
}

// New returns a Validator instance.
func New() *Validator {
	return &Validator{parser: parser.New()}
}

// Validate parses a SQL statement and returns any syntax error.
func (v *Validator) Validate(sql string) error {
	_, _, err := v.parser.Parse(sql, "", "")
	return err
}
