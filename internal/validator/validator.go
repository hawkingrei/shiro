package validator

import (
	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

type Validator struct {
	parser *parser.Parser
}

func New() *Validator {
	return &Validator{parser: parser.New()}
}

func (v *Validator) Validate(sql string) error {
	_, _, err := v.parser.Parse(sql, "", "")
	return err
}
