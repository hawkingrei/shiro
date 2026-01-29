package impo

import (
	"bytes"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pkg/errors"
)

// ErrWithClause indicates the statement uses an unsupported WITH clause.
var ErrWithClause = errors.New("impo init with clause")

// Init removes unsupported constructs to make the query mutation-friendly.
// It only supports SELECT or set-operation statements.
func Init(sql string) (string, error) {
	return InitWithOptions(sql, InitOptions{})
}

// InitOptions controls stage1 rewrites.
type InitOptions struct {
	DisableStage1 bool
	KeepLRJoin    bool
}

// InitWithOptions removes unsupported constructs with optional rewrites.
func InitWithOptions(sql string, opts InitOptions) (string, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return "", errors.Wrap(err, "impo init parse")
	}
	if len(stmtNodes) == 0 {
		return "", errors.New("impo init empty statement")
	}
	rootNode := &stmtNodes[0]
	switch (*rootNode).(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
	default:
		return "", errors.New("impo init unsupported statement")
	}
	switch stmt := (*rootNode).(type) {
	case *ast.SelectStmt:
		if stmt.With != nil && stmt.With.IsRecursive {
			return "", ErrWithClause
		}
	case *ast.SetOprStmt:
		if stmt.With != nil && stmt.With.IsRecursive {
			return "", ErrWithClause
		}
	}

	v := &InitVisitor{DisableStage1: opts.DisableStage1, KeepLRJoin: opts.KeepLRJoin}
	(*rootNode).Accept(v)

	buf := new(bytes.Buffer)
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
	if err := (*rootNode).Restore(ctx); err != nil {
		return "", errors.Wrap(err, "impo init restore")
	}
	return buf.String(), nil
}
