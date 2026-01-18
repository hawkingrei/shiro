package generator

import (
	"fmt"
	"strings"

	"shiro/internal/schema"
)

// Expr renders a SQL expression.
type Expr interface {
	Build(b *SQLBuilder)
	Columns() []ColumnRef
	Deterministic() bool
}

// ColumnRef identifies a column used in expressions.
type ColumnRef struct {
	Table string
	Name  string
	Type  schema.ColumnType
}

// ColumnExpr renders a column reference.
type ColumnExpr struct {
	Ref ColumnRef
}

// Build emits the qualified column reference.
func (e ColumnExpr) Build(b *SQLBuilder) {
	b.Write(fmt.Sprintf("%s.%s", e.Ref.Table, e.Ref.Name))
}

// Columns reports the column references used.
func (e ColumnExpr) Columns() []ColumnRef { return []ColumnRef{e.Ref} }

// Deterministic reports whether the expression is deterministic.
func (e ColumnExpr) Deterministic() bool { return true }

// LiteralExpr renders a literal value.
type LiteralExpr struct {
	Value any
}

// Build emits the literal as SQL text.
func (e LiteralExpr) Build(b *SQLBuilder) {
	switch v := e.Value.(type) {
	case string:
		b.Write("'")
		b.Write(strings.ReplaceAll(v, "'", "''"))
		b.Write("'")
	case nil:
		b.Write("NULL")
	default:
		b.Write(fmt.Sprintf("%v", v))
	}
}

// Columns reports the column references used.
func (e LiteralExpr) Columns() []ColumnRef { return nil }

// Deterministic reports whether the expression is deterministic.
func (e LiteralExpr) Deterministic() bool { return true }

// ParamExpr renders a prepared statement parameter.
type ParamExpr struct {
	Value any
}

// Build writes a parameter placeholder and tracks its value.
func (e ParamExpr) Build(b *SQLBuilder) {
	b.WriteArg(e.Value)
}

// Columns reports the column references used.
func (e ParamExpr) Columns() []ColumnRef { return nil }

// Deterministic reports whether the expression is deterministic.
func (e ParamExpr) Deterministic() bool { return true }

// UnaryExpr renders a unary expression.
type UnaryExpr struct {
	Op   string
	Expr Expr
}

// Build emits the unary expression.
func (e UnaryExpr) Build(b *SQLBuilder) {
	b.Write(e.Op)
	b.Write(" ")
	e.Expr.Build(b)
}

// Columns reports the column references used.
func (e UnaryExpr) Columns() []ColumnRef { return e.Expr.Columns() }

// Deterministic reports whether the expression is deterministic.
func (e UnaryExpr) Deterministic() bool { return e.Expr.Deterministic() }

// BinaryExpr renders a binary expression.
type BinaryExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

// Build emits the binary expression with parentheses.
func (e BinaryExpr) Build(b *SQLBuilder) {
	b.Write("(")
	e.Left.Build(b)
	b.Write(" ")
	b.Write(e.Op)
	b.Write(" ")
	e.Right.Build(b)
	b.Write(")")
}

// Columns reports the column references used.
func (e BinaryExpr) Columns() []ColumnRef {
	cols := append([]ColumnRef{}, e.Left.Columns()...)
	cols = append(cols, e.Right.Columns()...)
	return cols
}

// Deterministic reports whether the expression is deterministic.
func (e BinaryExpr) Deterministic() bool {
	return e.Left.Deterministic() && e.Right.Deterministic()
}

// FuncExpr renders a function call.
type FuncExpr struct {
	Name string
	Args []Expr
}

// Build emits the function call expression.
func (e FuncExpr) Build(b *SQLBuilder) {
	b.Write(e.Name)
	b.Write("(")
	for i, arg := range e.Args {
		if i > 0 {
			b.Write(", ")
		}
		arg.Build(b)
	}
	b.Write(")")
}

// Columns reports the column references used.
func (e FuncExpr) Columns() []ColumnRef {
	var cols []ColumnRef
	for _, arg := range e.Args {
		cols = append(cols, arg.Columns()...)
	}
	return cols
}

// Deterministic reports whether the expression is deterministic.
func (e FuncExpr) Deterministic() bool {
	for _, arg := range e.Args {
		if !arg.Deterministic() {
			return false
		}
	}
	return true
}

// CaseWhen represents a WHEN branch.
type CaseWhen struct {
	When Expr
	Then Expr
}

// CaseExpr renders a CASE expression.
type CaseExpr struct {
	Whens []CaseWhen
	Else  Expr
}

// Build emits a CASE expression.
func (e CaseExpr) Build(b *SQLBuilder) {
	b.Write("CASE ")
	for _, w := range e.Whens {
		b.Write("WHEN ")
		w.When.Build(b)
		b.Write(" THEN ")
		w.Then.Build(b)
		b.Write(" ")
	}
	if e.Else != nil {
		b.Write("ELSE ")
		e.Else.Build(b)
		b.Write(" ")
	}
	b.Write("END")
}

// Columns reports the column references used.
func (e CaseExpr) Columns() []ColumnRef {
	var cols []ColumnRef
	for _, w := range e.Whens {
		cols = append(cols, w.When.Columns()...)
		cols = append(cols, w.Then.Columns()...)
	}
	if e.Else != nil {
		cols = append(cols, e.Else.Columns()...)
	}
	return cols
}

// Deterministic reports whether the expression is deterministic.
func (e CaseExpr) Deterministic() bool {
	for _, w := range e.Whens {
		if !w.When.Deterministic() || !w.Then.Deterministic() {
			return false
		}
	}
	if e.Else != nil {
		return e.Else.Deterministic()
	}
	return true
}

// SubqueryExpr renders a scalar subquery.
type SubqueryExpr struct {
	Query *SelectQuery
}

// Build emits the scalar subquery expression.
func (e SubqueryExpr) Build(b *SQLBuilder) {
	b.Write("(")
	e.Query.Build(b)
	b.Write(")")
}

// Columns reports the column references used.
func (e SubqueryExpr) Columns() []ColumnRef { return nil }

// Deterministic reports whether the expression is deterministic.
func (e SubqueryExpr) Deterministic() bool { return true }

// ExistsExpr renders an EXISTS predicate.
type ExistsExpr struct {
	Query *SelectQuery
}

// Build emits the EXISTS predicate.
func (e ExistsExpr) Build(b *SQLBuilder) {
	b.Write("EXISTS (")
	e.Query.Build(b)
	b.Write(")")
}

// Columns reports the column references used.
func (e ExistsExpr) Columns() []ColumnRef { return nil }

// Deterministic reports whether the expression is deterministic.
func (e ExistsExpr) Deterministic() bool { return true }

// InExpr renders an IN predicate.
type InExpr struct {
	Left Expr
	List []Expr
}

// Build emits the IN predicate.
func (e InExpr) Build(b *SQLBuilder) {
	b.Write("(")
	e.Left.Build(b)
	b.Write(" IN (")
	for i, item := range e.List {
		if i > 0 {
			b.Write(", ")
		}
		item.Build(b)
	}
	b.Write("))")
}

// Columns reports the column references used.
func (e InExpr) Columns() []ColumnRef {
	cols := append([]ColumnRef{}, e.Left.Columns()...)
	for _, item := range e.List {
		cols = append(cols, item.Columns()...)
	}
	return cols
}

// Deterministic reports whether the expression is deterministic.
func (e InExpr) Deterministic() bool {
	if !e.Left.Deterministic() {
		return false
	}
	for _, item := range e.List {
		if !item.Deterministic() {
			return false
		}
	}
	return true
}

// WindowExpr renders a window function expression.
type WindowExpr struct {
	Name        string
	Args        []Expr
	PartitionBy []Expr
	OrderBy     []OrderBy
}

// Build emits the window function expression.
func (e WindowExpr) Build(b *SQLBuilder) {
	b.Write(e.Name)
	b.Write("(")
	for i, arg := range e.Args {
		if i > 0 {
			b.Write(", ")
		}
		arg.Build(b)
	}
	b.Write(") OVER (")
	needSpace := false
	if len(e.PartitionBy) > 0 {
		b.Write("PARTITION BY ")
		for i, expr := range e.PartitionBy {
			if i > 0 {
				b.Write(", ")
			}
			expr.Build(b)
		}
		needSpace = true
	}
	if len(e.OrderBy) > 0 {
		if needSpace {
			b.Write(" ")
		}
		b.Write("ORDER BY ")
		for i, ob := range e.OrderBy {
			if i > 0 {
				b.Write(", ")
			}
			ob.Expr.Build(b)
			if ob.Desc {
				b.Write(" DESC")
			}
		}
	}
	b.Write(")")
}

// Columns reports the column references used.
func (e WindowExpr) Columns() []ColumnRef {
	var cols []ColumnRef
	for _, arg := range e.Args {
		cols = append(cols, arg.Columns()...)
	}
	for _, expr := range e.PartitionBy {
		cols = append(cols, expr.Columns()...)
	}
	for _, ob := range e.OrderBy {
		cols = append(cols, ob.Expr.Columns()...)
	}
	return cols
}

// Deterministic reports whether the expression is deterministic.
func (e WindowExpr) Deterministic() bool {
	for _, arg := range e.Args {
		if !arg.Deterministic() {
			return false
		}
	}
	for _, expr := range e.PartitionBy {
		if !expr.Deterministic() {
			return false
		}
	}
	for _, ob := range e.OrderBy {
		if !ob.Expr.Deterministic() {
			return false
		}
	}
	return true
}
