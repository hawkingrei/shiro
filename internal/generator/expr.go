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
	if e.Ref.Table == "" {
		b.Write(e.Ref.Name)
		return
	}
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

// GroupByOrdinalExpr renders a GROUP BY ordinal while preserving its base expression.
type GroupByOrdinalExpr struct {
	Ordinal int
	Expr    Expr
}

// Build renders the expression. For GROUP BY clauses, it emits the ordinal position.
// For other contexts, it renders the underlying expression.
func (e GroupByOrdinalExpr) Build(b *SQLBuilder) {
	if e.Ordinal > 0 {
		b.Write(fmt.Sprintf("%d", e.Ordinal))
		return
	}
	if e.Expr != nil {
		e.Expr.Build(b)
		return
	}
	panic("invalid GroupByOrdinalExpr: Ordinal and Expr are both zero/nil")
}

// Columns reports the column references used by the base expression.
func (e GroupByOrdinalExpr) Columns() []ColumnRef {
	if e.Expr == nil {
		return nil
	}
	return e.Expr.Columns()
}

// Deterministic reports whether the base expression is deterministic.
func (e GroupByOrdinalExpr) Deterministic() bool {
	if e.Expr == nil {
		return true
	}
	return e.Expr.Deterministic()
}

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
	if e.Expr == nil {
		b.Write("NULL")
		return
	}
	e.Expr.Build(b)
}

// Columns reports the column references used.
func (e UnaryExpr) Columns() []ColumnRef {
	if e.Expr == nil {
		return nil
	}
	return e.Expr.Columns()
}

// Deterministic reports whether the expression is deterministic.
func (e UnaryExpr) Deterministic() bool {
	if e.Expr == nil {
		return false
	}
	return e.Expr.Deterministic()
}

// BinaryExpr renders a binary expression.
type BinaryExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

// Build emits the binary expression with parentheses.
func (e BinaryExpr) Build(b *SQLBuilder) {
	b.Write("(")
	if e.Left == nil {
		b.Write("NULL")
	} else {
		e.Left.Build(b)
	}
	b.Write(" ")
	b.Write(e.Op)
	b.Write(" ")
	if e.Right == nil {
		b.Write("NULL")
	} else {
		e.Right.Build(b)
	}
	b.Write(")")
}

// Columns reports the column references used.
func (e BinaryExpr) Columns() []ColumnRef {
	cols := make([]ColumnRef, 0, 4)
	if e.Left != nil {
		cols = append(cols, e.Left.Columns()...)
	}
	if e.Right != nil {
		cols = append(cols, e.Right.Columns()...)
	}
	return cols
}

// Deterministic reports whether the expression is deterministic.
func (e BinaryExpr) Deterministic() bool {
	if e.Left == nil || e.Right == nil {
		return false
	}
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
	cols := make([]ColumnRef, 0, len(e.Args))
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
func (e SubqueryExpr) Deterministic() bool {
	if e.Query == nil {
		return true
	}
	return QueryDeterministic(e.Query)
}

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
func (e ExistsExpr) Deterministic() bool {
	if e.Query == nil {
		return true
	}
	return QueryDeterministic(e.Query)
}

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

// CompareSubqueryExpr renders a quantified subquery predicate (ANY/SOME/ALL).
type CompareSubqueryExpr struct {
	Left       Expr
	Op         string
	Quantifier string
	Query      *SelectQuery
}

// Build emits the quantified subquery predicate.
func (e CompareSubqueryExpr) Build(b *SQLBuilder) {
	b.Write("(")
	e.Left.Build(b)
	b.Write(" ")
	b.Write(e.Op)
	b.Write(" ")
	b.Write(strings.ToUpper(strings.TrimSpace(e.Quantifier)))
	b.Write(" (")
	if e.Query != nil {
		e.Query.Build(b)
	}
	b.Write("))")
}

// Columns reports the column references used.
func (e CompareSubqueryExpr) Columns() []ColumnRef {
	if e.Left == nil {
		return nil
	}
	return e.Left.Columns()
}

// Deterministic reports whether the expression is deterministic.
func (e CompareSubqueryExpr) Deterministic() bool {
	if e.Left != nil && !e.Left.Deterministic() {
		return false
	}
	if e.Query == nil {
		return true
	}
	return QueryDeterministic(e.Query)
}

// IntervalExpr renders SQL INTERVAL literal (e.g., INTERVAL 1 DAY).
type IntervalExpr struct {
	Value int
	Unit  string
}

// Build emits SQL interval literal.
func (e IntervalExpr) Build(b *SQLBuilder) {
	b.Write("INTERVAL ")
	b.Write(fmt.Sprintf("%d", e.Value))
	b.Write(" ")
	unit := strings.ToUpper(strings.TrimSpace(e.Unit))
	if unit == "" {
		unit = "DAY"
	}
	b.Write(unit)
}

// Columns reports the column references used.
func (e IntervalExpr) Columns() []ColumnRef { return nil }

// Deterministic reports whether the expression is deterministic.
func (e IntervalExpr) Deterministic() bool { return true }

// WindowFrame describes a SQL window frame clause.
type WindowFrame struct {
	Unit  string
	Start string
	End   string
}

// Build emits SQL frame clause.
func (f WindowFrame) Build(b *SQLBuilder) {
	unit := strings.ToUpper(strings.TrimSpace(f.Unit))
	if unit == "" {
		unit = "ROWS"
	}
	start := strings.ToUpper(strings.TrimSpace(f.Start))
	if start == "" {
		start = "UNBOUNDED PRECEDING"
	}
	end := strings.ToUpper(strings.TrimSpace(f.End))
	if end == "" {
		end = "CURRENT ROW"
	}
	b.Write(unit)
	b.Write(" BETWEEN ")
	b.Write(start)
	b.Write(" AND ")
	b.Write(end)
}

// WindowDef describes a named WINDOW specification on SELECT query level.
type WindowDef struct {
	Name        string
	PartitionBy []Expr
	OrderBy     []OrderBy
	Frame       *WindowFrame
}

// WindowExpr renders a window function expression.
type WindowExpr struct {
	Name        string
	Args        []Expr
	WindowName  string
	PartitionBy []Expr
	OrderBy     []OrderBy
	Frame       *WindowFrame
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
	if e.WindowName != "" && len(e.PartitionBy) == 0 && len(e.OrderBy) == 0 && e.Frame == nil {
		b.Write(") OVER ")
		b.Write(e.WindowName)
		return
	}
	b.Write(") OVER (")
	writeWindowSpec(b, e.PartitionBy, e.OrderBy, e.Frame, e.WindowName)
	b.Write(")")
}

// Columns reports the column references used.
func (e WindowExpr) Columns() []ColumnRef {
	cols := make([]ColumnRef, 0, len(e.Args)+len(e.PartitionBy)+len(e.OrderBy))
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
