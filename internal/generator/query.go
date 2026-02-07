package generator

import (
	"fmt"
	"strings"
)

// CTE represents a common table expression.
type CTE struct {
	Name  string
	Query *SelectQuery
}

// JoinType defines join kinds for SQL generation.
type JoinType string

// Join type constants used by the SQL generator.
const (
	JoinInner JoinType = "JOIN"
	JoinLeft  JoinType = "LEFT JOIN"
	JoinRight JoinType = "RIGHT JOIN"
	JoinCross JoinType = "CROSS JOIN"
)

// Join models a FROM join clause.
type Join struct {
	Type       JoinType
	Natural    bool
	Table      string
	TableQuery *SelectQuery
	TableAlias string
	On         Expr
	Using      []string
}

// FromClause models a FROM clause with joins.
type FromClause struct {
	BaseTable string
	BaseQuery *SelectQuery
	BaseAlias string
	Joins     []Join
}

// SetOperationType defines query set-operation kinds.
type SetOperationType string

// Set-operation constants used by the SQL generator.
const (
	SetOperationUnion     SetOperationType = "UNION"
	SetOperationExcept    SetOperationType = "EXCEPT"
	SetOperationIntersect SetOperationType = "INTERSECT"
)

// SetOperation models a query set operation.
type SetOperation struct {
	Type  SetOperationType
	All   bool
	Query *SelectQuery
}

// OrderBy models an ORDER BY item.
type OrderBy struct {
	Expr Expr
	Desc bool
}

// SelectItem models a SELECT list item.
type SelectItem struct {
	Expr  Expr
	Alias string
}

// SelectQuery models a SELECT statement.
type SelectQuery struct {
	With              []CTE
	WithRecursive     bool
	SetOps            []SetOperation
	Distinct          bool
	Items             []SelectItem
	From              FromClause
	Where             Expr
	GroupBy           []Expr
	GroupByWithRollup bool
	Having            Expr
	WindowDefs        []WindowDef
	OrderBy           []OrderBy
	Limit             *int
	Analysis          *QueryAnalysis
}

// Build emits the SQL for the select query into the builder.
func (q *SelectQuery) Build(b *SQLBuilder) {
	q.buildQueryExpression(b, true)
}

func (q *SelectQuery) buildQueryExpression(b *SQLBuilder, includeWith bool) {
	if includeWith && len(q.With) > 0 {
		b.Write("WITH ")
		if q.WithRecursive {
			b.Write("RECURSIVE ")
		}
		for i, cte := range q.With {
			if i > 0 {
				b.Write(", ")
			}
			b.Write(cte.Name)
			b.Write(" AS (")
			requireNoInlineWith(cte.Query, "cte body")
			cte.Query.buildQueryExpression(b, false)
			b.Write(")")
		}
		b.Write(" ")
	}
	if len(q.SetOps) == 0 {
		q.buildQueryBody(b)
		return
	}
	b.Write("(")
	q.buildQueryBody(b)
	b.Write(")")
	for _, op := range q.SetOps {
		if op.Query == nil {
			continue
		}
		b.Write(" ")
		b.Write(string(op.Type))
		if op.All {
			b.Write(" ALL")
		}
		b.Write(" (")
		requireNoInlineWith(op.Query, "set-operation operand")
		op.Query.buildQueryExpression(b, false)
		b.Write(")")
	}
}

func (q *SelectQuery) buildQueryBody(b *SQLBuilder) {
	b.Write("SELECT ")
	if q.Distinct {
		b.Write("DISTINCT ")
	}
	for i, item := range q.Items {
		if i > 0 {
			b.Write(", ")
		}
		item.Expr.Build(b)
		b.Write(" AS ")
		b.Write(item.Alias)
	}
	b.Write(" FROM ")
	writeTableFactor(b, q.From.BaseTable, q.From.baseName(), q.From.BaseQuery)
	for _, join := range q.From.Joins {
		b.Write(" ")
		if join.Natural {
			b.Write("NATURAL ")
		}
		b.Write(string(join.Type))
		b.Write(" ")
		writeTableFactor(b, join.Table, join.tableName(), join.TableQuery)
		if join.Natural {
			continue
		}
		if len(join.Using) > 0 {
			b.Write(" USING (")
			for i, col := range join.Using {
				if i > 0 {
					b.Write(", ")
				}
				b.Write(col)
			}
			b.Write(")")
		} else if join.On != nil {
			b.Write(" ON ")
			join.On.Build(b)
		}
	}
	if q.Where != nil {
		b.Write(" WHERE ")
		q.Where.Build(b)
	}
	if len(q.GroupBy) > 0 {
		b.Write(" GROUP BY ")
		for i, expr := range q.GroupBy {
			if i > 0 {
				b.Write(", ")
			}
			expr.Build(b)
		}
		if q.GroupByWithRollup {
			b.Write(" WITH ROLLUP")
		}
	}
	if q.Having != nil {
		b.Write(" HAVING ")
		q.Having.Build(b)
	}
	if len(q.WindowDefs) > 0 {
		b.Write(" WINDOW ")
		for i, def := range q.WindowDefs {
			if i > 0 {
				b.Write(", ")
			}
			b.Write(def.Name)
			b.Write(" AS (")
			writeWindowSpec(b, def.PartitionBy, def.OrderBy, def.Frame, "")
			b.Write(")")
		}
	}
	if len(q.OrderBy) > 0 {
		b.Write(" ORDER BY ")
		for i, ob := range q.OrderBy {
			if i > 0 {
				b.Write(", ")
			}
			ob.Expr.Build(b)
			if ob.Desc {
				b.Write(" DESC")
			}
		}
	}
	if q.Limit != nil {
		b.Write(" LIMIT ")
		b.Write(fmt.Sprintf("%d", *q.Limit))
	}
}

func writeTableFactor(b *SQLBuilder, tableName string, alias string, subquery *SelectQuery) {
	if subquery == nil {
		b.Write(tableName)
		return
	}
	requireNoInlineWith(subquery, "derived table")
	b.Write("(")
	subquery.buildQueryExpression(b, false)
	b.Write(") AS ")
	b.Write(alias)
}

func writeWindowSpec(b *SQLBuilder, partitionBy []Expr, orderBy []OrderBy, frame *WindowFrame, base string) {
	needSpace := false
	if base != "" {
		b.Write(base)
		needSpace = true
	}
	if len(partitionBy) > 0 {
		if needSpace {
			b.Write(" ")
		}
		b.Write("PARTITION BY ")
		for i, expr := range partitionBy {
			if i > 0 {
				b.Write(", ")
			}
			expr.Build(b)
		}
		needSpace = true
	}
	if len(orderBy) > 0 {
		if needSpace {
			b.Write(" ")
		}
		b.Write("ORDER BY ")
		for i, ob := range orderBy {
			if i > 0 {
				b.Write(", ")
			}
			ob.Expr.Build(b)
			if ob.Desc {
				b.Write(" DESC")
			}
		}
		needSpace = true
	}
	if frame != nil {
		if needSpace {
			b.Write(" ")
		}
		frame.Build(b)
	}
}

// Inline subqueries are currently rendered without WITH. Keep this explicit so we
// can safely enable nested WITH later without silently changing SQL semantics.
func requireNoInlineWith(query *SelectQuery, context string) {
	if query == nil || len(query.With) == 0 {
		return
	}
	panic(fmt.Sprintf("nested WITH is not supported in %s", context))
}

func (f FromClause) baseName() string {
	if f.BaseAlias != "" {
		return f.BaseAlias
	}
	return f.BaseTable
}

func (j Join) tableName() string {
	if j.TableAlias != "" {
		return j.TableAlias
	}
	return j.Table
}

// SQL renders the query and returns SQL text plus arguments.
func (q *SelectQuery) SQL() (string, []any) {
	var b SQLBuilder
	q.Build(&b)
	return b.String(), b.Args()
}

// ColumnAliases returns the SELECT-list aliases in order.
func (q *SelectQuery) ColumnAliases() []string {
	aliases := make([]string, 0, len(q.Items))
	for _, item := range q.Items {
		aliases = append(aliases, item.Alias)
	}
	return aliases
}

// Clone creates a shallow copy of the query structure.
func (q *SelectQuery) Clone() *SelectQuery {
	clone := *q
	clone.Analysis = nil
	clone.Items = append([]SelectItem{}, q.Items...)
	clone.GroupBy = append([]Expr{}, q.GroupBy...)
	clone.OrderBy = append([]OrderBy{}, q.OrderBy...)
	if len(q.WindowDefs) > 0 {
		clone.WindowDefs = make([]WindowDef, len(q.WindowDefs))
		for i, def := range q.WindowDefs {
			cloned := WindowDef{
				Name:        def.Name,
				PartitionBy: append([]Expr{}, def.PartitionBy...),
				OrderBy:     append([]OrderBy{}, def.OrderBy...),
			}
			if def.Frame != nil {
				frame := *def.Frame
				cloned.Frame = &frame
			}
			clone.WindowDefs[i] = cloned
		}
	}
	clone.With = make([]CTE, len(q.With))
	for i, cte := range q.With {
		clonedCTE := CTE{Name: cte.Name}
		if cte.Query != nil {
			clonedCTE.Query = cte.Query.Clone()
		}
		clone.With[i] = clonedCTE
	}
	clone.From = FromClause{
		BaseTable: q.From.BaseTable,
		BaseAlias: q.From.BaseAlias,
		Joins:     make([]Join, len(q.From.Joins)),
	}
	if q.From.BaseQuery != nil {
		clone.From.BaseQuery = q.From.BaseQuery.Clone()
	}
	for i, join := range q.From.Joins {
		clonedJoin := join
		clonedJoin.Using = append([]string{}, join.Using...)
		if join.TableQuery != nil {
			clonedJoin.TableQuery = join.TableQuery.Clone()
		}
		clone.From.Joins[i] = clonedJoin
	}
	if len(q.SetOps) > 0 {
		clone.SetOps = make([]SetOperation, 0, len(q.SetOps))
		for _, op := range q.SetOps {
			cloned := SetOperation{Type: op.Type, All: op.All}
			if op.Query != nil {
				cloned.Query = op.Query.Clone()
			}
			clone.SetOps = append(clone.SetOps, cloned)
		}
	}
	return &clone
}

// SignatureSQL wraps the query to produce count and checksum.
func (q *SelectQuery) SignatureSQL() string {
	aliases := q.ColumnAliases()
	cols := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		cols = append(cols, fmt.Sprintf("q.%s", alias))
	}
	if len(cols) == 0 {
		return fmt.Sprintf("SELECT COUNT(*) AS cnt, 0 AS checksum FROM (%s) q", q.SQLString())
	}
	checksumExpr := fmt.Sprintf("IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0)", strings.Join(cols, ", "))
	return fmt.Sprintf("SELECT COUNT(*) AS cnt, %s AS checksum FROM (%s) q", checksumExpr, q.SQLString())
}

// SQLString renders the query as a SQL string.
func (q *SelectQuery) SQLString() string {
	sql, _ := q.SQL()
	return sql
}
