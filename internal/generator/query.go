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

const (
	JoinInner JoinType = "JOIN"
	JoinLeft  JoinType = "LEFT JOIN"
	JoinRight JoinType = "RIGHT JOIN"
	JoinCross JoinType = "CROSS JOIN"
)

// Join models a FROM join clause.
type Join struct {
	Type  JoinType
	Table string
	On    Expr
	Using []string
}

// FromClause models a FROM clause with joins.
type FromClause struct {
	BaseTable string
	Joins     []Join
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
	With     []CTE
	Distinct bool
	Items    []SelectItem
	From     FromClause
	Where    Expr
	GroupBy  []Expr
	Having   Expr
	OrderBy  []OrderBy
	Limit    *int
}

// Build emits the SQL for the select query into the builder.
func (q *SelectQuery) Build(b *SQLBuilder) {
	if len(q.With) > 0 {
		b.Write("WITH ")
		for i, cte := range q.With {
			if i > 0 {
				b.Write(", ")
			}
			b.Write(cte.Name)
			b.Write(" AS (")
			cte.Query.Build(b)
			b.Write(")")
		}
		b.Write(" ")
	}
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
	b.Write(q.From.BaseTable)
	for _, join := range q.From.Joins {
		b.Write(" ")
		b.Write(string(join.Type))
		b.Write(" ")
		b.Write(join.Table)
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
	}
	if q.Having != nil {
		b.Write(" HAVING ")
		q.Having.Build(b)
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
	clone.Items = append([]SelectItem{}, q.Items...)
	clone.GroupBy = append([]Expr{}, q.GroupBy...)
	clone.OrderBy = append([]OrderBy{}, q.OrderBy...)
	clone.With = append([]CTE{}, q.With...)
	clone.From = FromClause{BaseTable: q.From.BaseTable, Joins: append([]Join{}, q.From.Joins...)}
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
