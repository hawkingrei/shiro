package generator

import (
	"fmt"
	"strings"

	"shiro/internal/schema"
	"shiro/internal/util"
)

func (g *Generator) InsertSQL(tbl *schema.Table) string {
	rowCount := g.Rand.Intn(3) + 1
	cols := make([]string, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		cols = append(cols, col.Name)
	}
	values := make([]string, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		vals := make([]string, 0, len(tbl.Columns))
		for _, col := range tbl.Columns {
			if col.Name == "id" {
				vals = append(vals, fmt.Sprintf("%d", tbl.NextID))
				tbl.NextID++
				continue
			}
			vals = append(vals, g.exprSQL(g.literalForColumn(col)))
		}
		values = append(values, fmt.Sprintf("(%s)", strings.Join(vals, ", ")))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tbl.Name, strings.Join(cols, ", "), strings.Join(values, ", "))
}

// UpdateSQL emits an UPDATE statement and returns predicate metadata.
func (g *Generator) UpdateSQL(tbl schema.Table) (string, Expr, Expr, ColumnRef) {
	if len(tbl.Columns) < 2 {
		return "", nil, nil, ColumnRef{}
	}
	col, ok := g.pickUpdatableColumn(tbl)
	if !ok {
		return "", nil, nil, ColumnRef{}
	}
	allowSubquery := g.Config.Features.Subqueries && util.Chance(g.Rand, 20)
	predicate := g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth, allowSubquery, g.maxSubqDepth)
	colRef := ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}
	var setExpr Expr
	if g.isNumericType(col.Type) {
		setExpr = BinaryExpr{Left: ColumnExpr{Ref: colRef}, Op: "+", Right: LiteralExpr{Value: 1}}
	} else {
		setExpr = g.literalForColumn(col)
	}
	builder := SQLBuilder{}
	predicate.Build(&builder)
	return fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s", tbl.Name, col.Name, g.exprSQL(setExpr), builder.String()), predicate, setExpr, colRef
}

// DeleteSQL emits a DELETE statement and returns its predicate.
func (g *Generator) DeleteSQL(tbl schema.Table) (string, Expr) {
	allowSubquery := g.Config.Features.Subqueries && util.Chance(g.Rand, 20)
	predicate := g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth, allowSubquery, g.maxSubqDepth)
	builder := SQLBuilder{}
	predicate.Build(&builder)
	return fmt.Sprintf("DELETE FROM %s WHERE %s", tbl.Name, builder.String()), predicate
}
