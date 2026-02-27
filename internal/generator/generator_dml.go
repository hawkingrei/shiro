package generator

import (
	"fmt"
	"strings"

	"shiro/internal/schema"
	"shiro/internal/util"
)

// (constants moved to constants.go)

// InsertSQL emits an INSERT statement and advances auto IDs.
func (g *Generator) InsertSQL(tbl *schema.Table) string {
	if tbl == nil {
		return ""
	}
	rowCount := g.Rand.Intn(InsertRowCountMax) + 1
	cols := make([]string, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		cols = append(cols, col.Name)
	}
	values := make([]string, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		vals := make([]string, 0, len(tbl.Columns))
		rowValid := true
		for _, col := range tbl.Columns {
			if fk, ok := foreignKeyByColumn(*tbl, col.Name); ok {
				val, consumeID, ok := g.foreignKeyInsertValue(tbl, col, fk)
				if !ok {
					rowValid = false
					break
				}
				if consumeID {
					tbl.NextID++
				}
				vals = append(vals, val)
				continue
			}
			if col.Name == "id" {
				vals = append(vals, fmt.Sprintf("%d", tbl.NextID))
				tbl.NextID++
				continue
			}
			lit := g.literalForColumn(col)
			if col.Type == schema.TypeDate || col.Type == schema.TypeDatetime || col.Type == schema.TypeTimestamp {
				if v, ok := lit.Value.(string); ok {
					g.recordDateSample(tbl.Name, col.Name, v)
				}
			}
			vals = append(vals, g.exprSQL(lit))
		}
		if !rowValid {
			continue
		}
		values = append(values, fmt.Sprintf("(%s)", strings.Join(vals, ", ")))
	}
	if len(values) == 0 {
		return ""
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tbl.Name, strings.Join(cols, ", "), strings.Join(values, ", "))
}

// UpdateSQL emits an UPDATE statement and returns predicate metadata.
func (g *Generator) UpdateSQL(tbl schema.Table) (sql string, predicate Expr, setExpr Expr, colRef ColumnRef) {
	if len(tbl.Columns) < 2 {
		return "", nil, nil, ColumnRef{}
	}
	col, ok := g.pickUpdatableColumn(tbl)
	if !ok {
		return "", nil, nil, ColumnRef{}
	}
	allowSubquery := g.Config.Features.Subqueries && util.Chance(g.Rand, DMLSubqueryProb)
	predicate = g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth, allowSubquery, g.maxSubqDepth)
	colRef = ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}
	if g.isNumericType(col.Type) {
		setExpr = BinaryExpr{Left: ColumnExpr{Ref: colRef}, Op: "+", Right: LiteralExpr{Value: 1}}
	} else {
		setExpr = g.literalForColumn(col)
	}
	builder := SQLBuilder{}
	predicate.Build(&builder)
	sql = fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s", tbl.Name, col.Name, g.exprSQL(setExpr), builder.String())
	return sql, predicate, setExpr, colRef
}

// DeleteSQL emits a DELETE statement and returns its predicate.
func (g *Generator) DeleteSQL(tbl schema.Table) (string, Expr) {
	allowSubquery := g.Config.Features.Subqueries && util.Chance(g.Rand, DMLSubqueryProb)
	predicate := g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth, allowSubquery, g.maxSubqDepth)
	builder := SQLBuilder{}
	predicate.Build(&builder)
	return fmt.Sprintf("DELETE FROM %s WHERE %s", tbl.Name, builder.String()), predicate
}

func foreignKeyByColumn(tbl schema.Table, columnName string) (schema.ForeignKey, bool) {
	for _, fk := range tbl.ForeignKeys {
		if fk.Column == columnName {
			return fk, true
		}
	}
	return schema.ForeignKey{}, false
}

func (g *Generator) foreignKeyInsertValue(tbl *schema.Table, col schema.Column, fk schema.ForeignKey) (value string, consumeID bool, ok bool) {
	if g == nil || g.State == nil || tbl == nil {
		return "", false, false
	}
	parent, ok := g.State.TableByName(fk.RefTable)
	if !ok {
		return "", false, false
	}
	parentRows := parent.NextID - 1
	if parentRows <= 0 {
		if col.Nullable {
			return "NULL", false, true
		}
		return "", false, false
	}
	// For id->id references, keep monotonic child ids while ensuring the parent row exists.
	if col.Name == "id" && fk.RefColumn == "id" {
		if tbl.NextID <= parentRows {
			return fmt.Sprintf("%d", tbl.NextID), true, true
		}
		return "", false, false
	}
	// For non-id references, pick an existing parent value.
	return fmt.Sprintf("(SELECT %s FROM %s ORDER BY id LIMIT 1)", fk.RefColumn, fk.RefTable), false, true
}
