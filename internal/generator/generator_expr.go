package generator

import (
	"fmt"
	"strings"

	"shiro/internal/schema"
	"shiro/internal/util"
)

func (g *Generator) GenerateNumericExpr(tables []schema.Table) Expr {
	for i := 0; i < 3; i++ {
		col := g.randomColumn(tables)
		if col.Table == "" {
			break
		}
		if g.isNumericType(col.Type) {
			return ColumnExpr{Ref: col}
		}
	}
	return LiteralExpr{Value: g.Rand.Intn(100)}
}

// GenerateNumericExprPreferDecimal prefers DECIMAL columns for aggregates.
func (g *Generator) GenerateNumericExprPreferDecimal(tables []schema.Table) Expr {
	if col, ok := g.pickNumericColumnPreferDecimal(tables); ok {
		return ColumnExpr{Ref: col}
	}
	return g.GenerateNumericExpr(tables)
}

// GenerateStringExpr returns a varchar expression or literal.
func (g *Generator) GenerateStringExpr(tables []schema.Table) Expr {
	for i := 0; i < 3; i++ {
		col := g.randomColumn(tables)
		if col.Table == "" {
			break
		}
		if col.Type == schema.TypeVarchar {
			return ColumnExpr{Ref: col}
		}
	}
	return g.literalForColumn(schema.Column{Type: schema.TypeVarchar})
}

// GenerateGroupBy builds a GROUP BY list.
func (g *Generator) literalForExprType(expr Expr) Expr {
	switch v := expr.(type) {
	case ColumnExpr:
		return g.literalForColumn(schema.Column{Type: v.Ref.Type})
	case LiteralExpr:
		return g.literalForValue(v.Value)
	case FuncExpr:
		if g.isNumericFunc(v.Name) {
			return g.literalForColumn(schema.Column{Type: schema.TypeInt})
		}
		return g.literalForColumn(schema.Column{Type: schema.TypeVarchar})
	default:
		return g.randomLiteralExpr()
	}
}

func (g *Generator) literalForValue(val any) Expr {
	switch val.(type) {
	case int:
		return g.literalForColumn(schema.Column{Type: schema.TypeInt})
	case int64:
		return g.literalForColumn(schema.Column{Type: schema.TypeBigInt})
	case float64:
		return g.literalForColumn(schema.Column{Type: schema.TypeDouble})
	case bool:
		return g.literalForColumn(schema.Column{Type: schema.TypeBool})
	case string:
		return g.literalForColumn(schema.Column{Type: schema.TypeVarchar})
	default:
		return g.randomLiteralExpr()
	}
}

func (g *Generator) isNumericExpr(expr Expr) bool {
	switch v := expr.(type) {
	case ColumnExpr:
		return g.isNumericType(v.Ref.Type)
	case LiteralExpr:
		switch v.Value.(type) {
		case int, int64, float64:
			return true
		default:
			return false
		}
	case FuncExpr:
		return g.isNumericFunc(v.Name)
	default:
		return false
	}
}

func (g *Generator) isNumericFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "ABS", "ROUND", "LENGTH":
		return true
	default:
		return false
	}
}

func (g *Generator) exprType(expr Expr) (schema.ColumnType, bool) {
	switch v := expr.(type) {
	case ColumnExpr:
		return v.Ref.Type, true
	case LiteralExpr:
		switch v.Value.(type) {
		case int:
			return schema.TypeInt, true
		case int64:
			return schema.TypeBigInt, true
		case float64:
			return schema.TypeDouble, true
		case bool:
			return schema.TypeBool, true
		case string:
			if t, ok := literalStringType(v.Value.(string)); ok {
				return t, true
			}
			return schema.TypeVarchar, true
		default:
			return 0, false
		}
	case FuncExpr:
		if g.isNumericFunc(v.Name) {
			return schema.TypeInt, true
		}
		return schema.TypeVarchar, true
	default:
		return 0, false
	}
}

func (g *Generator) pickComparableExpr(tables []schema.Table) (Expr, schema.ColumnType) {
	col := g.randomColumn(tables)
	if col.Table != "" {
		return ColumnExpr{Ref: col}, col.Type
	}
	colType := g.randomColumnType()
	return g.literalForColumn(schema.Column{Type: colType}), colType
}

func literalStringType(value string) (schema.ColumnType, bool) {
	if isDateLiteral(value) {
		return schema.TypeDate, true
	}
	if isDateTimeLiteral(value) {
		return schema.TypeDatetime, true
	}
	return 0, false
}

func isDateLiteral(value string) bool {
	if len(value) != 10 {
		return false
	}
	for i, ch := range value {
		switch i {
		case 4, 7:
			if ch != '-' {
				return false
			}
		default:
			if ch < '0' || ch > '9' {
				return false
			}
		}
	}
	return true
}

func isDateTimeLiteral(value string) bool {
	if len(value) != 19 {
		return false
	}
	for i, ch := range value {
		switch i {
		case 4, 7:
			if ch != '-' {
				return false
			}
		case 10:
			if ch != ' ' {
				return false
			}
		case 13, 16:
			if ch != ':' {
				return false
			}
		default:
			if ch < '0' || ch > '9' {
				return false
			}
		}
	}
	return true
}

func (g *Generator) generateComparablePair(tables []schema.Table, allowSubquery bool, subqDepth int) (Expr, Expr) {
	if util.Chance(g.Rand, 60) {
		left, colType := g.pickComparableExpr(tables)
		right := g.literalForColumn(schema.Column{Type: colType})
		return left, right
	}
	left := g.generateScalarExpr(tables, 0, allowSubquery, subqDepth)
	right := g.generateScalarExpr(tables, 0, allowSubquery, subqDepth)
	if t, ok := g.exprType(left); ok {
		return left, g.literalForColumn(schema.Column{Type: t})
	}
	if t, ok := g.exprType(right); ok {
		return g.literalForColumn(schema.Column{Type: t}), right
	}
	return left, right
}

func (g *Generator) generateScalarExpr(tables []schema.Table, depth int, allowSubquery bool, subqDepth int) Expr {
	if depth <= 0 {
		if util.Chance(g.Rand, 50) {
			col := g.randomColumn(tables)
			if col.Table != "" {
				return ColumnExpr{Ref: col}
			}
		}
		return g.randomLiteralExpr()
	}

	choice := g.Rand.Intn(5)
	switch choice {
	case 0:
		return g.randomLiteralExpr()
	case 1:
		col := g.randomColumn(tables)
		if col.Table != "" {
			return ColumnExpr{Ref: col}
		}
		return g.randomLiteralExpr()
	case 2:
		left := g.GenerateNumericExpr(tables)
		right := g.GenerateNumericExpr(tables)
		return BinaryExpr{Left: left, Op: g.pickArithmetic(), Right: right}
	case 3:
		name := g.pickFunc()
		var arg Expr
		if g.isNumericFunc(name) {
			arg = g.GenerateNumericExpr(tables)
		} else {
			arg = g.GenerateStringExpr(tables)
		}
		return FuncExpr{Name: name, Args: []Expr{arg}}
	case 4:
		if allowSubquery && subqDepth > 0 {
			sub := g.GenerateSubquery(tables, subqDepth-1)
			if sub != nil {
				return SubqueryExpr{Query: sub}
			}
		}
		return g.randomLiteralExpr()
	default:
		return g.randomLiteralExpr()
	}
}

// GenerateSubquery builds a scalar subquery, optionally correlated.
func (g *Generator) pickColumnByType(tbl schema.Table, t schema.ColumnType) schema.Column {
	for _, col := range tbl.Columns {
		if col.Type == t {
			return col
		}
	}
	if len(tbl.Columns) == 0 {
		return schema.Column{}
	}
	return tbl.Columns[g.Rand.Intn(len(tbl.Columns))]
}

func (g *Generator) pickForeignKeyColumns(child, parent schema.Table) (schema.Column, schema.Column) {
	for _, ccol := range child.Columns {
		for _, pcol := range parent.Columns {
			if ccol.Type == pcol.Type {
				return ccol, pcol
			}
		}
	}
	return schema.Column{}, schema.Column{}
}

func (g *Generator) randomColumnType() schema.ColumnType {
	types := []schema.ColumnType{
		schema.TypeInt,
		schema.TypeBigInt,
		schema.TypeFloat,
		schema.TypeDouble,
		schema.TypeDecimal,
		schema.TypeVarchar,
		schema.TypeDate,
		schema.TypeDatetime,
		schema.TypeTimestamp,
		schema.TypeBool,
	}
	return types[g.Rand.Intn(len(types))]
}

func (g *Generator) pickUpdatableColumn(tbl schema.Table) (schema.Column, bool) {
	candidates := make([]schema.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		if col.Name == "id" {
			continue
		}
		candidates = append(candidates, col)
	}
	if len(candidates) == 0 {
		return schema.Column{}, false
	}
	return candidates[g.Rand.Intn(len(candidates))], true
}

func (g *Generator) isNumericType(t schema.ColumnType) bool {
	switch t {
	case schema.TypeInt, schema.TypeBigInt, schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
		return true
	default:
		return false
	}
}

func (g *Generator) pickComparison() string {
	ops := []string{"=", "<", ">", "<=", ">=", "!=", "<=>"}
	return ops[g.Rand.Intn(len(ops))]
}

func (g *Generator) pickArithmetic() string {
	ops := []string{"+", "-", "*"}
	return ops[g.Rand.Intn(len(ops))]
}

func (g *Generator) pickFunc() string {
	funcs := []string{"ABS", "LENGTH", "LOWER", "UPPER", "ROUND"}
	return funcs[g.Rand.Intn(len(funcs))]
}

func (g *Generator) randomLiteralExpr() Expr {
	return g.literalForColumn(schema.Column{Type: g.randomColumnType()})
}

func (g *Generator) literalForColumn(col schema.Column) LiteralExpr {
	switch col.Type {
	case schema.TypeInt, schema.TypeBigInt:
		return LiteralExpr{Value: g.Rand.Intn(100)}
	case schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
		return LiteralExpr{Value: float64(int(g.Rand.Float64()*10000)) / 100}
	case schema.TypeVarchar:
		return LiteralExpr{Value: fmt.Sprintf("s%d", g.Rand.Intn(100))}
	case schema.TypeDate:
		return LiteralExpr{Value: fmt.Sprintf("2024-01-%02d", g.Rand.Intn(28)+1)}
	case schema.TypeDatetime:
		return LiteralExpr{Value: fmt.Sprintf("2024-01-%02d 12:%02d:00", g.Rand.Intn(28)+1, g.Rand.Intn(59))}
	case schema.TypeTimestamp:
		return LiteralExpr{Value: fmt.Sprintf("2024-01-%02d 08:%02d:00", g.Rand.Intn(28)+1, g.Rand.Intn(59))}
	case schema.TypeBool:
		if util.Chance(g.Rand, 50) {
			return LiteralExpr{Value: 1}
		}
		return LiteralExpr{Value: 0}
	default:
		return LiteralExpr{Value: g.Rand.Intn(10)}
	}
}

// orderedArgs expects comparable values of the same type and returns them ordered.
// If types differ, it returns inputs unchanged.
func orderedArgs(a, b any) (any, any) {
	switch v := a.(type) {
	case int:
		vb, ok := b.(int)
		if ok && v > vb {
			return vb, v
		}
	case int64:
		vb, ok := b.(int64)
		if ok && v > vb {
			return vb, v
		}
	case float64:
		vb, ok := b.(float64)
		if ok && v > vb {
			return vb, v
		}
	case string:
		vb, ok := b.(string)
		if ok && v > vb {
			return vb, v
		}
	}
	return a, b
}

func (g *Generator) exprSQL(expr Expr) string {
	b := SQLBuilder{}
	expr.Build(&b)
	return b.String()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
