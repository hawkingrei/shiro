package generator

import (
	"fmt"
	"strings"

	"shiro/internal/schema"
	"shiro/internal/util"
)

// (constants moved to constants.go)

// GenerateNumericExpr picks a numeric column or literal.
func (g *Generator) GenerateNumericExpr(tables []schema.Table) Expr {
	for i := 0; i < NumericExprPickTries; i++ {
		col := g.randomColumn(tables)
		if col.Table == "" {
			break
		}
		if g.isNumericType(col.Type) {
			return ColumnExpr{Ref: col}
		}
	}
	return LiteralExpr{Value: g.Rand.Intn(NumericLiteralMax)}
}

// GenerateNumericExprNoDouble avoids DOUBLE columns for aggregates.
func (g *Generator) GenerateNumericExprNoDouble(tables []schema.Table) Expr {
	for i := 0; i < NumericExprPickTries; i++ {
		col := g.randomColumn(tables)
		if col.Table == "" {
			break
		}
		if g.isNumericType(col.Type) && col.Type != schema.TypeDouble {
			return ColumnExpr{Ref: col}
		}
	}
	return LiteralExpr{Value: g.Rand.Intn(NumericLiteralMax)}
}

// GenerateNumericExprPreferDecimal prefers DECIMAL columns for aggregates.
func (g *Generator) GenerateNumericExprPreferDecimal(tables []schema.Table) Expr {
	if col, ok := g.pickNumericColumnPreferDecimal(tables); ok {
		return ColumnExpr{Ref: col}
	}
	return g.GenerateNumericExprNoDouble(tables)
}

// GenerateNumericExprPreferDecimalNoDouble prefers DECIMAL and avoids DOUBLE columns for aggregates.
func (g *Generator) GenerateNumericExprPreferDecimalNoDouble(tables []schema.Table) Expr {
	if col, ok := g.pickNumericColumnPreferDecimal(tables); ok {
		return ColumnExpr{Ref: col}
	}
	return g.GenerateNumericExprNoDouble(tables)
}

// GenerateStringExpr returns a varchar expression or literal.
func (g *Generator) GenerateStringExpr(tables []schema.Table) Expr {
	for i := 0; i < NumericExprPickTries; i++ {
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
	case GroupByOrdinalExpr:
		if v.Expr == nil {
			return 0, false
		}
		return g.exprType(v.Expr)
	default:
		return 0, false
	}
}

func (g *Generator) pickComparableExpr(tables []schema.Table) (Expr, schema.ColumnType) {
	if col, ok := g.pickComparableColumn(tables); ok {
		return ColumnExpr{Ref: col}, col.Type
	}
	colType := g.randomColumnType()
	return g.literalForColumn(schema.Column{Type: colType}), colType
}

func (g *Generator) pickComparableExprPreferJoinGraph(tables []schema.Table) (Expr, schema.ColumnType, bool) {
	if leftCol, _, ok := g.pickJoinGraphComparablePair(tables); ok {
		return ColumnExpr{Ref: leftCol}, leftCol.Type, true
	}
	expr, colType := g.pickComparableExpr(tables)
	return expr, colType, false
}

func (g *Generator) pickNumericExprPreferJoinGraph(tables []schema.Table) (Expr, bool) {
	if leftCol, _, ok := g.pickJoinGraphComparablePair(tables); ok && g.isNumericType(leftCol.Type) {
		return ColumnExpr{Ref: leftCol}, true
	}
	return nil, false
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

func (g *Generator) generateComparablePair(tables []schema.Table, allowSubquery bool, subqDepth int) (left Expr, right Expr) {
	if leftCol, rightCol, ok := g.pickJoinGraphComparablePair(tables); ok {
		g.trackPredicatePair(true)
		return ColumnExpr{Ref: leftCol}, ColumnExpr{Ref: rightCol}
	}
	g.trackPredicatePair(false)
	if leftCol, rightCol, ok := g.pickComparableColumnPair(tables); ok {
		return ColumnExpr{Ref: leftCol}, ColumnExpr{Ref: rightCol}
	}
	if util.Chance(g.Rand, ComparablePairColumnLiteralProb) {
		var colType schema.ColumnType
		left, colType = g.pickComparableExpr(tables)
		right = g.literalForExprType(left, colType)
		return left, right
	}
	left = g.generateScalarExpr(tables, 0, allowSubquery, subqDepth)
	if t, ok := g.exprType(left); ok {
		return left, g.literalForExprType(left, t)
	}
	right = g.generateScalarExpr(tables, 0, allowSubquery, subqDepth)
	if t, ok := g.exprType(right); ok {
		return g.literalForExprType(right, t), right
	}
	colType := g.randomColumnType()
	lit := g.literalForColumn(schema.Column{Type: colType})
	return lit, g.literalForColumn(schema.Column{Type: colType})
}

func (g *Generator) pickJoinGraphComparablePair(tables []schema.Table) (left ColumnRef, right ColumnRef, ok bool) {
	if len(tables) < 2 {
		return
	}
	adj := buildJoinAdjacency(tables)
	if !hasJoinEdges(adj) {
		return
	}
	edges := make([][2]int, 0, len(tables))
	for i := 0; i < len(adj); i++ {
		for _, j := range adj[i] {
			if i < j {
				edges = append(edges, [2]int{i, j})
			}
		}
	}
	if len(edges) == 0 {
		return
	}
	useIndexPrefix := util.Chance(g.Rand, g.indexPrefixProb())
	tries := min(4, len(edges))
	for i := 0; i < tries; i++ {
		edge := edges[g.Rand.Intn(len(edges))]
		pairs := g.collectJoinPairs(tables[edge[0]], tables[edge[1]], false, useIndexPrefix)
		if len(pairs) == 0 && useIndexPrefix {
			pairs = g.collectJoinPairs(tables[edge[0]], tables[edge[1]], false, false)
		}
		if len(pairs) == 0 {
			continue
		}
		pair := pairs[g.Rand.Intn(len(pairs))]
		return pair.Left, pair.Right, true
	}
	return
}

func (g *Generator) pickComparableColumn(tables []schema.Table) (ColumnRef, bool) {
	if util.Chance(g.Rand, g.indexPrefixProb()) {
		if idxCols := g.collectIndexPrefixColumns(tables); len(idxCols) > 0 {
			return idxCols[g.Rand.Intn(len(idxCols))], true
		}
	}
	cols := g.collectColumns(tables)
	if len(cols) == 0 {
		return ColumnRef{}, false
	}
	return cols[g.Rand.Intn(len(cols))], true
}

func (g *Generator) pickComparableColumnPair(tables []schema.Table) (left ColumnRef, right ColumnRef, ok bool) {
	if util.Chance(g.Rand, g.indexPrefixProb()) {
		if idxCols := g.collectIndexPrefixColumns(tables); len(idxCols) >= 2 {
			byCategory := map[int][]ColumnRef{}
			for _, col := range idxCols {
				byCategory[TypeCategory(col.Type)] = append(byCategory[TypeCategory(col.Type)], col)
			}
			typeCandidates := make([]int, 0, len(byCategory))
			for t, list := range byCategory {
				if len(list) >= 2 {
					typeCandidates = append(typeCandidates, t)
				}
			}
			if len(typeCandidates) > 0 {
				t := typeCandidates[g.Rand.Intn(len(typeCandidates))]
				list := byCategory[t]
				i := g.Rand.Intn(len(list))
				j := g.Rand.Intn(len(list))
				for i == j && len(list) > 1 {
					j = g.Rand.Intn(len(list))
				}
				left, right, ok = list[i], list[j], true
				return
			}
		}
	}
	cols := g.collectColumns(tables)
	if len(cols) < 2 {
		return
	}
	byCategory := map[int][]ColumnRef{}
	for _, col := range cols {
		byCategory[TypeCategory(col.Type)] = append(byCategory[TypeCategory(col.Type)], col)
	}
	typeCandidates := make([]int, 0, len(byCategory))
	for t, list := range byCategory {
		if len(list) >= 2 {
			typeCandidates = append(typeCandidates, t)
		}
	}
	if len(typeCandidates) == 0 {
		return
	}
	t := typeCandidates[g.Rand.Intn(len(typeCandidates))]
	list := byCategory[t]
	if len(list) < 2 {
		return
	}
	i := g.Rand.Intn(len(list))
	j := g.Rand.Intn(len(list))
	for i == j && len(list) > 1 {
		j = g.Rand.Intn(len(list))
	}
	left, right, ok = list[i], list[j], true
	return
}

func (g *Generator) generateScalarExpr(tables []schema.Table, depth int, allowSubquery bool, subqDepth int) Expr {
	if g.disallowScalarSubq {
		allowSubquery = false
	}
	if depth <= 0 {
		if util.Chance(g.Rand, ScalarExprColumnProb) {
			col := g.randomColumn(tables)
			if col.Table != "" {
				return ColumnExpr{Ref: col}
			}
		}
		return g.randomLiteralExpr()
	}

	choice := g.Rand.Intn(ScalarExprChoiceCount)
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
			g.subqueryAttempts++
			sub := g.GenerateSubquery(tables, subqDepth-1)
			if sub != nil {
				g.subqueryBuilt++
				return SubqueryExpr{Query: sub}
			}
			g.subqueryFailed++
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

func (g *Generator) pickForeignKeyColumns(child, parent schema.Table) (childCol schema.Column, parentCol schema.Column) {
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

func (g *Generator) recordDateSample(table string, column string, value string) {
	if table == "" || column == "" || value == "" {
		return
	}
	if g.dateSamples == nil {
		g.dateSamples = make(map[string]map[string][]string)
	}
	tableSamples := g.dateSamples[table]
	if tableSamples == nil {
		tableSamples = make(map[string][]string)
		g.dateSamples[table] = tableSamples
	}
	samples := tableSamples[column]
	if len(samples) < DateSampleMax {
		tableSamples[column] = append(samples, value)
		return
	}
	tableSamples[column][g.Rand.Intn(DateSampleMax)] = value
}

func (g *Generator) sampleDateLiteral(ref ColumnRef) (LiteralExpr, bool) {
	if g.dateSamples == nil || ref.Table == "" || ref.Name == "" {
		return LiteralExpr{}, false
	}
	tableSamples := g.dateSamples[ref.Table]
	if tableSamples == nil {
		return LiteralExpr{}, false
	}
	samples := tableSamples[ref.Name]
	if len(samples) == 0 {
		return LiteralExpr{}, false
	}
	return LiteralExpr{Value: samples[g.Rand.Intn(len(samples))]}, true
}

func (g *Generator) literalForColumnRef(ref ColumnRef) LiteralExpr {
	if ref.Type == schema.TypeDate || ref.Type == schema.TypeDatetime || ref.Type == schema.TypeTimestamp {
		if lit, ok := g.sampleDateLiteral(ref); ok {
			return lit
		}
	}
	return g.literalForColumn(schema.Column{Type: ref.Type})
}

func (g *Generator) literalForExprType(expr Expr, colType schema.ColumnType) LiteralExpr {
	if colType == schema.TypeDate || colType == schema.TypeDatetime || colType == schema.TypeTimestamp {
		if col, ok := expr.(ColumnExpr); ok {
			return g.literalForColumnRef(col.Ref)
		}
	}
	return g.literalForColumn(schema.Column{Type: colType})
}

func (g *Generator) randomDateParts() (year int, month int, day int) {
	year = util.RandIntRange(g.Rand, DateYearMin, DateYearMax)
	month = util.RandIntRange(g.Rand, 1, 12)
	day = util.RandIntRange(g.Rand, 1, util.DaysInMonth(year, month))
	return year, month, day
}

func (g *Generator) literalForColumn(col schema.Column) LiteralExpr {
	switch col.Type {
	case schema.TypeInt, schema.TypeBigInt:
		return LiteralExpr{Value: g.Rand.Intn(NumericLiteralMax)}
	case schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
		return LiteralExpr{Value: float64(int(g.Rand.Float64()*FloatLiteralScale)) / FloatLiteralDiv}
	case schema.TypeVarchar:
		return LiteralExpr{Value: fmt.Sprintf("s%d", g.Rand.Intn(StringLiteralMax))}
	case schema.TypeDate:
		year, month, day := g.randomDateParts()
		return LiteralExpr{Value: fmt.Sprintf("%04d-%02d-%02d", year, month, day)}
	case schema.TypeDatetime, schema.TypeTimestamp:
		year, month, day := g.randomDateParts()
		hour := util.RandIntRange(g.Rand, 0, 23)
		minute := util.RandIntRange(g.Rand, 0, TimeMinuteMax)
		second := util.RandIntRange(g.Rand, 0, TimeSecondMax)
		return LiteralExpr{Value: fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)}
	case schema.TypeBool:
		if util.Chance(g.Rand, BoolLiteralTrueProb) {
			return LiteralExpr{Value: 1}
		}
		return LiteralExpr{Value: 0}
	default:
		return LiteralExpr{Value: g.Rand.Intn(SmallIntLiteralMax)}
	}
}

// orderedArgs expects comparable values of the same type and returns them ordered.
// If types differ, it returns inputs unchanged.
func orderedArgs(a, b any) (left any, right any) {
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
