package generator

import (
	"fmt"
	"strings"

	"shiro/internal/schema"
	"shiro/internal/util"
)

// GeneratePreparedQuery builds a prepared query candidate for plan cache testing.
func (g *Generator) GeneratePreparedQuery() PreparedQuery {
	if len(g.State.Tables) == 0 {
		return PreparedQuery{}
	}
	candidates := []func() PreparedQuery{
		g.preparedSingleTable,
		g.preparedJoinQuery,
		g.preparedAggregateQuery,
	}
	if !g.Config.PlanCacheOnly {
		candidates = append(candidates, g.preparedCTEQuery)
	}
	for i := 0; i < len(candidates); i++ {
		pick := candidates[g.Rand.Intn(len(candidates))]
		if pq := pick(); pq.SQL != "" {
			if len(pq.Args) <= maxPreparedParams {
				return pq
			}
		}
	}
	return PreparedQuery{}
}

// GenerateNonPreparedPlanCacheQuery builds a simple query for non-prepared plan cache testing.
func (g *Generator) GenerateNonPreparedPlanCacheQuery() PreparedQuery {
	if len(g.State.Tables) == 0 {
		return PreparedQuery{}
	}
	for i := 0; i < 4; i++ {
		if pq := g.nonPreparedSingleTable(); pq.SQL != "" {
			if len(pq.Args) <= maxPreparedParams {
				return pq
			}
		}
	}
	return PreparedQuery{}
}

// GeneratePreparedArgsForQuery mutates previous args to produce a new execution.
func (g *Generator) GeneratePreparedArgsForQuery(prev []any, types []schema.ColumnType) []any {
	if len(prev) == 0 {
		return prev
	}
	out := make([]any, len(prev))
	copy(out, prev)
	for i := range out {
		if i < len(types) {
			out[i] = g.nextArgForType(types[i], out[i])
			continue
		}
		out[i] = out[i]
	}
	return out
}

// GenerateCTEQuery builds a simple CTE query over a base table.
func (g *Generator) preparedSingleTable() PreparedQuery {
	tbl := g.pickPreparedTable()
	if len(tbl.Columns) == 0 {
		return PreparedQuery{}
	}
	cols := make([]schema.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		if col.Name == "id" {
			// Avoid primary keys to reduce overly selective filters.
			continue
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		return PreparedQuery{}
	}
	col1 := cols[g.Rand.Intn(len(cols))]
	arg1 := g.literalForColumn(col1).Value
	arg2 := g.literalForColumn(col1).Value
	arg1, arg2 = orderedArgs(arg1, arg2)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? AND %s < ?", col1.Name, tbl.Name, col1.Name, col1.Name)
	args := []any{arg1, arg2}
	argTypes := []schema.ColumnType{col1.Type, col1.Type}
	if len(cols) > 1 && util.Chance(g.Rand, preparedExtraPredicateProb) {
		col2 := cols[g.Rand.Intn(len(cols))]
		if col2.Name != col1.Name {
			arg3 := g.literalForColumn(col2).Value
			query += fmt.Sprintf(" AND %s <> ?", col2.Name)
			args = append(args, arg3)
			argTypes = append(argTypes, col2.Type)
		}
	}
	return PreparedQuery{SQL: query, Args: args, ArgTypes: argTypes}
}

func (g *Generator) preparedJoinQuery() PreparedQuery {
	if !g.Config.Features.Joins || len(g.State.Tables) < 2 {
		return PreparedQuery{}
	}
	leftTbl, rightTbl, leftJoin, rightJoin := g.pickJoinColumns()
	if leftTbl.Name == "" || rightTbl.Name == "" || leftJoin.Name == "" || rightJoin.Name == "" {
		return PreparedQuery{}
	}
	leftParam, ok := g.pickAnyColumn(leftTbl)
	if !ok {
		return PreparedQuery{}
	}
	rightParam, ok := g.pickAnyColumn(rightTbl)
	if !ok {
		return PreparedQuery{}
	}
	leftArg1 := g.literalForColumn(leftParam).Value
	leftArg2 := g.literalForColumn(leftParam).Value
	leftArg1, leftArg2 = orderedArgs(leftArg1, leftArg2)
	rightArg1 := g.literalForColumn(rightParam).Value
	rightArg2 := g.literalForColumn(rightParam).Value
	rightArg1, rightArg2 = orderedArgs(rightArg1, rightArg2)
	query := fmt.Sprintf(
		"SELECT %s.%s, %s.%s FROM %s JOIN %s ON %s.%s = %s.%s WHERE %s.%s > ? AND %s.%s < ? AND %s.%s > ? AND %s.%s < ?",
		leftTbl.Name, leftParam.Name,
		rightTbl.Name, rightParam.Name,
		leftTbl.Name, rightTbl.Name,
		leftTbl.Name, leftJoin.Name, rightTbl.Name, rightJoin.Name,
		leftTbl.Name, leftParam.Name,
		leftTbl.Name, leftParam.Name,
		rightTbl.Name, rightParam.Name,
		rightTbl.Name, rightParam.Name,
	)
	return PreparedQuery{
		SQL:      query,
		Args:     []any{leftArg1, leftArg2, rightArg1, rightArg2},
		ArgTypes: []schema.ColumnType{leftParam.Type, leftParam.Type, rightParam.Type, rightParam.Type},
	}
}

func (g *Generator) preparedAggregateQuery() PreparedQuery {
	tbl := g.pickPreparedTable()
	if len(tbl.Columns) == 0 {
		return PreparedQuery{}
	}
	col, ok := g.pickNumericColumnPreferDecimalForTable(tbl)
	if !ok {
		return PreparedQuery{}
	}
	arg1 := g.literalForColumn(col).Value
	arg2 := g.literalForColumn(col).Value
	arg1, arg2 = orderedArgs(arg1, arg2)
	selectSQL := "COUNT(*) AS cnt"
	if g.isNumericType(col.Type) && util.Chance(g.Rand, preparedAggExtraProb) {
		selectSQL = "COUNT(*) AS cnt, SUM(" + col.Name + ") AS sum1"
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? AND %s < ?", selectSQL, tbl.Name, col.Name, col.Name)
	args := []any{arg1, arg2}
	argTypes := []schema.ColumnType{col.Type, col.Type}
	cols := g.collectNonIDColumns(tbl)
	if len(cols) > 1 && util.Chance(g.Rand, preparedAggExtraProb) {
		col2 := cols[g.Rand.Intn(len(cols))]
		if col2.Name != col.Name {
			arg3 := g.literalForColumn(col2).Value
			query += fmt.Sprintf(" AND %s <> ?", col2.Name)
			args = append(args, arg3)
			argTypes = append(argTypes, col2.Type)
		}
	}
	return PreparedQuery{SQL: query, Args: args, ArgTypes: argTypes}
}

func (g *Generator) nonPreparedSingleTable() PreparedQuery {
	tbl, ok := g.pickNonPartitionedTable()
	if !ok || len(tbl.Columns) == 0 {
		return PreparedQuery{}
	}
	col := tbl.Columns[g.Rand.Intn(len(tbl.Columns))]
	arg1 := g.literalForColumn(col).Value
	arg2 := g.literalForColumn(col).Value
	arg1, arg2 = orderedArgs(arg1, arg2)
	selectCols := []string{col.Name}
	if len(tbl.Columns) > 1 && util.Chance(g.Rand, 50) {
		col2 := tbl.Columns[g.Rand.Intn(len(tbl.Columns))]
		if col2.Name != col.Name {
			selectCols = append(selectCols, col2.Name)
		}
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? AND %s < ?", strings.Join(selectCols, ", "), tbl.Name, col.Name, col.Name)
	return PreparedQuery{SQL: query, Args: []any{arg1, arg2}, ArgTypes: []schema.ColumnType{col.Type, col.Type}}
}

func (g *Generator) pickPreparedTable() schema.Table {
	if len(g.State.Tables) == 0 {
		return schema.Table{}
	}
	partitioned := make([]schema.Table, 0, len(g.State.Tables))
	for _, tbl := range g.State.Tables {
		if tbl.Partitioned {
			partitioned = append(partitioned, tbl)
		}
	}
	if len(partitioned) > 0 && util.Chance(g.Rand, 60) {
		return partitioned[g.Rand.Intn(len(partitioned))]
	}
	return g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
}

func (g *Generator) collectNonIDColumns(tbl schema.Table) []schema.Column {
	cols := make([]schema.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		if col.Name == "id" {
			continue
		}
		cols = append(cols, col)
	}
	return cols
}

func (g *Generator) pickNumericColumnPreferDecimal(tables []schema.Table) (ColumnRef, bool) {
	decimalCols := make([]ColumnRef, 0)
	numericCols := make([]ColumnRef, 0)
	for _, tbl := range tables {
		for _, col := range tbl.Columns {
			if col.Name == "id" {
				continue
			}
			if !g.isNumericType(col.Type) {
				continue
			}
			ref := ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}
			numericCols = append(numericCols, ref)
			if col.Type == schema.TypeDecimal {
				decimalCols = append(decimalCols, ref)
			}
		}
	}
	if len(numericCols) == 0 {
		return ColumnRef{}, false
	}
	if len(decimalCols) > 0 && util.Chance(g.Rand, g.Config.Weights.Features.DecimalAggProb) {
		return decimalCols[g.Rand.Intn(len(decimalCols))], true
	}
	return numericCols[g.Rand.Intn(len(numericCols))], true
}

func (g *Generator) pickNumericColumnPreferDecimalForTable(tbl schema.Table) (schema.Column, bool) {
	decimalCols := make([]schema.Column, 0)
	numericCols := make([]schema.Column, 0)
	for _, col := range tbl.Columns {
		if col.Name == "id" {
			continue
		}
		if !g.isNumericType(col.Type) {
			continue
		}
		numericCols = append(numericCols, col)
		if col.Type == schema.TypeDecimal {
			decimalCols = append(decimalCols, col)
		}
	}
	if len(numericCols) == 0 {
		return schema.Column{}, false
	}
	if len(decimalCols) > 0 && util.Chance(g.Rand, g.Config.Weights.Features.DecimalAggProb) {
		return decimalCols[g.Rand.Intn(len(decimalCols))], true
	}
	return numericCols[g.Rand.Intn(len(numericCols))], true
}

func (g *Generator) pickNonPartitionedTable() (schema.Table, bool) {
	candidates := make([]schema.Table, 0, len(g.State.Tables))
	for _, tbl := range g.State.Tables {
		if !tbl.Partitioned {
			candidates = append(candidates, tbl)
		}
	}
	if len(candidates) == 0 {
		return schema.Table{}, false
	}
	return candidates[g.Rand.Intn(len(candidates))], true
}

func (g *Generator) preparedCTEQuery() PreparedQuery {
	if !g.Config.Features.CTE {
		return PreparedQuery{}
	}
	tbl := g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
	if len(tbl.Columns) < 2 {
		return PreparedQuery{}
	}
	col1 := tbl.Columns[g.Rand.Intn(len(tbl.Columns))]
	col2 := tbl.Columns[g.Rand.Intn(len(tbl.Columns))]
	arg1 := g.literalForColumn(col1).Value
	arg2 := g.literalForColumn(col2).Value
	query := fmt.Sprintf(
		"WITH cte AS (SELECT %s AS c0, %s AS c1 FROM %s WHERE %s = ?) SELECT c1 FROM cte WHERE c1 = ?",
		col1.Name, col2.Name, tbl.Name, col1.Name,
	)
	return PreparedQuery{
		SQL:      query,
		Args:     []any{arg1, arg2},
		ArgTypes: []schema.ColumnType{col1.Type, col2.Type},
	}
}

func (g *Generator) pickJoinColumns() (schema.Table, schema.Table, schema.Column, schema.Column) {
	if len(g.State.Tables) < 2 {
		return schema.Table{}, schema.Table{}, schema.Column{}, schema.Column{}
	}
	for i := 0; i < 6; i++ {
		left := g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
		right := g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
		if left.Name == right.Name {
			continue
		}
		for _, lcol := range left.Columns {
			for _, rcol := range right.Columns {
				if lcol.Type == rcol.Type {
					return left, right, lcol, rcol
				}
			}
		}
	}
	return schema.Table{}, schema.Table{}, schema.Column{}, schema.Column{}
}

func (g *Generator) pickAnyColumn(tbl schema.Table) (schema.Column, bool) {
	if len(tbl.Columns) == 0 {
		return schema.Column{}, false
	}
	return tbl.Columns[g.Rand.Intn(len(tbl.Columns))], true
}

func (g *Generator) nextArgForType(t schema.ColumnType, prev any) any {
	switch t {
	case schema.TypeInt, schema.TypeBigInt:
		if v, ok := prev.(int); ok {
			return v + g.Rand.Intn(3) + 1
		}
		if v, ok := prev.(int64); ok {
			return v + int64(g.Rand.Intn(3)+1)
		}
		return g.literalForColumn(schema.Column{Type: t}).Value
	case schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
		if v, ok := prev.(float64); ok {
			return v + float64(g.Rand.Intn(5)+1)
		}
		return g.literalForColumn(schema.Column{Type: t}).Value
	case schema.TypeVarchar, schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return g.literalForColumn(schema.Column{Type: t}).Value
	case schema.TypeBool:
		if v, ok := prev.(int); ok {
			if v == 0 {
				return 1
			}
			return 0
		}
		return g.literalForColumn(schema.Column{Type: t}).Value
	default:
		return prev
	}
}
