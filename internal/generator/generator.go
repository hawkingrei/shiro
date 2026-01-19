package generator

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"shiro/internal/config"
	"shiro/internal/schema"
	"shiro/internal/util"
)

// Generator creates SQL statements based on schema state.
type Generator struct {
	Rand          *rand.Rand
	Config        config.Config
	State         *schema.State
	Adaptive      *AdaptiveWeights
	LastFeatures  *QueryFeatures
	tableSeq      int
	viewSeq       int
	indexSeq      int
	constraintSeq int
	maxDepth      int
	maxSubqDepth  int
}

// PreparedQuery holds a prepared statement and args.
type PreparedQuery struct {
	SQL      string
	Args     []any
	ArgTypes []schema.ColumnType
}

// maxPreparedParams caps parameters for prepared statements.
// This keeps generated queries readable and avoids driver/engine limits.
const maxPreparedParams = 8

const (
	preparedExtraPredicateProb = 60
	preparedAggExtraProb       = 50
)

// New constructs a Generator with a seed.
func New(cfg config.Config, state *schema.State, seed int64) *Generator {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Generator{
		Rand:         rand.New(rand.NewSource(seed)),
		Config:       cfg,
		State:        state,
		maxDepth:     3,
		maxSubqDepth: 2,
	}
}

// SetAdaptiveWeights overrides feature weights for adaptive sampling.
func (g *Generator) SetAdaptiveWeights(weights AdaptiveWeights) {
	g.Adaptive = &weights
}

// ClearAdaptiveWeights disables adaptive sampling overrides.
func (g *Generator) ClearAdaptiveWeights() {
	g.Adaptive = nil
}

// NextTableName returns a unique table name.
func (g *Generator) NextTableName() string {
	name := fmt.Sprintf("t%d", g.tableSeq)
	g.tableSeq++
	return name
}

// NextViewName returns a unique view name.
func (g *Generator) NextViewName() string {
	name := fmt.Sprintf("v%d", g.viewSeq)
	g.viewSeq++
	return name
}

// NextConstraintName returns a unique constraint name with a prefix.
func (g *Generator) NextConstraintName(prefix string) string {
	name := fmt.Sprintf("%s_%d", prefix, g.constraintSeq)
	g.constraintSeq++
	return name
}

// GenerateTable creates a random table schema with columns and indexes.
func (g *Generator) GenerateTable() schema.Table {
	colCount := g.Rand.Intn(g.Config.MaxColumns-2) + 2
	cols := make([]schema.Column, 0, colCount+1)
	cols = append(cols, schema.Column{Name: "id", Type: schema.TypeBigInt, Nullable: false})

	for i := 0; i < colCount; i++ {
		col := schema.Column{
			Name:     fmt.Sprintf("c%d", i),
			Type:     g.randomColumnType(),
			Nullable: util.Chance(g.Rand, 20),
			HasIndex: util.Chance(g.Rand, 30),
		}
		cols = append(cols, col)
	}

	return schema.Table{
		Name:    g.NextTableName(),
		Columns: cols,
		HasPK:   true,
		NextID:  1,
	}
}

// CreateTableSQL renders a CREATE TABLE statement for a schema table.
func (g *Generator) CreateTableSQL(tbl schema.Table) string {
	parts := make([]string, 0, len(tbl.Columns)+2)
	for _, col := range tbl.Columns {
		line := fmt.Sprintf("%s %s", col.Name, col.SQLType())
		if !col.Nullable {
			line += " NOT NULL"
		}
		parts = append(parts, line)
	}
	if tbl.HasPK {
		parts = append(parts, "PRIMARY KEY (id)")
	}
	for _, col := range tbl.Columns {
		if col.HasIndex {
			parts = append(parts, fmt.Sprintf("INDEX idx_%s (%s)", col.Name, col.Name))
		}
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", tbl.Name, strings.Join(parts, ", "))
}

// CreateIndexSQL emits a CREATE INDEX statement and updates table metadata.
func (g *Generator) CreateIndexSQL(tbl *schema.Table) (string, bool) {
	candidates := make([]*schema.Column, 0, len(tbl.Columns))
	for i := range tbl.Columns {
		col := &tbl.Columns[i]
		if col.HasIndex {
			continue
		}
		candidates = append(candidates, col)
	}
	if len(candidates) == 0 {
		return "", false
	}
	col := candidates[g.Rand.Intn(len(candidates))]
	col.HasIndex = true
	indexName := fmt.Sprintf("idx_%s_%d", col.Name, g.indexSeq)
	g.indexSeq++
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s)", indexName, tbl.Name, col.Name), true
}

// CreateViewSQL emits a CREATE VIEW statement from a generated query.
func (g *Generator) CreateViewSQL() string {
	query := g.GenerateSelectQuery()
	if query == nil {
		return ""
	}
	if len(query.With) > 0 {
		cteEnabled := g.Config.Features.CTE
		g.Config.Features.CTE = false
		query = g.GenerateSelectQuery()
		g.Config.Features.CTE = cteEnabled
		if query == nil {
			return ""
		}
	}
	query = query.Clone()
	query.Items = ensureUniqueAliases(query.Items)
	viewName := g.NextViewName()
	return fmt.Sprintf("CREATE VIEW %s AS %s", viewName, query.SQLString())
}

func ensureUniqueAliases(items []SelectItem) []SelectItem {
	used := map[string]int{}
	out := make([]SelectItem, len(items))
	for i, item := range items {
		base := strings.TrimSpace(item.Alias)
		if base == "" {
			if col, ok := item.Expr.(ColumnExpr); ok {
				base = col.Ref.Name
			} else {
				base = fmt.Sprintf("c%d", i)
			}
		}
		if count, ok := used[base]; ok {
			count++
			used[base] = count
			item.Alias = fmt.Sprintf("%s_%d", base, count)
		} else {
			used[base] = 0
			item.Alias = base
		}
		out[i] = item
	}
	return out
}

// AddCheckConstraintSQL emits a CHECK constraint for a table.
func (g *Generator) AddCheckConstraintSQL(tbl schema.Table) string {
	predicate := g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth-1, false, 0)
	name := g.NextConstraintName("chk")
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)", tbl.Name, name, g.exprSQL(predicate))
}

// AddForeignKeySQL emits a FOREIGN KEY constraint when possible.
func (g *Generator) AddForeignKeySQL(state *schema.State) string {
	if state == nil || len(state.Tables) < 2 {
		return ""
	}
	child := state.Tables[g.Rand.Intn(len(state.Tables))]
	parent := state.Tables[g.Rand.Intn(len(state.Tables))]
	if child.Name == parent.Name {
		return ""
	}
	childCol, parentCol := g.pickForeignKeyColumns(child, parent)
	if childCol.Name == "" || parentCol.Name == "" {
		return ""
	}
	name := g.NextConstraintName("fk")
	return fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
		child.Name, name, childCol.Name, parent.Name, parentCol.Name,
	)
}

// InsertSQL emits an INSERT statement and advances auto IDs.
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

// GenerateSelectQuery builds a randomized SELECT query for current schema.
func (g *Generator) GenerateSelectQuery() *SelectQuery {
	baseTables := g.pickTables()
	if len(baseTables) == 0 {
		return nil
	}

	query := &SelectQuery{}
	queryTables := append([]schema.Table{}, baseTables...)

	if g.Config.Features.CTE && util.Chance(g.Rand, g.Config.Weights.Features.CTECount*10) {
		cteCount := g.Rand.Intn(2) + 1
		for i := 0; i < cteCount; i++ {
			cteBase := baseTables[g.Rand.Intn(len(baseTables))]
			cteQuery := g.GenerateCTEQuery(cteBase)
			cteName := fmt.Sprintf("cte_%d", i)
			query.With = append(query.With, CTE{Name: cteName, Query: cteQuery})
			queryTables = append(queryTables, schema.Table{Name: cteName, Columns: cteBase.Columns})
		}
	}

	query.From = g.buildFromClause(queryTables)
	query.Items = g.GenerateSelectList(queryTables)

	if util.Chance(g.Rand, g.Config.Weights.Features.DistinctProb) && g.Config.Features.Distinct {
		query.Distinct = true
	}

	query.Where = g.GeneratePredicate(queryTables, g.maxDepth, g.Config.Features.Subqueries, g.maxSubqDepth)

	if g.Config.Features.Aggregates && util.Chance(g.Rand, g.aggProb()) {
		withGroupBy := g.Config.Features.GroupBy && util.Chance(g.Rand, g.Config.Weights.Features.GroupByProb)
		if withGroupBy {
			query.GroupBy = g.GenerateGroupBy(queryTables)
		}
		query.Items = g.GenerateAggregateSelectList(queryTables, len(query.GroupBy) > 0)
		if g.Config.Features.Having && len(query.GroupBy) > 0 && util.Chance(g.Rand, g.Config.Weights.Features.HavingProb) {
			query.Having = g.GenerateHavingPredicate(query.GroupBy, queryTables)
		}
	}

	if g.Config.Features.OrderBy && util.Chance(g.Rand, g.Config.Weights.Features.OrderByProb) {
		if query.Distinct {
			query.OrderBy = g.GenerateOrderByFromItems(query.Items)
		} else {
			query.OrderBy = g.GenerateOrderBy(queryTables)
		}
	}
	if g.Config.Features.Limit && util.Chance(g.Rand, g.Config.Weights.Features.LimitProb) {
		limit := g.Rand.Intn(20) + 1
		query.Limit = &limit
	}

	queryFeatures := AnalyzeQueryFeatures(query)
	g.LastFeatures = &queryFeatures
	return query
}

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
func (g *Generator) GenerateCTEQuery(tbl schema.Table) *SelectQuery {
	query := &SelectQuery{}
	query.Items = g.GenerateCTESelectList(tbl)
	query.From = FromClause{BaseTable: tbl.Name}
	query.Where = g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth-1, false, g.maxSubqDepth)
	limit := g.Rand.Intn(10) + 1
	query.Limit = &limit
	return query
}

// GenerateCTESelectList builds a small SELECT list for CTEs.
func (g *Generator) GenerateCTESelectList(tbl schema.Table) []SelectItem {
	if len(tbl.Columns) == 0 {
		return nil
	}
	count := min(3, len(tbl.Columns))
	perm := g.Rand.Perm(len(tbl.Columns))[:count]
	items := make([]SelectItem, 0, count)
	for i, idx := range perm {
		col := tbl.Columns[idx]
		items = append(items, SelectItem{Expr: ColumnExpr{Ref: ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}}, Alias: fmt.Sprintf("c%d", i)})
	}
	return items
}

// GenerateSelectList builds a SELECT list for the given tables.
func (g *Generator) GenerateSelectList(tables []schema.Table) []SelectItem {
	count := g.Rand.Intn(3) + 1
	items := make([]SelectItem, 0, count)
	for i := 0; i < count; i++ {
		expr := g.GenerateSelectExpr(tables, g.maxDepth)
		items = append(items, SelectItem{Expr: expr, Alias: fmt.Sprintf("c%d", i)})
	}
	return items
}

// GenerateSelectExpr builds a scalar or window expression for SELECT.
func (g *Generator) GenerateSelectExpr(tables []schema.Table, depth int) Expr {
	if g.Config.Features.WindowFuncs && util.Chance(g.Rand, g.Config.Weights.Features.WindowProb) {
		return g.GenerateWindowExpr(tables)
	}
	return g.GenerateScalarExpr(tables, depth, g.Config.Features.Subqueries)
}

// GenerateWindowExpr builds a window function expression.
func (g *Generator) GenerateWindowExpr(tables []schema.Table) Expr {
	funcs := []string{"ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "AVG"}
	name := funcs[g.Rand.Intn(len(funcs))]
	var args []Expr
	if name == "SUM" || name == "AVG" {
		args = []Expr{g.GenerateNumericExpr(tables)}
	}
	partitionBy := []Expr{}
	if util.Chance(g.Rand, 50) {
		col := g.randomColumn(tables)
		if col.Table != "" {
			partitionBy = append(partitionBy, ColumnExpr{Ref: col})
		}
	}
	orderCol := g.randomColumn(tables)
	orderExpr := Expr(LiteralExpr{Value: 1})
	if orderCol.Table != "" {
		orderExpr = ColumnExpr{Ref: orderCol}
	}
	orderBy := []OrderBy{{Expr: orderExpr, Desc: util.Chance(g.Rand, 50)}}
	return WindowExpr{
		Name:        name,
		Args:        args,
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
	}
}

// GenerateAggregateSelectList builds a SELECT list with aggregates.
func (g *Generator) GenerateAggregateSelectList(tables []schema.Table, withGroupBy bool) []SelectItem {
	items := make([]SelectItem, 0, 3)
	items = append(items, SelectItem{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"})
	items = append(items, SelectItem{Expr: FuncExpr{Name: "SUM", Args: []Expr{g.GenerateNumericExpr(tables)}}, Alias: "sum1"})
	if withGroupBy {
		items = append(items, SelectItem{Expr: g.GenerateScalarExpr(tables, g.maxDepth-1, false), Alias: "g1"})
	}
	return items
}

// GenerateNumericExpr returns a numeric expression or literal.
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
func (g *Generator) GenerateGroupBy(tables []schema.Table) []Expr {
	col := g.randomColumn(tables)
	if col.Table == "" {
		return nil
	}
	return []Expr{ColumnExpr{Ref: col}}
}

// GenerateOrderBy builds an ORDER BY list.
func (g *Generator) GenerateOrderBy(tables []schema.Table) []OrderBy {
	count := g.Rand.Intn(2) + 1
	items := make([]OrderBy, 0, count)
	for i := 0; i < count; i++ {
		expr := g.GenerateScalarExpr(tables, g.maxDepth, false)
		if _, ok := expr.(LiteralExpr); ok {
			col := g.randomColumn(tables)
			if col.Table != "" {
				expr = ColumnExpr{Ref: col}
			}
		}
		items = append(items, OrderBy{Expr: expr, Desc: util.Chance(g.Rand, 50)})
	}
	return items
}

// GenerateOrderByFromItems uses SELECT-list expressions for ORDER BY.
func (g *Generator) GenerateOrderByFromItems(items []SelectItem) []OrderBy {
	if len(items) == 0 {
		return nil
	}
	count := 1
	if len(items) > 1 && util.Chance(g.Rand, 40) {
		count = 2
	}
	idxs := g.Rand.Perm(len(items))[:count]
	orders := make([]OrderBy, 0, count)
	for _, idx := range idxs {
		orders = append(orders, OrderBy{Expr: items[idx].Expr, Desc: util.Chance(g.Rand, 50)})
	}
	return orders
}

// GeneratePredicate builds a boolean predicate expression.
func (g *Generator) GeneratePredicate(tables []schema.Table, depth int, allowSubquery bool, subqDepth int) Expr {
	if allowSubquery && subqDepth > 0 && util.Chance(g.Rand, g.subqCount()*5) {
		sub := g.GenerateSubquery(tables, subqDepth-1)
		if sub != nil {
			if util.Chance(g.Rand, 50) {
				expr := Expr(ExistsExpr{Query: sub})
				if g.Config.Features.NotExists && util.Chance(g.Rand, g.Config.Weights.Features.NotExistsProb) {
					return UnaryExpr{Op: "NOT", Expr: expr}
				}
				return expr
			}
			left := g.generateScalarExpr(tables, 0, false, 0)
			if !g.isNumericExpr(left) {
				left = g.GenerateNumericExpr(tables)
			}
			expr := Expr(InExpr{Left: left, List: []Expr{SubqueryExpr{Query: sub}}})
			if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
				return UnaryExpr{Op: "NOT", Expr: expr}
			}
			return expr
		}
	}
	if depth <= 0 {
		left, right := g.generateComparablePair(tables, allowSubquery, subqDepth)
		return BinaryExpr{Left: left, Op: g.pickComparison(), Right: right}
	}
	if util.Chance(g.Rand, 20) {
		leftExpr, colType := g.pickComparableExpr(tables)
		listSize := g.Rand.Intn(3) + 1
		list := make([]Expr, 0, listSize)
		for i := 0; i < listSize; i++ {
			list = append(list, g.literalForColumn(schema.Column{Type: colType}))
		}
		expr := Expr(InExpr{Left: leftExpr, List: list})
		if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
			return UnaryExpr{Op: "NOT", Expr: expr}
		}
		return expr
	}
	choice := g.Rand.Intn(3)
	if choice == 0 {
		left, right := g.generateComparablePair(tables, allowSubquery, subqDepth)
		return BinaryExpr{Left: left, Op: g.pickComparison(), Right: right}
	}
	left := g.GeneratePredicate(tables, depth-1, allowSubquery, subqDepth)
	right := g.GeneratePredicate(tables, depth-1, allowSubquery, subqDepth)
	op := "AND"
	if util.Chance(g.Rand, 30) {
		op = "OR"
	}
	return BinaryExpr{Left: left, Op: op, Right: right}
}

// GenerateHavingPredicate builds a HAVING predicate from group-by expressions and aggregates.
func (g *Generator) GenerateHavingPredicate(groupBy []Expr, tables []schema.Table) Expr {
	candidates := make([]Expr, 0, len(groupBy)+2)
	candidates = append(candidates, groupBy...)
	candidates = append(candidates, FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}})
	candidates = append(candidates, FuncExpr{Name: "SUM", Args: []Expr{g.GenerateNumericExpr(tables)}})
	expr := candidates[g.Rand.Intn(len(candidates))]
	if colType, ok := g.exprType(expr); ok {
		return BinaryExpr{Left: expr, Op: g.pickComparison(), Right: g.literalForColumn(schema.Column{Type: colType})}
	}
	return BinaryExpr{Left: expr, Op: g.pickComparison(), Right: g.randomLiteralExpr()}
}

// GenerateScalarExpr builds a scalar expression with bounded depth.
func (g *Generator) GenerateScalarExpr(tables []schema.Table, depth int, allowSubquery bool) Expr {
	return g.generateScalarExpr(tables, depth, allowSubquery, g.maxSubqDepth)
}

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
func (g *Generator) GenerateSubquery(outerTables []schema.Table, subqDepth int) *SelectQuery {
	if len(g.State.Tables) == 0 {
		return nil
	}
	inner := g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
	query := &SelectQuery{
		Items: []SelectItem{
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
		},
		From: FromClause{BaseTable: inner.Name},
	}

	if g.Config.Features.CorrelatedSubq && len(outerTables) > 0 && util.Chance(g.Rand, 40) {
		outerCol := g.randomColumn(outerTables)
		innerCol := g.pickColumnByType(inner, outerCol.Type)
		if outerCol.Table != "" && innerCol.Name != "" {
			query.Where = BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: inner.Name, Name: innerCol.Name, Type: innerCol.Type}},
				Op:    "=",
				Right: ColumnExpr{Ref: outerCol},
			}
			return query
		}
	}

	query.Where = g.GeneratePredicate([]schema.Table{inner}, 1, false, subqDepth)
	return query
}

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

func (g *Generator) pickTables() []schema.Table {
	if len(g.State.Tables) == 0 {
		return nil
	}
	max := len(g.State.Tables)
	count := 1
	if g.Config.Features.Joins && max > 1 {
		limit := min(max, g.Config.MaxJoinTables)
		count = g.Rand.Intn(min(limit, g.joinCount()+1)) + 1
	}
	idxs := g.Rand.Perm(max)[:count]
	picked := make([]schema.Table, 0, count)
	for _, idx := range idxs {
		picked = append(picked, g.State.Tables[idx])
	}
	return picked
}

func (g *Generator) joinCount() int {
	if g.Adaptive != nil && g.Adaptive.JoinCount > 0 {
		return g.Adaptive.JoinCount
	}
	return g.Config.Weights.Features.JoinCount
}

func (g *Generator) subqCount() int {
	if g.Adaptive != nil && g.Adaptive.SubqCount >= 0 {
		return g.Adaptive.SubqCount
	}
	return g.Config.Weights.Features.SubqCount
}

func (g *Generator) aggProb() int {
	if g.Adaptive != nil && g.Adaptive.AggProb >= 0 {
		return g.Adaptive.AggProb
	}
	return g.Config.Weights.Features.AggProb
}

func (g *Generator) buildFromClause(tables []schema.Table) FromClause {
	if len(tables) == 0 {
		return FromClause{}
	}
	from := FromClause{BaseTable: tables[0].Name}
	if len(tables) == 1 || !g.Config.Features.Joins {
		return from
	}
	for i := 1; i < len(tables); i++ {
		joinType := JoinInner
		switch g.Rand.Intn(4) {
		case 0:
			joinType = JoinInner
		case 1:
			joinType = JoinLeft
		case 2:
			joinType = JoinRight
		case 3:
			joinType = JoinCross
		}
		join := Join{Type: joinType, Table: tables[i].Name}
		if joinType != JoinCross {
			using := g.pickUsingColumns(tables[:i], tables[i])
			if len(using) > 0 && util.Chance(g.Rand, 50) {
				join.Using = using
			} else {
				join.On = g.joinCondition(tables[:i], tables[i])
			}
		}
		from.Joins = append(from.Joins, join)
	}
	return from
}

func (g *Generator) randomColumn(tables []schema.Table) ColumnRef {
	if len(tables) == 0 {
		return ColumnRef{}
	}
	bl := tables[g.Rand.Intn(len(tables))]
	if len(bl.Columns) == 0 {
		return ColumnRef{}
	}
	col := bl.Columns[g.Rand.Intn(len(bl.Columns))]
	return ColumnRef{Table: bl.Name, Name: col.Name, Type: col.Type}
}

func (g *Generator) pickUsingColumns(left []schema.Table, right schema.Table) []string {
	leftCounts := map[string]int{}
	leftTypes := map[string]schema.ColumnType{}
	for _, ltbl := range left {
		for _, lcol := range ltbl.Columns {
			leftCounts[lcol.Name]++
			if _, ok := leftTypes[lcol.Name]; !ok {
				leftTypes[lcol.Name] = lcol.Type
			}
		}
	}
	names := []string{}
	for _, ltbl := range left {
		for _, lcol := range ltbl.Columns {
			for _, rcol := range right.Columns {
				if lcol.Name == rcol.Name && lcol.Type == rcol.Type && leftCounts[lcol.Name] == 1 && leftTypes[lcol.Name] == lcol.Type {
					names = append(names, lcol.Name)
				}
			}
		}
	}
	if len(names) == 0 {
		return nil
	}
	count := 1
	if len(names) > 1 && util.Chance(g.Rand, 30) {
		count = 2
	}
	g.Rand.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	return names[:count]
}

func (g *Generator) joinCondition(left []schema.Table, right schema.Table) Expr {
	leftCols := g.collectColumns(left)
	rightCols := g.collectColumns([]schema.Table{right})
	if len(leftCols) == 0 || len(rightCols) == 0 {
		return g.GeneratePredicate(append(left, right), g.maxDepth-1, false, g.maxSubqDepth)
	}
	for i := 0; i < 4; i++ {
		l := leftCols[g.Rand.Intn(len(leftCols))]
		r := rightCols[g.Rand.Intn(len(rightCols))]
		if l.Type == r.Type {
			return BinaryExpr{Left: ColumnExpr{Ref: l}, Op: "=", Right: ColumnExpr{Ref: r}}
		}
	}
	return g.GeneratePredicate(append(left, right), g.maxDepth-1, false, g.maxSubqDepth)
}

func (g *Generator) collectColumns(tables []schema.Table) []ColumnRef {
	cols := make([]ColumnRef, 0, 8)
	for _, tbl := range tables {
		for _, col := range tbl.Columns {
			cols = append(cols, ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type})
		}
	}
	return cols
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

func (g *Generator) preparedSingleTable() PreparedQuery {
	tbl := g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
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
	tbl := g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
	if len(tbl.Columns) == 0 {
		return PreparedQuery{}
	}
	cols := make([]schema.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		if col.Name == "id" {
			continue
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		return PreparedQuery{}
	}
	col := cols[g.Rand.Intn(len(cols))]
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
