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

type PreparedQuery struct {
	SQL      string
	Args     []any
	ArgTypes []schema.ColumnType
}

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

func (g *Generator) SetAdaptiveWeights(weights AdaptiveWeights) {
	g.Adaptive = &weights
}

func (g *Generator) ClearAdaptiveWeights() {
	g.Adaptive = nil
}

func (g *Generator) NextTableName() string {
	name := fmt.Sprintf("t%d", g.tableSeq)
	g.tableSeq++
	return name
}

func (g *Generator) NextViewName() string {
	name := fmt.Sprintf("v%d", g.viewSeq)
	g.viewSeq++
	return name
}

func (g *Generator) NextConstraintName(prefix string) string {
	name := fmt.Sprintf("%s_%d", prefix, g.constraintSeq)
	g.constraintSeq++
	return name
}

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

func (g *Generator) CreateViewSQL() string {
	query := g.GenerateSelectQuery()
	if query == nil {
		return ""
	}
	if len(query.With) > 0 {
		query = query.Clone()
		query.With = nil
	}
	viewName := g.NextViewName()
	return fmt.Sprintf("CREATE VIEW %s AS %s", viewName, query.SQLString())
}

func (g *Generator) AddCheckConstraintSQL(tbl schema.Table) string {
	predicate := g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth-1, false, 0)
	name := g.NextConstraintName("chk")
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)", tbl.Name, name, g.exprSQL(predicate))
}

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

func (g *Generator) DeleteSQL(tbl schema.Table) (string, Expr) {
	allowSubquery := g.Config.Features.Subqueries && util.Chance(g.Rand, 20)
	predicate := g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth, allowSubquery, g.maxSubqDepth)
	builder := SQLBuilder{}
	predicate.Build(&builder)
	return fmt.Sprintf("DELETE FROM %s WHERE %s", tbl.Name, builder.String()), predicate
}

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
			query.Having = g.GeneratePredicate(queryTables, g.maxDepth, false, g.maxSubqDepth)
		}
	}

	if g.Config.Features.OrderBy && util.Chance(g.Rand, g.Config.Weights.Features.OrderByProb) {
		query.OrderBy = g.GenerateOrderBy(queryTables)
	}
	if g.Config.Features.Limit && util.Chance(g.Rand, g.Config.Weights.Features.LimitProb) {
		limit := g.Rand.Intn(20) + 1
		query.Limit = &limit
	}

	queryFeatures := AnalyzeQueryFeatures(query)
	g.LastFeatures = &queryFeatures
	return query
}

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
			return pq
		}
	}
	return PreparedQuery{}
}

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

func (g *Generator) GenerateCTEQuery(tbl schema.Table) *SelectQuery {
	query := &SelectQuery{}
	query.Items = g.GenerateCTESelectList(tbl)
	query.From = FromClause{BaseTable: tbl.Name}
	query.Where = g.GeneratePredicate([]schema.Table{tbl}, g.maxDepth-1, false, g.maxSubqDepth)
	limit := g.Rand.Intn(10) + 1
	query.Limit = &limit
	return query
}

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

func (g *Generator) GenerateSelectList(tables []schema.Table) []SelectItem {
	count := g.Rand.Intn(3) + 1
	items := make([]SelectItem, 0, count)
	for i := 0; i < count; i++ {
		expr := g.GenerateSelectExpr(tables, g.maxDepth)
		items = append(items, SelectItem{Expr: expr, Alias: fmt.Sprintf("c%d", i)})
	}
	return items
}

func (g *Generator) GenerateSelectExpr(tables []schema.Table, depth int) Expr {
	if g.Config.Features.WindowFuncs && util.Chance(g.Rand, g.Config.Weights.Features.WindowProb) {
		return g.GenerateWindowExpr(tables)
	}
	return g.GenerateScalarExpr(tables, depth, g.Config.Features.Subqueries)
}

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

func (g *Generator) GenerateAggregateSelectList(tables []schema.Table, withGroupBy bool) []SelectItem {
	items := make([]SelectItem, 0, 3)
	items = append(items, SelectItem{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"})
	items = append(items, SelectItem{Expr: FuncExpr{Name: "SUM", Args: []Expr{g.GenerateNumericExpr(tables)}}, Alias: "sum1"})
	if withGroupBy {
		items = append(items, SelectItem{Expr: g.GenerateScalarExpr(tables, g.maxDepth-1, false), Alias: "g1"})
	}
	return items
}

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

func (g *Generator) GenerateGroupBy(tables []schema.Table) []Expr {
	col := g.randomColumn(tables)
	if col.Table == "" {
		return nil
	}
	return []Expr{ColumnExpr{Ref: col}}
}

func (g *Generator) GenerateOrderBy(tables []schema.Table) []OrderBy {
	count := g.Rand.Intn(2) + 1
	items := make([]OrderBy, 0, count)
	for i := 0; i < count; i++ {
		expr := g.GenerateScalarExpr(tables, g.maxDepth, false)
		items = append(items, OrderBy{Expr: expr, Desc: util.Chance(g.Rand, 50)})
	}
	return items
}

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
			expr := Expr(InExpr{Left: left, List: []Expr{SubqueryExpr{Query: sub}}})
			if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
				return UnaryExpr{Op: "NOT", Expr: expr}
			}
			return expr
		}
	}
	if depth <= 0 {
		left := g.generateScalarExpr(tables, 0, allowSubquery, subqDepth)
		right := g.generateScalarExpr(tables, 0, allowSubquery, subqDepth)
		return BinaryExpr{Left: left, Op: g.pickComparison(), Right: right}
	}
	if util.Chance(g.Rand, 20) {
		left := g.generateScalarExpr(tables, depth-1, false, 0)
		listSize := g.Rand.Intn(3) + 1
		list := make([]Expr, 0, listSize)
		for i := 0; i < listSize; i++ {
			list = append(list, g.randomLiteralExpr())
		}
		expr := Expr(InExpr{Left: left, List: list})
		if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
			return UnaryExpr{Op: "NOT", Expr: expr}
		}
		return expr
	}
	choice := g.Rand.Intn(3)
	if choice == 0 {
		left := g.generateScalarExpr(tables, depth-1, allowSubquery, subqDepth)
		right := g.generateScalarExpr(tables, depth-1, allowSubquery, subqDepth)
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

func (g *Generator) GenerateScalarExpr(tables []schema.Table, depth int, allowSubquery bool) Expr {
	return g.generateScalarExpr(tables, depth, allowSubquery, g.maxSubqDepth)
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
		left := g.GenerateScalarExpr(tables, depth-1, allowSubquery)
		right := g.GenerateScalarExpr(tables, depth-1, allowSubquery)
		return BinaryExpr{Left: left, Op: g.pickArithmetic(), Right: right}
	case 3:
		return FuncExpr{Name: g.pickFunc(), Args: []Expr{g.GenerateScalarExpr(tables, depth-1, allowSubquery)}}
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
	names := []string{}
	for _, ltbl := range left {
		for _, lcol := range ltbl.Columns {
			for _, rcol := range right.Columns {
				if lcol.Name == rcol.Name && lcol.Type == rcol.Type {
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
	col := tbl.Columns[g.Rand.Intn(len(tbl.Columns))]
	arg := g.literalForColumn(col).Value
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", col.Name, tbl.Name, col.Name)
	return PreparedQuery{SQL: query, Args: []any{arg}, ArgTypes: []schema.ColumnType{col.Type}}
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
	leftArg := g.literalForColumn(leftParam).Value
	rightArg := g.literalForColumn(rightParam).Value
	query := fmt.Sprintf(
		"SELECT %s.%s, %s.%s FROM %s JOIN %s ON %s.%s = %s.%s WHERE %s.%s = ? AND %s.%s = ?",
		leftTbl.Name, leftParam.Name,
		rightTbl.Name, rightParam.Name,
		leftTbl.Name, rightTbl.Name,
		leftTbl.Name, leftJoin.Name, rightTbl.Name, rightJoin.Name,
		leftTbl.Name, leftParam.Name,
		rightTbl.Name, rightParam.Name,
	)
	return PreparedQuery{
		SQL:      query,
		Args:     []any{leftArg, rightArg},
		ArgTypes: []schema.ColumnType{leftParam.Type, rightParam.Type},
	}
}

func (g *Generator) preparedAggregateQuery() PreparedQuery {
	tbl := g.State.Tables[g.Rand.Intn(len(g.State.Tables))]
	if len(tbl.Columns) == 0 {
		return PreparedQuery{}
	}
	col := tbl.Columns[g.Rand.Intn(len(tbl.Columns))]
	arg := g.literalForColumn(col).Value
	selectSQL := "COUNT(*) AS cnt"
	if g.isNumericType(col.Type) && util.Chance(g.Rand, 50) {
		selectSQL = "COUNT(*) AS cnt, SUM(" + col.Name + ") AS sum1"
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", selectSQL, tbl.Name, col.Name)
	return PreparedQuery{SQL: query, Args: []any{arg}, ArgTypes: []schema.ColumnType{col.Type}}
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
		"WITH cte AS (SELECT %s, %s FROM %s WHERE %s = ?) SELECT %s FROM cte WHERE %s = ?",
		col1.Name, col2.Name, tbl.Name, col1.Name, col2.Name, col2.Name,
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
