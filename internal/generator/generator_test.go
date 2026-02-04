package generator

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"

	"shiro/internal/config"
	"shiro/internal/schema"
)

// TestGeneratorQueryConstraints validates generator invariants to prevent known execution errors.
func TestGeneratorQueryConstraints(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.Joins = true
	cfg.Features.CTE = true
	cfg.Features.Subqueries = true
	cfg.Features.Aggregates = true
	cfg.Features.GroupBy = true
	cfg.Features.Having = true
	cfg.Features.OrderBy = true
	cfg.Features.Distinct = true
	cfg.Features.WindowFuncs = true

	state := schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeVarchar},
					{Name: "c2", Type: schema.TypeDate},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeDouble},
					{Name: "c1", Type: schema.TypeDatetime},
					{Name: "c2", Type: schema.TypeBool},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeDecimal},
					{Name: "c1", Type: schema.TypeTimestamp},
					{Name: "c2", Type: schema.TypeVarchar},
				},
			},
		},
	}

	gen := New(cfg, &state, 7)
	p := parser.New()
	for i := 0; i < 1000; i++ {
		q := gen.GenerateSelectQuery()
		if q == nil {
			continue
		}
		if _, _, err := p.Parse(q.SQLString(), "", ""); err != nil {
			t.Fatalf("parse failed: %v\nsql=%s", err, q.SQLString())
		}
		if q.Having != nil && len(q.GroupBy) == 0 {
			t.Fatalf("having without group by: %s", q.SQLString())
		}
		if q.Having != nil {
			groupSet := exprSet(q.GroupBy)
			if hasNonGroupColumn(q.Having, groupSet) {
				t.Fatalf("having uses non-group column: %s", q.SQLString())
			}
		}
		if len(q.OrderBy) > 0 && gen.queryRequiresSelectOrder(q) {
			items := exprSet(selectItemExprs(q.Items))
			itemCount := len(q.Items)
			for _, ob := range q.OrderBy {
				if items[exprString(ob.Expr)] {
					continue
				}
				if ord, ok := orderByOrdinalExpr(ob.Expr, itemCount); ok {
					if ord >= 1 && ord <= itemCount {
						continue
					}
				}
				t.Fatalf("order by not in select list: %s", q.SQLString())
			}
		}
		checkExprTree(t, gen, q)
	}
}

func TestCreateTablePartitionedSQL(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.PartitionTables = true
	cfg.Weights.Features.PartitionProb = 100
	state := schema.State{}
	gen := New(cfg, &state, 1)
	tbl := gen.GenerateTable()
	tbl.Partitioned = true
	tbl.PartitionCount = 3

	sql := gen.CreateTableSQL(tbl)
	if !strings.Contains(sql, "PARTITION BY HASH") {
		t.Fatalf("expected partition clause, got: %s", sql)
	}
	p := parser.New()
	if _, _, err := p.Parse(sql, "", ""); err != nil {
		t.Fatalf("parse failed: %v\nsql=%s", err, sql)
	}

	tbl.Partitioned = false
	sql = gen.CreateTableSQL(tbl)
	if strings.Contains(sql, "PARTITION BY HASH") {
		t.Fatalf("unexpected partition clause when disabled: %s", sql)
	}
}

func TestAnalyzeQueryFeaturesInSubquery(t *testing.T) {
	query := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}}},
		From:  FromClause{BaseTable: "t0"},
		Where: InExpr{Left: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}, List: []Expr{SubqueryExpr{Query: &SelectQuery{Items: []SelectItem{{Expr: LiteralExpr{Value: 1}}}, From: FromClause{BaseTable: "t1"}}}}},
	}
	features := AnalyzeQueryFeatures(query)
	if !features.HasInSubquery || features.HasNotInSubquery {
		t.Fatalf("expected HasInSubquery only, got in=%v notIn=%v", features.HasInSubquery, features.HasNotInSubquery)
	}

	query.Where = UnaryExpr{Op: "NOT", Expr: query.Where}
	features = AnalyzeQueryFeatures(query)
	if features.HasInSubquery || !features.HasNotInSubquery {
		t.Fatalf("expected HasNotInSubquery only, got in=%v notIn=%v", features.HasInSubquery, features.HasNotInSubquery)
	}
}

func TestAnalyzeQueryFeaturesExistsAndInList(t *testing.T) {
	sub := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}}},
		From:  FromClause{BaseTable: "t1"},
	}
	query := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}}},
		From:  FromClause{BaseTable: "t0"},
		Where: ExistsExpr{Query: sub},
	}
	features := AnalyzeQueryFeatures(query)
	if !features.HasExistsSubquery || features.HasNotExistsSubquery {
		t.Fatalf("expected HasExistsSubquery only, got exists=%v notExists=%v", features.HasExistsSubquery, features.HasNotExistsSubquery)
	}

	query.Where = UnaryExpr{Op: "NOT", Expr: ExistsExpr{Query: sub}}
	features = AnalyzeQueryFeatures(query)
	if features.HasExistsSubquery || !features.HasNotExistsSubquery {
		t.Fatalf("expected HasNotExistsSubquery only, got exists=%v notExists=%v", features.HasExistsSubquery, features.HasNotExistsSubquery)
	}

	query.Where = InExpr{
		Left: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}},
		List: []Expr{LiteralExpr{Value: 1}, LiteralExpr{Value: 2}},
	}
	features = AnalyzeQueryFeatures(query)
	if !features.HasInList || features.HasNotInList {
		t.Fatalf("expected HasInList only, got inList=%v notInList=%v", features.HasInList, features.HasNotInList)
	}

	query.Where = UnaryExpr{Op: "NOT", Expr: query.Where}
	features = AnalyzeQueryFeatures(query)
	if features.HasInList || !features.HasNotInList {
		t.Fatalf("expected HasNotInList only, got inList=%v notInList=%v", features.HasInList, features.HasNotInList)
	}
}

func TestAnalyzeQueryFeaturesWindow(t *testing.T) {
	query := &SelectQuery{
		Items: []SelectItem{{
			Expr: WindowExpr{
				Name:        "ROW_NUMBER",
				PartitionBy: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}},
			},
		}},
		From: FromClause{BaseTable: "t0"},
	}
	features := AnalyzeQueryFeatures(query)
	if !features.HasWindow {
		t.Fatalf("expected HasWindow true")
	}
}

func TestGroupByOrdinalExprBuild(t *testing.T) {
	expr := GroupByOrdinalExpr{
		Ordinal: 2,
		Expr:    ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}},
	}
	var b SQLBuilder
	expr.Build(&b)
	if got := b.String(); got != "2" {
		t.Fatalf("expected ordinal build, got: %s", got)
	}

	expr = GroupByOrdinalExpr{
		Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1"}},
	}
	b = SQLBuilder{}
	expr.Build(&b)
	if got := b.String(); got != "t1.c1" {
		t.Fatalf("expected expr build, got: %s", got)
	}

	assertPanic(t, func() {
		empty := GroupByOrdinalExpr{}
		var b SQLBuilder
		empty.Build(&b)
	})
}

func TestGenerateGroupByMultipleColumns(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	state := schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "c0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
					{Name: "c2", Type: schema.TypeInt},
				},
			},
		},
	}
	gen := New(cfg, &state, 1)
	groupBy := gen.GenerateGroupBy(state.Tables)
	if len(groupBy) != 2 {
		t.Fatalf("expected 2 group by columns, got %d", len(groupBy))
	}
	col0, ok0 := groupBy[0].(ColumnExpr)
	col1, ok1 := groupBy[1].(ColumnExpr)
	if !ok0 || !ok1 {
		t.Fatalf("expected column expressions in group by")
	}
	if col0.Ref.Table == "" || col0.Ref.Name == "" || col1.Ref.Table == "" || col1.Ref.Name == "" {
		t.Fatalf("expected non-empty group by columns")
	}
	if col0.Ref.Table == col1.Ref.Table && col0.Ref.Name == col1.Ref.Name {
		t.Fatalf("expected distinct group by columns")
	}
}

func TestEnsureOrderByDistinctColumns(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	state := schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "c0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
		},
	}
	gen := New(cfg, &state, 1)
	orderBy := []OrderBy{{Expr: LiteralExpr{Value: 1}}}
	out := gen.ensureOrderByDistinctColumns(orderBy, state.Tables)
	if got := orderByDistinctColumns(out); got != 2 {
		t.Fatalf("expected 2 distinct columns, got %d", got)
	}
}

func TestOrderByFromItemsStableUsesOrdinals(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	state := schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "c0", Type: schema.TypeInt},
				},
			},
		},
	}
	gen := New(cfg, &state, 1)
	items := []SelectItem{
		{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "c0"},
		{Expr: LiteralExpr{Value: 1}, Alias: "c1"},
	}
	orderBy := gen.orderByFromItemsStable(items)
	if len(orderBy) < 2 {
		t.Fatalf("expected ordinal order by, got %v", orderBy)
	}
	ord0, ok0 := orderBy[0].Expr.(LiteralExpr)
	ord1, ok1 := orderBy[1].Expr.(LiteralExpr)
	if !ok0 || !ok1 || ord0.Value != 1 || ord1.Value != 2 {
		t.Fatalf("expected ordinals 1,2, got %v", orderBy)
	}
}

func orderByOrdinalExpr(expr Expr, itemCount int) (int, bool) {
	if itemCount <= 0 {
		return 0, false
	}
	lit, ok := expr.(LiteralExpr)
	if !ok {
		return 0, false
	}
	value, ok := literalIntTest(lit.Value)
	if !ok {
		return 0, false
	}
	if value < 1 || value > itemCount {
		return 0, false
	}
	return value, true
}

func literalIntTest(value any) (int, bool) {
	maxInt := int(^uint(0) >> 1)
	switch v := value.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		if v > int64(maxInt) || v < -int64(maxInt)-1 {
			return 0, false
		}
		return int(v), true
	case uint:
		if v > uint(maxInt) {
			return 0, false
		}
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		if v > uint32(maxInt) {
			return 0, false
		}
		return int(v), true
	case uint64:
		if v > uint64(maxInt) {
			return 0, false
		}
		return int(v), true
	default:
		return 0, false
	}
}

func assertPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic")
		}
	}()
	fn()
}

func TestGenerateNonPreparedPlanCacheQuery(t *testing.T) {
	cfg, err := config.Load("../../config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Features.NonPreparedPlanCache = true
	state := schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeDecimal},
					{Name: "c1", Type: schema.TypeDouble},
				},
				Partitioned: true,
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeDecimal},
					{Name: "c1", Type: schema.TypeVarchar},
				},
				Partitioned: false,
			},
		},
	}
	gen := New(cfg, &state, 2)
	pq := gen.GenerateNonPreparedPlanCacheQuery()
	if pq.SQL == "" {
		t.Fatalf("expected non-prepared query")
	}
	if strings.Contains(pq.SQL, "t0") {
		t.Fatalf("expected non-partitioned table only, got: %s", pq.SQL)
	}
	if len(pq.Args) == 0 || len(pq.Args) != len(pq.ArgTypes) {
		t.Fatalf("args/types mismatch: args=%d types=%d", len(pq.Args), len(pq.ArgTypes))
	}
	p := parser.New()
	if _, _, err := p.Parse(pq.SQL, "", ""); err != nil {
		t.Fatalf("parse failed: %v\nsql=%s", err, pq.SQL)
	}
	if strings.Count(pq.SQL, "?") != len(pq.Args) {
		t.Fatalf("placeholder count mismatch: sql=%s args=%d", pq.SQL, len(pq.Args))
	}
}

func selectItemExprs(items []SelectItem) []Expr {
	exprs := make([]Expr, 0, len(items))
	for _, item := range items {
		exprs = append(exprs, item.Expr)
	}
	return exprs
}

func exprSet(exprs []Expr) map[string]bool {
	out := make(map[string]bool, len(exprs))
	for _, expr := range exprs {
		out[exprString(expr)] = true
	}
	return out
}

func hasNonGroupColumn(expr Expr, groupSet map[string]bool) bool {
	switch v := expr.(type) {
	case ColumnExpr:
		return !groupSet[exprString(v)]
	case GroupByOrdinalExpr:
		return hasNonGroupColumn(v.Expr, groupSet)
	case FuncExpr:
		if isAggregateFunc(v.Name) {
			return false
		}
		for _, arg := range v.Args {
			if hasNonGroupColumn(arg, groupSet) {
				return true
			}
		}
		return false
	case BinaryExpr:
		return hasNonGroupColumn(v.Left, groupSet) || hasNonGroupColumn(v.Right, groupSet)
	case UnaryExpr:
		return hasNonGroupColumn(v.Expr, groupSet)
	case CaseExpr:
		for _, w := range v.Whens {
			if hasNonGroupColumn(w.When, groupSet) || hasNonGroupColumn(w.Then, groupSet) {
				return true
			}
		}
		if v.Else != nil {
			return hasNonGroupColumn(v.Else, groupSet)
		}
		return false
	default:
		return false
	}
}

func checkExprTree(t *testing.T, gen *Generator, q *SelectQuery) {
	exprs := queryExprs(q)
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		if err := validateExpr(gen, expr); err != nil {
			t.Fatalf("%v\nsql=%s", err, q.SQLString())
		}
	}
}

func queryExprs(q *SelectQuery) []Expr {
	exprs := make([]Expr, 0, 8)
	for _, item := range q.Items {
		exprs = append(exprs, item.Expr)
	}
	if q.Where != nil {
		exprs = append(exprs, q.Where)
	}
	if q.Having != nil {
		exprs = append(exprs, q.Having)
	}
	exprs = append(exprs, q.GroupBy...)
	for _, ob := range q.OrderBy {
		exprs = append(exprs, ob.Expr)
	}
	for _, join := range q.From.Joins {
		if join.On != nil {
			exprs = append(exprs, join.On)
		}
	}
	return exprs
}

func exprString(expr Expr) string {
	if v, ok := expr.(GroupByOrdinalExpr); ok {
		if v.Expr != nil {
			return exprString(v.Expr)
		}
	}
	var b SQLBuilder
	expr.Build(&b)
	return b.String()
}

func validateExpr(gen *Generator, expr Expr) error {
	switch v := expr.(type) {
	case BinaryExpr:
		if isArithmeticOp(v.Op) {
			if !gen.isNumericExpr(v.Left) || !gen.isNumericExpr(v.Right) {
				return fmt.Errorf("non-numeric arithmetic: %s", exprString(v))
			}
		}
		if isComparisonOp(v.Op) {
			if !typesCompatible(gen, v.Left, v.Right) {
				return fmt.Errorf("type mismatch comparison: %s", exprString(v))
			}
		}
		if err := validateExpr(gen, v.Left); err != nil {
			return err
		}
		return validateExpr(gen, v.Right)
	case UnaryExpr:
		return validateExpr(gen, v.Expr)
	case FuncExpr:
		for _, arg := range v.Args {
			if err := validateExpr(gen, arg); err != nil {
				return err
			}
		}
	case GroupByOrdinalExpr:
		if v.Expr != nil {
			return validateExpr(gen, v.Expr)
		}
	case CaseExpr:
		for _, w := range v.Whens {
			if err := validateExpr(gen, w.When); err != nil {
				return err
			}
			if err := validateExpr(gen, w.Then); err != nil {
				return err
			}
		}
		if v.Else != nil {
			return validateExpr(gen, v.Else)
		}
	}
	return nil
}

func isArithmeticOp(op string) bool {
	return op == "+" || op == "-" || op == "*"
}

func isComparisonOp(op string) bool {
	switch op {
	case "=", "<", ">", "<=", ">=", "!=", "<=>":
		return true
	default:
		return false
	}
}

func typesCompatible(gen *Generator, left, right Expr) bool {
	lt, lok := gen.exprType(left)
	rt, rok := gen.exprType(right)
	if !lok || !rok {
		return true
	}
	if lt == rt {
		return true
	}
	if gen.isNumericType(lt) && gen.isNumericType(rt) {
		return true
	}
	if lt == schema.TypeBool && gen.isNumericType(rt) {
		return true
	}
	if rt == schema.TypeBool && gen.isNumericType(lt) {
		return true
	}
	if isTimeType(lt) && isTimeType(rt) {
		return true
	}
	return false
}

func isTimeType(t schema.ColumnType) bool {
	return t == schema.TypeDate || t == schema.TypeDatetime || t == schema.TypeTimestamp
}
