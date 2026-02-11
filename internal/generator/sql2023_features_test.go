package generator

import (
	"math/rand"
	"strings"
	"testing"

	"shiro/internal/schema"
)

func TestBuildWithRecursiveCTE(t *testing.T) {
	cteBody := &SelectQuery{
		Items: []SelectItem{{Expr: LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  FromClause{BaseTable: "t0"},
	}
	query := &SelectQuery{
		WithRecursive: true,
		With:          []CTE{{Name: "c", Query: cteBody}},
		Items:         []SelectItem{{Expr: ColumnExpr{Ref: ColumnRef{Table: "c", Name: "c0"}}, Alias: "c0"}},
		From:          FromClause{BaseTable: "c"},
	}
	sql := query.SQLString()
	if !strings.HasPrefix(sql, "WITH RECURSIVE c AS") {
		t.Fatalf("expected WITH RECURSIVE prefix, got %s", sql)
	}
	features := AnalyzeQueryFeatures(query)
	if !features.HasRecursiveCTE {
		t.Fatalf("expected recursive CTE feature flag")
	}
}

func TestBuildNaturalJoin(t *testing.T) {
	query := &SelectQuery{
		Items: []SelectItem{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id"}}, Alias: "id"}},
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{{
				Type:    JoinLeft,
				Natural: true,
				Table:   "t1",
				Using:   []string{"id"},
			}},
		},
	}
	sql := query.SQLString()
	if !strings.Contains(sql, "NATURAL LEFT JOIN t1") {
		t.Fatalf("expected NATURAL LEFT JOIN, got %s", sql)
	}
	if strings.Contains(sql, "USING (") || strings.Contains(sql, " ON ") {
		t.Fatalf("natural join should not emit USING/ON: %s", sql)
	}
	features := AnalyzeQueryFeatures(query)
	if !features.HasNaturalJoin {
		t.Fatalf("expected natural join feature flag")
	}
}

func TestBuildTableAliases(t *testing.T) {
	query := &SelectQuery{
		Items: []SelectItem{{Expr: ColumnExpr{Ref: ColumnRef{Table: "b", Name: "id"}}, Alias: "id"}},
		From: FromClause{
			BaseTable: "t0",
			BaseAlias: "b",
			Joins: []Join{{
				Type:       JoinInner,
				Table:      "t1",
				TableAlias: "j",
				On: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "b", Name: "id"}},
					Op:    "=",
					Right: ColumnExpr{Ref: ColumnRef{Table: "j", Name: "id"}},
				},
			}},
		},
	}
	sql := query.SQLString()
	if !strings.Contains(sql, "FROM t0 AS b") {
		t.Fatalf("expected base table alias in SQL, got %s", sql)
	}
	if !strings.Contains(sql, "JOIN t1 AS j") {
		t.Fatalf("expected join table alias in SQL, got %s", sql)
	}
}

func TestBuildWindowNamedAndFrame(t *testing.T) {
	query := &SelectQuery{
		Items: []SelectItem{{
			Expr: WindowExpr{
				Name:       "SUM",
				Args:       []Expr{ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}},
				WindowName: "w0",
			},
			Alias: "w",
		}},
		From: FromClause{BaseTable: "t0"},
		WindowDefs: []WindowDef{{
			Name:        "w0",
			PartitionBy: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id"}}},
			OrderBy:     []OrderBy{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}}},
			Frame:       &WindowFrame{Unit: "ROWS", Start: "UNBOUNDED PRECEDING", End: "CURRENT ROW"},
		}},
	}
	sql := query.SQLString()
	if !strings.Contains(sql, "OVER w0") {
		t.Fatalf("expected named window reference, got %s", sql)
	}
	if !strings.Contains(sql, "WINDOW w0 AS (PARTITION BY t0.id ORDER BY t0.c0 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)") {
		t.Fatalf("expected WINDOW definition with frame, got %s", sql)
	}
}

func TestBuildGroupByWithRollup(t *testing.T) {
	query := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}, Alias: "c0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
		},
		From:              FromClause{BaseTable: "t0"},
		GroupBy:           []Expr{ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}},
		GroupByWithRollup: true,
	}
	sql := query.SQLString()
	if !strings.Contains(sql, "GROUP BY t0.c0 WITH ROLLUP") {
		t.Fatalf("expected GROUP BY ... WITH ROLLUP, got %s", sql)
	}
}

func TestIntervalExprBuild(t *testing.T) {
	expr := BinaryExpr{
		Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "d0"}},
		Op:    "+",
		Right: IntervalExpr{Value: 2, Unit: "day"},
	}
	b := SQLBuilder{}
	expr.Build(&b)
	if got := b.String(); got != "(t0.d0 + INTERVAL 2 DAY)" {
		t.Fatalf("unexpected interval expression: %s", got)
	}
}

func TestApplyFullJoinEmulation(t *testing.T) {
	gen := &Generator{Rand: rand.New(rand.NewSource(1))}
	query := &SelectQuery{
		Items: []SelectItem{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id"}}, Alias: "id"}},
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{{
				Type:  JoinInner,
				Table: "t1",
				Using: []string{"id"},
			}},
		},
	}
	if ok := gen.applyFullJoinEmulation(query); !ok {
		t.Fatalf("expected full join emulation to apply")
	}
	features := AnalyzeQueryFeatures(query)
	if !features.HasFullJoinEmulation {
		t.Fatalf("expected full join emulation feature flag")
	}
	if query.From.Joins[0].Type != JoinLeft {
		t.Fatalf("expected left branch join type LEFT JOIN, got %s", query.From.Joins[0].Type)
	}
	if len(query.SetOps) != 1 || !query.SetOps[0].All || query.SetOps[0].Type != SetOperationUnion {
		t.Fatalf("expected UNION ALL branch after emulation")
	}
	rhs := query.SetOps[0].Query
	if rhs == nil || len(rhs.From.Joins) != 1 || rhs.From.Joins[0].Type != JoinRight {
		t.Fatalf("expected right-join rhs branch")
	}
	sql := query.SQLString()
	if !strings.Contains(sql, "UNION ALL") {
		t.Fatalf("expected UNION ALL in emulated SQL: %s", sql)
	}
	if !strings.Contains(sql, "id IS NULL") {
		t.Fatalf("expected anti-null filter in rhs branch: %s", sql)
	}
	if strings.Contains(sql, "t0.id IS NULL") {
		t.Fatalf("expected anti-null filter to use unqualified USING column: %s", sql)
	}
}

func TestGenerateRecursiveCTEQuery(t *testing.T) {
	gen := &Generator{Rand: rand.New(rand.NewSource(2))}
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "id", Type: schema.TypeInt},
			{Name: "c0", Type: schema.TypeVarchar},
		},
	}
	query := gen.GenerateRecursiveCTEQuery(tbl, "cte_0")
	if query == nil {
		t.Fatalf("expected recursive cte query")
	}
	if len(query.SetOps) != 1 || !query.SetOps[0].All || query.SetOps[0].Type != SetOperationUnion {
		t.Fatalf("expected UNION ALL recursive body")
	}
	sql := query.SQLString()
	if !strings.Contains(sql, "FROM cte_0") {
		t.Fatalf("expected recursive reference, got %s", sql)
	}
	features := AnalyzeQueryFeatures(query)
	if !features.HasRecursiveCTE {
		t.Fatalf("expected recursive CTE feature flag")
	}
}

func TestGenerateRecursiveCTEQueryRequiresNumericColumn(t *testing.T) {
	gen := &Generator{Rand: rand.New(rand.NewSource(2))}
	tbl := schema.Table{
		Name: "t0",
		Columns: []schema.Column{
			{Name: "c0", Type: schema.TypeVarchar},
		},
	}
	if query := gen.GenerateRecursiveCTEQuery(tbl, "cte_0"); query != nil {
		t.Fatalf("expected nil recursive cte query when no numeric column exists")
	}
}

func TestApplyFullJoinEmulationRequiresBaseJoinKey(t *testing.T) {
	gen := &Generator{Rand: rand.New(rand.NewSource(3))}
	query := &SelectQuery{
		Items: []SelectItem{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id"}}, Alias: "id"}},
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{{
				Type:  JoinInner,
				Table: "t1",
				On:    BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
			}},
		},
	}
	if ok := gen.applyFullJoinEmulation(query); ok {
		t.Fatalf("expected full join emulation to skip when base join key is missing")
	}
	if len(query.SetOps) != 0 {
		t.Fatalf("expected query to remain unchanged on failed emulation")
	}
}

func TestPickBaseJoinKeyFromAndTree(t *testing.T) {
	on := BinaryExpr{
		Left: BinaryExpr{
			Left:  LiteralExpr{Value: 1},
			Op:    "=",
			Right: LiteralExpr{Value: 1},
		},
		Op: "AND",
		Right: BinaryExpr{
			Left:  UnaryExpr{Op: "+", Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0"}}},
			Op:    "=",
			Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0"}},
		},
	}
	key, ok := pickBaseJoinKey(on, "t0")
	if !ok {
		t.Fatalf("expected to find base join key from AND tree")
	}
	if key != "k0" {
		t.Fatalf("expected base key k0, got %s", key)
	}
}

func TestWindowExprBuildWithNamedBaseAndOverrides(t *testing.T) {
	expr := WindowExpr{
		Name:       "SUM",
		Args:       []Expr{ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}},
		WindowName: "w0",
		OrderBy:    []OrderBy{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c1"}}}},
		Frame:      &WindowFrame{Unit: "ROWS", Start: "1 PRECEDING", End: "CURRENT ROW"},
	}
	var b SQLBuilder
	expr.Build(&b)
	sql := b.String()
	if !strings.Contains(sql, "OVER (w0 ORDER BY t0.c1 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)") {
		t.Fatalf("unexpected named window override SQL: %s", sql)
	}
}

func TestMaybeAppendGroupingSelectItemUnwrapsOrdinal(t *testing.T) {
	gen := &Generator{}
	query := &SelectQuery{
		GroupBy: []Expr{
			GroupByOrdinalExpr{
				Ordinal: 1,
				Expr:    ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}},
			},
		},
	}
	gen.maybeAppendGroupingSelectItem(query)
	if len(query.Items) != 1 {
		t.Fatalf("expected one grouping select item, got %d", len(query.Items))
	}
	grouping, ok := query.Items[0].Expr.(FuncExpr)
	if !ok || grouping.Name != "GROUPING" || len(grouping.Args) != 1 {
		t.Fatalf("expected GROUPING(expr) item, got %#v", query.Items[0].Expr)
	}
	arg, ok := grouping.Args[0].(ColumnExpr)
	if !ok {
		t.Fatalf("expected GROUPING argument to unwrap ordinal into column expr")
	}
	if arg.Ref.Table != "t0" || arg.Ref.Name != "c0" {
		t.Fatalf("unexpected GROUPING arg %s.%s", arg.Ref.Table, arg.Ref.Name)
	}
}

func TestPickSetOperationAllIntersectAndExceptAlwaysFalse(t *testing.T) {
	gen := &Generator{Rand: rand.New(rand.NewSource(7))}
	for i := 0; i < 50; i++ {
		if gen.pickSetOperationAll(SetOperationIntersect) {
			t.Fatalf("INTERSECT ALL should never be generated")
		}
		if gen.pickSetOperationAll(SetOperationExcept) {
			t.Fatalf("EXCEPT ALL should never be generated")
		}
	}
}

func TestClearSetOperationOrderLimit(t *testing.T) {
	rootLimit := 5
	rhsLimit := 3
	rhs := &SelectQuery{
		Items:   []SelectItem{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c0"}}, Alias: "c0"}},
		From:    FromClause{BaseTable: "t1"},
		OrderBy: []OrderBy{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c0"}}}},
		Limit:   &rhsLimit,
	}
	query := &SelectQuery{
		Items:   []SelectItem{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}, Alias: "c0"}},
		From:    FromClause{BaseTable: "t0"},
		OrderBy: []OrderBy{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}}},
		Limit:   &rootLimit,
		SetOps: []SetOperation{{
			Type:  SetOperationUnion,
			All:   true,
			Query: rhs,
		}},
	}

	clearSetOperationOrderLimit(query)

	if len(query.OrderBy) != 0 || query.Limit != nil {
		t.Fatalf("expected root set-op ORDER/LIMIT to be cleared")
	}
	if rhs == nil || len(rhs.OrderBy) != 0 || rhs.Limit != nil {
		t.Fatalf("expected operand set-op ORDER/LIMIT to be cleared")
	}
}

func TestClearSetOperationOrderLimitKeepsPlainQuery(t *testing.T) {
	limit := 2
	query := &SelectQuery{
		Items:   []SelectItem{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}, Alias: "c0"}},
		From:    FromClause{BaseTable: "t0"},
		OrderBy: []OrderBy{{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0"}}}},
		Limit:   &limit,
	}

	clearSetOperationOrderLimit(query)

	if len(query.OrderBy) == 0 || query.Limit == nil || *query.Limit != 2 {
		t.Fatalf("expected plain query ORDER/LIMIT to be preserved")
	}
}
