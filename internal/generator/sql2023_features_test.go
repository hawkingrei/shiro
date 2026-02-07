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
	if !strings.Contains(sql, "t0.id IS NULL") {
		t.Fatalf("expected anti-null filter in rhs branch: %s", sql)
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
}
