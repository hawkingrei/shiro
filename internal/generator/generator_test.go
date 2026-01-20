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
		if q.Distinct && len(q.OrderBy) > 0 {
			items := exprSet(selectItemExprs(q.Items))
			for _, ob := range q.OrderBy {
				if !items[exprString(ob.Expr)] {
					t.Fatalf("distinct order by not in select list: %s", q.SQLString())
				}
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
