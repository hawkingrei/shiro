package oracle

import (
	"strings"
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/schema"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"

	"github.com/go-sql-driver/mysql"
)

func TestRewritePredicateDoubleNot(t *testing.T) {
	stmt := parseSelect(t, "SELECT a AS a FROM t WHERE a > 1")
	stmt.Where = rewritePredicate(stmt.Where, eetRewriteDoubleNot)
	sql, err := restoreEETSQL(stmt)
	if err != nil {
		t.Fatalf("restore err: %v", err)
	}
	upper := strings.ToUpper(sql)
	if !strings.Contains(upper, "NOT NOT") && !strings.Contains(upper, "NOT (NOT") {
		t.Fatalf("expected double NOT rewrite, got: %s", sql)
	}
}

func TestRewritePredicateAndTrue(t *testing.T) {
	stmt := parseSelect(t, "SELECT a AS a FROM t WHERE a > 1")
	stmt.Where = rewritePredicate(stmt.Where, eetRewriteAndTrue)
	sql, err := restoreEETSQL(stmt)
	if err != nil {
		t.Fatalf("restore err: %v", err)
	}
	upper := strings.ToUpper(sql)
	if !strings.Contains(upper, "AND 1") {
		t.Fatalf("expected AND TRUE rewrite, got: %s", sql)
	}
}

func TestRewritePredicateOrFalse(t *testing.T) {
	stmt := parseSelect(t, "SELECT a AS a FROM t WHERE a > 1")
	stmt.Where = rewritePredicate(stmt.Where, eetRewriteOrFalse)
	sql, err := restoreEETSQL(stmt)
	if err != nil {
		t.Fatalf("restore err: %v", err)
	}
	upper := strings.ToUpper(sql)
	if !strings.Contains(upper, "OR 0") {
		t.Fatalf("expected OR FALSE rewrite, got: %s", sql)
	}
}

func TestApplyEETTransformDefault(t *testing.T) {
	sql := "SELECT a AS a FROM t WHERE a > 1"
	out, details, err := applyEETTransform(sql, nil)
	if err != nil {
		t.Fatalf("transform err: %v", err)
	}
	if out == "" || out == sql {
		t.Fatalf("expected transformed sql, got: %s", out)
	}
	if details["skip_reason"] != nil {
		t.Fatalf("unexpected skip reason: %v", details["skip_reason"])
	}
	if details["rewrite"] == nil {
		t.Fatalf("expected rewrite detail")
	}
}

func TestApplyEETTransformHaving(t *testing.T) {
	sql := "SELECT a AS a FROM t GROUP BY a HAVING a > 1"
	out, details, err := applyEETTransform(sql, nil)
	if err != nil {
		t.Fatalf("transform err: %v", err)
	}
	if out == "" || out == sql {
		t.Fatalf("expected transformed sql, got: %s", out)
	}
	if details["skip_reason"] != nil {
		t.Fatalf("unexpected skip reason: %v", details["skip_reason"])
	}
	if details["rewrite"] == nil {
		t.Fatalf("expected rewrite detail")
	}
}

func TestApplyEETTransformJoinOn(t *testing.T) {
	sql := "SELECT t1.a AS a FROM t1 JOIN t2 ON t1.a = 1"
	out, details, err := applyEETTransform(sql, nil)
	if err != nil {
		t.Fatalf("transform err: %v", err)
	}
	if out == "" || out == sql {
		t.Fatalf("expected transformed sql, got: %s", out)
	}
	if details["skip_reason"] != nil {
		t.Fatalf("unexpected skip reason: %v", details["skip_reason"])
	}
	if details["rewrite"] == nil {
		t.Fatalf("expected rewrite detail")
	}
}

func TestApplyEETTransformColumnIdentity(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.EETRewrites = config.EETRewriteWeights{
		NumericIdentity: 10,
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
	gen := generator.New(cfg, &state, 1)
	sql := "SELECT t0.c0 AS c0 FROM t0 WHERE t0.c0 > t0.c0"
	out, details, err := applyEETTransform(sql, gen)
	if err != nil {
		t.Fatalf("transform err: %v", err)
	}
	if out == "" || out == sql {
		t.Fatalf("expected transformed sql, got: %s", out)
	}
	if details["skip_reason"] != nil {
		t.Fatalf("unexpected skip reason: %v", details["skip_reason"])
	}
	if !strings.Contains(out, "+0") {
		t.Fatalf("expected numeric identity rewrite, got: %s", out)
	}
}

func TestApplyEETTransformFallbackRewriteKind(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.EETRewrites = config.EETRewriteWeights{
		NumericIdentity: 10,
	}
	state := schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "c0", Type: schema.TypeVarchar},
					{Name: "c1", Type: schema.TypeVarchar},
				},
			},
		},
	}
	gen := generator.New(cfg, &state, 1)
	sql := "SELECT t0.c0 AS c0 FROM t0 WHERE t0.c0 = t0.c1"
	out, details, err := applyEETTransform(sql, gen)
	if err != nil {
		t.Fatalf("transform err: %v", err)
	}
	if out == "" || out == sql {
		t.Fatalf("expected transformed sql via fallback kind, got: %s", out)
	}
	if details["rewrite"] == nil {
		t.Fatalf("expected rewrite detail")
	}
}

func TestOrderByAllConstant(t *testing.T) {
	orderBy := []generator.OrderBy{
		{Expr: generator.LiteralExpr{Value: 1}},
		{Expr: generator.BinaryExpr{
			Left:  generator.LiteralExpr{Value: 1},
			Op:    "+",
			Right: generator.LiteralExpr{Value: 2},
		}},
	}
	if !orderByAllConstant(orderBy, 0) {
		t.Fatalf("expected orderByAllConstant to be true")
	}
	orderBy = []generator.OrderBy{
		{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "c0"}}},
	}
	if orderByAllConstant(orderBy, 1) {
		t.Fatalf("expected orderByAllConstant to be false")
	}
	orderBy = []generator.OrderBy{
		{Expr: generator.LiteralExpr{Value: 1}},
	}
	if orderByAllConstant(orderBy, 2) {
		t.Fatalf("expected orderByAllConstant to be false for ordinal")
	}
}

func TestOrderByDistinctKeys(t *testing.T) {
	orderBy := []generator.OrderBy{
		{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "c0"}}},
		{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "c1"}}},
	}
	if got := orderByDistinctKeys(orderBy, 2); got != 2 {
		t.Fatalf("expected 2 distinct columns, got %d", got)
	}
	orderBy = []generator.OrderBy{
		{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "c0"}}},
		{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "c0"}}},
	}
	if got := orderByDistinctKeys(orderBy, 2); got != 1 {
		t.Fatalf("expected 1 distinct column, got %d", got)
	}
	orderBy = []generator.OrderBy{
		{Expr: generator.SubqueryExpr{Query: &generator.SelectQuery{}}},
		{Expr: generator.LiteralExpr{Value: 1}},
	}
	if got := orderByDistinctKeys(orderBy, 2); got != 1 {
		t.Fatalf("expected 1 distinct ordinal, got %d", got)
	}
	orderBy = []generator.OrderBy{
		{Expr: generator.SubqueryExpr{Query: &generator.SelectQuery{}}},
		{Expr: generator.LiteralExpr{Value: 1}},
		{Expr: generator.LiteralExpr{Value: 2}},
	}
	if got := orderByDistinctKeys(orderBy, 2); got != 2 {
		t.Fatalf("expected 2 distinct ordinals, got %d", got)
	}
	orderBy = []generator.OrderBy{
		{Expr: generator.SubqueryExpr{Query: &generator.SelectQuery{}}},
	}
	if got := orderByDistinctKeys(orderBy, 1); got != 0 {
		t.Fatalf("expected 0 distinct columns, got %d", got)
	}
}

func TestEETDistinctOrderByCompatible(t *testing.T) {
	query := &generator.SelectQuery{
		Distinct: true,
		Items: []generator.SelectItem{
			{
				Expr:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "c0"}},
				Alias: "c0",
			},
		},
		OrderBy: []generator.OrderBy{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Name: "c0"}}},
		},
	}
	if !eetDistinctOrderByCompatible(query) {
		t.Fatalf("expected alias-based DISTINCT ORDER BY to be compatible")
	}

	query.OrderBy = []generator.OrderBy{{Expr: generator.LiteralExpr{Value: 1}}}
	if !eetDistinctOrderByCompatible(query) {
		t.Fatalf("expected ordinal DISTINCT ORDER BY to be compatible")
	}

	query.OrderBy = []generator.OrderBy{
		{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "c1"}}},
	}
	if eetDistinctOrderByCompatible(query) {
		t.Fatalf("expected non-selected DISTINCT ORDER BY expression to be incompatible")
	}
}

func TestEETShouldRetryNoTransform(t *testing.T) {
	retryable := []string{
		"eet:no_transform",
		"eet:no_rewrite_kind",
		"eet:rewrite_no_boolean_target",
		"eet:rewrite_no_literal_target",
	}
	for _, reason := range retryable {
		if !eetShouldRetryNoTransform(map[string]any{"skip_reason": reason}) {
			t.Fatalf("expected %s to be retryable", reason)
		}
	}
	nonRetryable := []string{
		"eet:scope_invalid",
		"eet:distinct_order_by",
	}
	for _, reason := range nonRetryable {
		if eetShouldRetryNoTransform(map[string]any{"skip_reason": reason}) {
			t.Fatalf("expected %s to be non-retryable", reason)
		}
	}
	if eetShouldRetryNoTransform(nil) {
		t.Fatalf("nil details should not be retryable")
	}
}

func TestEETIsDistinctOrderByErr(t *testing.T) {
	err := &mysql.MySQLError{
		Number:  3065,
		Message: "Expression #1 of ORDER BY clause is not in SELECT list",
	}
	if !eetIsDistinctOrderByErr(err) {
		t.Fatalf("expected distinct-order-by runtime error to be detected")
	}
}

func TestEETSignatureErrorDetailsDistinctOrderBy(t *testing.T) {
	err := &mysql.MySQLError{
		Number:  3065,
		Message: "Expression #1 of ORDER BY clause is not in SELECT list",
	}
	reason, bugHint := eetSignatureErrorDetails(err, "base")
	if reason != "eet:distinct_order_by" {
		t.Fatalf("unexpected reason: %s", reason)
	}
	if bugHint != "" {
		t.Fatalf("unexpected bug hint: %s", bugHint)
	}
}

func TestRewriteLiteralNumericIdentity(t *testing.T) {
	expr := ast.NewValueExpr(5, "", "")
	next, ok := rewriteLiteralValue(expr, eetRewriteNumericIdentity)
	if !ok {
		t.Fatalf("expected numeric identity rewrite")
	}
	sql, err := restoreEETSQL(next.(ast.Node))
	if err != nil {
		t.Fatalf("restore err: %v", err)
	}
	if !strings.Contains(sql, "+") {
		t.Fatalf("expected numeric identity expression, got: %s", sql)
	}
}

func TestRewriteLiteralStringIdentity(t *testing.T) {
	expr := ast.NewValueExpr("x", "", "")
	next, ok := rewriteLiteralValue(expr, eetRewriteStringIdentity)
	if !ok {
		t.Fatalf("expected string identity rewrite")
	}
	sql, err := restoreEETSQL(next.(ast.Node))
	if err != nil {
		t.Fatalf("restore err: %v", err)
	}
	if !strings.Contains(strings.ToUpper(sql), "CONCAT") {
		t.Fatalf("expected CONCAT identity expression, got: %s", sql)
	}
}

func TestRewriteLiteralDateIdentity(t *testing.T) {
	expr := ast.NewValueExpr("2025-01-01", "", "")
	next, ok := rewriteLiteralValue(expr, eetRewriteDateIdentity)
	if !ok {
		t.Fatalf("expected date identity rewrite")
	}
	sql, err := restoreEETSQL(next.(ast.Node))
	if err != nil {
		t.Fatalf("restore err: %v", err)
	}
	if !strings.Contains(strings.ToUpper(sql), "DATE_ADD") {
		t.Fatalf("expected DATE_ADD identity expression, got: %s", sql)
	}
	if !strings.Contains(strings.ToUpper(sql), "INTERVAL 0 DAY") {
		t.Fatalf("expected INTERVAL 0 DAY, got: %s", sql)
	}
}

func TestQueryHasUsingQualifiedRefs(t *testing.T) {
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t4", Name: "k0"}}},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t4", Using: []string{"k0"}},
			},
		},
	}
	if !queryHasUsingQualifiedRefs(query) {
		t.Fatalf("expected using-qualified ref to be detected")
	}

	query.Items = []generator.SelectItem{
		{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "", Name: "k0"}}},
	}
	if queryHasUsingQualifiedRefs(query) {
		t.Fatalf("expected no using-qualified ref for unqualified column")
	}
}

func parseSelect(t *testing.T, sql string) *ast.SelectStmt {
	t.Helper()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if sel, ok := stmt.(*ast.SelectStmt); ok {
		return sel
	}
	t.Fatalf("unexpected stmt type: %T", stmt)
	return nil
}
