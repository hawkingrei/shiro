package oracle

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"shiro/internal/generator"
)

func TestInjectHintWithCTE(t *testing.T) {
	cte := generator.CTE{
		Name: "cte1",
		Query: &generator.SelectQuery{
			Items: []generator.SelectItem{
				{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "c1"}}, Alias: "c1"},
			},
			From: generator.FromClause{BaseTable: "t1"},
		},
	}
	query := &generator.SelectQuery{
		With:  []generator.CTE{cte},
		Items: []generator.SelectItem{{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "cte1", Name: "c1"}}, Alias: "c1"}},
		From:  generator.FromClause{BaseTable: "cte1"},
	}

	sql := query.SQLString()
	withIdx := strings.Index(strings.ToUpper(sql), "WITH")
	selectIdx := strings.Index(strings.ToUpper(sql), "SELECT")
	if withIdx == -1 || selectIdx == -1 || selectIdx <= withIdx {
		t.Fatalf("unexpected SQL: %s", sql)
	}
	hinted := injectHint(query, "HASH_JOIN(t1, t2)")
	if strings.Index(hinted, "/*+") < selectIdx {
		t.Fatalf("hint injected before top-level SELECT: %s", hinted)
	}
}

func TestJoinReorderThresholdHintsRange(t *testing.T) {
	rand.Seed(1)
	hints := joinReorderThresholdHints(5)
	if len(hints) != 1 {
		t.Fatalf("expected 1 hint, got %d", len(hints))
	}
	value := strings.TrimPrefix(hints[0], "SET_VAR(tidb_opt_join_reorder_threshold=")
	value = strings.TrimSuffix(value, ")")
	threshold, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("parse threshold: %v", err)
	}
	if threshold < 3 || threshold > 7 {
		t.Fatalf("threshold out of range: %d", threshold)
	}
}

func TestDQPSetVarHintsCount(t *testing.T) {
	rand.Seed(2)
	for i := 0; i < 20; i++ {
		hints := dqpSetVarHints(3, true, true, true, true, true, true)
		if len(hints) > 2 {
			t.Fatalf("expected <=2 set_var hints, got %d", len(hints))
		}
	}
}

func TestDQPHintsForQueryCount(t *testing.T) {
	rand.Seed(3)
	noArgHints := map[string]struct{}{
		HintStraightJoin:    {},
		HintSemiJoinRewrite: {},
		HintNoDecorrelate:   {},
		HintHashAgg:         {},
		HintStreamAgg:       {},
		HintAggToCop:        {},
	}
	hints := dqpHintsForQuery([]string{"t1", "t2"}, true, true, true, true, noArgHints)
	if len(hints) > 2 {
		t.Fatalf("expected <=2 hints, got %d", len(hints))
	}
	for _, hint := range hints {
		if strings.TrimSpace(hint) == "" {
			t.Fatalf("unexpected empty hint")
		}
	}
}

func TestBuildCombinedHints(t *testing.T) {
	setVars := []string{"SET_VAR(a=1)", "SET_VAR(b=2)"}
	base := []string{"HASH_JOIN(t1,t2)", "LEADING(t1,t2)"}
	hints := buildCombinedHints(setVars, base, 2)
	if len(hints) != 2 {
		t.Fatalf("expected 2 hints, got %d", len(hints))
	}
	for _, hint := range hints {
		if !strings.Contains(hint, ", ") {
			t.Fatalf("expected comma-separated hint, got %s", hint)
		}
	}
}

func TestFindTopLevelSelectIndex(t *testing.T) {
	sql := "WITH cte AS (SELECT c1 FROM t1) SELECT c1 FROM cte WHERE c1 IN (SELECT c1 FROM t2)"
	idx := findTopLevelSelectIndex(sql)
	if idx == -1 {
		t.Fatalf("expected select index, got -1")
	}
	if !strings.EqualFold(sql[idx:idx+6], "SELECT") {
		t.Fatalf("expected SELECT at index, got %s", sql[idx:])
	}
	marker := ") SELECT"
	expected := strings.Index(sql, marker)
	if expected == -1 {
		t.Fatalf("unexpected SQL shape: %s", sql)
	}
	expected += len(marker) - len("SELECT")
	if idx != expected {
		t.Fatalf("unexpected SELECT index: %d (expected %d)", idx, expected)
	}
}
