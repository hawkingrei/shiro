package oracle

import (
	"strconv"
	"strings"
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/schema"
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
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	state := schema.State{}
	gen := generator.New(cfg, &state, 1)
	hints := joinReorderThresholdHints(gen, 5)
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
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	state := schema.State{}
	gen := generator.New(cfg, &state, 2)
	for i := 0; i < 20; i++ {
		hints := dqpSetVarHints(gen, 3, true, true, true, true, true, true, nil)
		if len(hints) > 3 {
			t.Fatalf("expected <=3 set_var hints, got %d", len(hints))
		}
		if len(hints) == 0 {
			t.Fatalf("expected at least one set_var hint")
		}
	}
}

func TestDQPHintsForQueryCount(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	state := schema.State{}
	gen := generator.New(cfg, &state, 3)
	noArgHints := map[string]struct{}{
		HintStraightJoin:    {},
		HintSemiJoinRewrite: {},
		HintNoDecorrelate:   {},
		HintHashAgg:         {},
		HintStreamAgg:       {},
		HintAggToCop:        {},
	}
	hints := dqpHintsForQuery(gen, []string{"t1", "t2"}, true, true, true, true, noArgHints, nil)
	if len(hints) > 3 {
		t.Fatalf("expected <=3 hints, got %d", len(hints))
	}
	for _, hint := range hints {
		if strings.TrimSpace(hint) == "" {
			t.Fatalf("unexpected empty hint")
		}
	}
}

func TestDQPHintPickLimitsFromConfig(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.DQPBaseHintPick = 6
	cfg.Oracles.DQPSetVarHintPick = 7

	state := schema.State{}
	gen := generator.New(cfg, &state, 13)

	noArgHints := map[string]struct{}{
		HintStraightJoin:    {},
		HintSemiJoinRewrite: {},
		HintNoDecorrelate:   {},
		HintHashAgg:         {},
		HintStreamAgg:       {},
		HintAggToCop:        {},
	}
	baseHints := dqpHintsForQuery(gen, []string{"t1", "t2", "t3"}, true, true, true, true, noArgHints, nil)
	if len(baseHints) > 6 {
		t.Fatalf("expected <=6 base hints, got %d", len(baseHints))
	}
	setVarHints := dqpSetVarHints(gen, 3, true, true, true, true, true, true, nil)
	if len(setVarHints) > 7 {
		t.Fatalf("expected <=7 set_var hints, got %d", len(setVarHints))
	}
	if len(setVarHints) == 0 {
		t.Fatalf("expected set_var hints when candidates exist")
	}
}

func TestDQPSetVarHintCandidatesIncludePartialOrderedTopN(t *testing.T) {
	candidates := dqpSetVarHintCandidates(nil, 3, true, true, true, true, true, true, nil)
	if !containsHint(candidates, SetVarPartialOrderedTopNCost) {
		t.Fatalf("expected %s in candidates, got %v", SetVarPartialOrderedTopNCost, candidates)
	}
	if !containsHint(candidates, SetVarPartialOrderedTopNDisable) {
		t.Fatalf("expected %s in candidates, got %v", SetVarPartialOrderedTopNDisable, candidates)
	}
}

func TestDQPSetVarHintCandidatesIncludeMPPWhenJoin(t *testing.T) {
	candidates := dqpSetVarHintCandidates(nil, 3, true, true, true, true, true, true, nil)
	if !containsHint(candidates, SetVarAllowMPPOn) {
		t.Fatalf("expected %s in candidates, got %v", SetVarAllowMPPOn, candidates)
	}
	if !containsHint(candidates, SetVarAllowMPPOff) {
		t.Fatalf("expected %s in candidates, got %v", SetVarAllowMPPOff, candidates)
	}
}

func TestDQPSetVarHintCandidatesSkipMPPWithoutJoin(t *testing.T) {
	candidates := dqpSetVarHintCandidates(nil, 1, false, false, false, false, false, false, nil)
	if containsHint(candidates, SetVarAllowMPPOn) {
		t.Fatalf("did not expect %s without joins, got %v", SetVarAllowMPPOn, candidates)
	}
	if containsHint(candidates, SetVarAllowMPPOff) {
		t.Fatalf("did not expect %s without joins, got %v", SetVarAllowMPPOff, candidates)
	}
}

func TestDQPSetVarHintCandidatesSkipMPPWhenDisabled(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.DisableMPP = true
	state := schema.State{}
	gen := generator.New(cfg, &state, 17)
	candidates := dqpSetVarHintCandidates(gen, 3, true, true, true, true, true, true, nil)
	if containsHint(candidates, SetVarAllowMPPOn) {
		t.Fatalf("did not expect %s when disable_mpp is true, got %v", SetVarAllowMPPOn, candidates)
	}
	if containsHint(candidates, SetVarAllowMPPOff) {
		t.Fatalf("did not expect %s when disable_mpp is true, got %v", SetVarAllowMPPOff, candidates)
	}
}

func TestDQPExternalHintCandidates(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.DQPExternalHints = []string{
		"SET_VAR(tidb_opt_partial_ordered_index_for_topn='COST')",
		"tidb_opt_partial_ordered_index_for_topn='DISABLE'",
		"hash_join",
		"   ",
	}
	state := schema.State{}
	gen := generator.New(cfg, &state, 7)
	noArgHints := map[string]struct{}{
		HintStraightJoin:    {},
		HintSemiJoinRewrite: {},
		HintNoDecorrelate:   {},
		HintHashAgg:         {},
		HintStreamAgg:       {},
		HintAggToCop:        {},
	}
	baseHints, setVarHints := dqpExternalHintCandidates(gen, []string{"t1", "t2"}, noArgHints)
	if !containsHint(baseHints, "HASH_JOIN(t1, t2)") {
		t.Fatalf("expected normalized HASH_JOIN base hint, got %v", baseHints)
	}
	if !containsHint(setVarHints, "SET_VAR(tidb_opt_partial_ordered_index_for_topn='COST')") {
		t.Fatalf("expected external set-var COST hint, got %v", setVarHints)
	}
	if !containsHint(setVarHints, "SET_VAR(tidb_opt_partial_ordered_index_for_topn='DISABLE')") {
		t.Fatalf("expected wrapped set-var DISABLE hint, got %v", setVarHints)
	}
}

func TestDQPExternalHintCandidatesSkipsUnsafeAndMalformed(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.DQPExternalHints = []string{
		"SET_VAR(tidb_opt_partial_ordered_index_for_topn='COST'",
		"hash_join */ select 1",
		"MERGE_JOIN",
	}
	state := schema.State{}
	gen := generator.New(cfg, &state, 9)
	noArgHints := map[string]struct{}{
		HintStraightJoin:    {},
		HintSemiJoinRewrite: {},
		HintNoDecorrelate:   {},
		HintHashAgg:         {},
		HintStreamAgg:       {},
		HintAggToCop:        {},
	}

	baseHints, setVarHints := dqpExternalHintCandidates(gen, []string{"t1", "t2"}, noArgHints)
	if !containsHint(baseHints, "MERGE_JOIN(t1, t2)") {
		t.Fatalf("expected MERGE_JOIN base hint, got %v", baseHints)
	}
	if len(setVarHints) != 0 {
		t.Fatalf("expected malformed set-var hint to be dropped, got %v", setVarHints)
	}
	for _, hint := range baseHints {
		if strings.Contains(hint, "*/") {
			t.Fatalf("unexpected unsafe hint in candidates: %q", hint)
		}
	}
}

func TestDQPExternalHintCandidatesSkipMPPSetVarWhenDisabled(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.DisableMPP = true
	cfg.Oracles.DQPExternalHints = []string{
		"tidb_allow_mpp=ON",
		"SET_VAR(tidb_enforce_mpp=ON)",
		"SET_VAR(tidb_opt_use_toja=OFF)",
	}
	state := schema.State{}
	gen := generator.New(cfg, &state, 19)
	noArgHints := map[string]struct{}{
		HintStraightJoin:    {},
		HintSemiJoinRewrite: {},
		HintNoDecorrelate:   {},
		HintHashAgg:         {},
		HintStreamAgg:       {},
		HintAggToCop:        {},
	}
	_, setVarHints := dqpExternalHintCandidates(gen, []string{"t1", "t2"}, noArgHints)
	if containsHint(setVarHints, "SET_VAR(tidb_allow_mpp=ON)") {
		t.Fatalf("did not expect tidb_allow_mpp hint when disable_mpp is true: %v", setVarHints)
	}
	if containsHint(setVarHints, "SET_VAR(tidb_enforce_mpp=ON)") {
		t.Fatalf("did not expect tidb_enforce_mpp hint when disable_mpp is true: %v", setVarHints)
	}
	if !containsHint(setVarHints, "SET_VAR(tidb_opt_use_toja=OFF)") {
		t.Fatalf("expected non-mpp set-var hint to remain, got %v", setVarHints)
	}
}

func TestNormalizeSetVarHintClassification(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		hint     string
		isSetVar bool
		valid    bool
	}{
		{
			name:     "full_set_var",
			raw:      "SET_VAR(tidb_opt_partial_ordered_index_for_topn='COST')",
			hint:     "SET_VAR(tidb_opt_partial_ordered_index_for_topn='COST')",
			isSetVar: true,
			valid:    true,
		},
		{
			name:     "set_var_shorthand",
			raw:      "tidb_opt_partial_ordered_index_for_topn='DISABLE'",
			hint:     "SET_VAR(tidb_opt_partial_ordered_index_for_topn='DISABLE')",
			isSetVar: true,
			valid:    true,
		},
		{
			name:     "malformed_set_var",
			raw:      "SET_VAR(tidb_opt_partial_ordered_index_for_topn='COST'",
			hint:     "",
			isSetVar: true,
			valid:    false,
		},
		{
			name:     "base_hint",
			raw:      "HASH_JOIN",
			hint:     "",
			isSetVar: false,
			valid:    false,
		},
	}

	for _, tc := range cases {
		hint, isSetVar, valid := normalizeSetVarHint(tc.raw)
		if hint != tc.hint || isSetVar != tc.isSetVar || valid != tc.valid {
			t.Fatalf(
				"%s: normalizeSetVarHint(%q) = (%q,%v,%v), want (%q,%v,%v)",
				tc.name,
				tc.raw,
				hint,
				isSetVar,
				valid,
				tc.hint,
				tc.isSetVar,
				tc.valid,
			)
		}
	}
}

func containsHint(hints []string, target string) bool {
	for _, hint := range hints {
		if hint == target {
			return true
		}
	}
	return false
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

func TestDQPHintReward(t *testing.T) {
	if dqpHintReward(false) >= dqpHintReward(true) {
		t.Fatalf("expected mismatch reward to be higher")
	}
}

func TestDQPReplaySetVarAssignment(t *testing.T) {
	cases := []struct {
		name string
		hint string
		want string
		ok   bool
	}{
		{
			name: "set_var_only",
			hint: "SET_VAR(tidb_opt_use_toja=ON)",
			want: "tidb_opt_use_toja=ON",
			ok:   true,
		},
		{
			name: "combined_set_var_first",
			hint: "SET_VAR(tidb_opt_use_toja=OFF), HASH_JOIN(t1, t2)",
			want: "tidb_opt_use_toja=OFF",
			ok:   true,
		},
		{
			name: "combined_set_var_later",
			hint: "HASH_JOIN(t1, t2), SET_VAR(tidb_opt_partial_ordered_index_for_topn='DISABLE')",
			want: "tidb_opt_partial_ordered_index_for_topn='DISABLE'",
			ok:   true,
		},
		{
			name: "no_set_var",
			hint: "HASH_JOIN(t1, t2)",
			want: "",
			ok:   false,
		},
		{
			name: "reject_statement_injection",
			hint: "SET_VAR(tidb_opt_use_toja=ON;DROP TABLE t1)",
			want: "",
			ok:   false,
		},
		{
			name: "reject_comment_injection",
			hint: "SET_VAR(tidb_opt_use_toja=ON/*x*/)",
			want: "",
			ok:   false,
		},
		{
			name: "reject_multiple_assignments",
			hint: "SET_VAR(tidb_opt_use_toja=ON,tidb_opt_fix_control='123:ON')",
			want: "",
			ok:   false,
		},
		{
			name: "reject_unsafe_name",
			hint: "SET_VAR(@@tidb_opt_use_toja=ON)",
			want: "",
			ok:   false,
		},
	}

	for _, tc := range cases {
		got, ok := dqpReplaySetVarAssignment(tc.hint)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("%s: dqpReplaySetVarAssignment(%q)=(%q,%v), want (%q,%v)", tc.name, tc.hint, got, ok, tc.want, tc.ok)
		}
	}
}

func TestDQPComplexitySkipDetailsSetOpsAndDerived(t *testing.T) {
	simpleDerived := &generator.SelectQuery{
		Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}},
		From:  generator.FromClause{BaseTable: "t0"},
	}
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}},
		From: generator.FromClause{
			BaseTable: "t0",
			BaseQuery: simpleDerived,
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", TableQuery: simpleDerived},
				{Type: generator.JoinInner, Table: "t2", TableQuery: simpleDerived},
			},
		},
		SetOps: []generator.SetOperation{
			{Type: generator.SetOperationIntersect, Query: simpleDerived},
			{Type: generator.SetOperationIntersect, Query: simpleDerived},
		},
	}
	details, skip := dqpComplexitySkipDetails(query)
	if !skip {
		t.Fatalf("expected complexity guard skip")
	}
	reason, _ := details["dqp_complexity_reason"].(string)
	if reason != "set_ops_and_derived_tables" {
		t.Fatalf("unexpected complexity reason: %s", reason)
	}
}

func TestDQPComplexitySkipDetailsAlwaysFalseJoinChain(t *testing.T) {
	falseJoin := generator.BinaryExpr{
		Left:  generator.LiteralExpr{Value: 1},
		Op:    "=",
		Right: generator.LiteralExpr{Value: 0},
	}
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", On: falseJoin},
				{Type: generator.JoinInner, Table: "t2", On: falseJoin},
				{Type: generator.JoinInner, Table: "t3", On: falseJoin},
			},
		},
	}
	details, skip := dqpComplexitySkipDetails(query)
	if !skip {
		t.Fatalf("expected complexity guard skip")
	}
	reason, _ := details["dqp_complexity_reason"].(string)
	if reason != "always_false_join_chain" {
		t.Fatalf("unexpected complexity reason: %s", reason)
	}
}

func TestDQPExprConstBoolLiteralComparison(t *testing.T) {
	expr := generator.BinaryExpr{
		Left:  generator.LiteralExpr{Value: 1},
		Op:    "=",
		Right: generator.LiteralExpr{Value: 0},
	}
	if !dqpExprAlwaysFalse(expr) {
		t.Fatalf("expected literal equality to be recognized as always false")
	}
}

func TestNormalizeReplaySetVarAssignment(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{
			name: "valid_unquoted",
			raw:  "tidb_opt_use_toja=ON",
			want: "tidb_opt_use_toja=ON",
			ok:   true,
		},
		{
			name: "valid_single_quoted",
			raw:  "tidb_opt_partial_ordered_index_for_topn='DISABLE'",
			want: "tidb_opt_partial_ordered_index_for_topn='DISABLE'",
			ok:   true,
		},
		{
			name: "reject_semicolon",
			raw:  "tidb_opt_use_toja=ON;DROP TABLE t1",
			want: "",
			ok:   false,
		},
		{
			name: "reject_comment",
			raw:  "tidb_opt_use_toja=ON/*x*/",
			want: "",
			ok:   false,
		},
		{
			name: "reject_double_equals",
			raw:  "tidb_opt_use_toja=ON=OFF",
			want: "",
			ok:   false,
		},
	}
	for _, tc := range cases {
		got, ok := normalizeReplaySetVarAssignment(tc.raw)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("%s: normalizeReplaySetVarAssignment(%q)=(%q,%v), want (%q,%v)", tc.name, tc.raw, got, ok, tc.want, tc.ok)
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
