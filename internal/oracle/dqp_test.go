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

func TestDQPJoinTableCountIncludesCTEAndBodyTables(t *testing.T) {
	cteQuery := &generator.SelectQuery{
		Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t1",
					On: generator.BinaryExpr{
						Left:  generator.LiteralExpr{Value: 1},
						Op:    "=",
						Right: generator.LiteralExpr{Value: 1},
					},
				},
			},
		},
	}
	query := &generator.SelectQuery{
		With: []generator.CTE{{Name: "c1", Query: cteQuery}},
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "c1", Name: "c0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "c1",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t2",
					On: generator.BinaryExpr{
						Left:  generator.LiteralExpr{Value: 1},
						Op:    "=",
						Right: generator.LiteralExpr{Value: 1},
					},
				},
			},
		},
	}
	if got := dqpJoinTableCountWithCTE(query); got != 5 {
		t.Fatalf("dqpJoinTableCountWithCTE=%d want=5", got)
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
		if len(hints) > 2 {
			t.Fatalf("expected <=2 set_var hints, got %d", len(hints))
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
	if len(hints) > 4 {
		t.Fatalf("expected <=4 hints, got %d", len(hints))
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
	if len(setVarHints) > 2 {
		t.Fatalf("expected <=2 set_var hints, got %d", len(setVarHints))
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

func TestDQPSetVarHintCandidatesIncludeAllowMPPWhenJoin(t *testing.T) {
	candidates := dqpSetVarHintCandidates(nil, 3, true, true, true, true, true, true, nil)
	if !containsHint(candidates, SetVarAllowMPPOn) {
		t.Fatalf("expected %s in candidates, got %v", SetVarAllowMPPOn, candidates)
	}
	if !containsHint(candidates, SetVarAllowMPPOff) {
		t.Fatalf("expected %s in candidates, got %v", SetVarAllowMPPOff, candidates)
	}
}

func TestDQPSetVarHintCandidatesIncludeEnforceMPPWhenJoin(t *testing.T) {
	candidates := dqpSetVarHintCandidates(nil, 3, true, true, true, true, true, true, nil)
	if !containsHint(candidates, SetVarEnforceMPPOn) {
		t.Fatalf("expected %s in candidates, got %v", SetVarEnforceMPPOn, candidates)
	}
	if containsHint(candidates, SetVarEnforceMPPOff) {
		t.Fatalf("did not expect %s in candidates, got %v", SetVarEnforceMPPOff, candidates)
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
	if containsHint(candidates, SetVarEnforceMPPOn) {
		t.Fatalf("did not expect %s without joins, got %v", SetVarEnforceMPPOn, candidates)
	}
	if containsHint(candidates, SetVarEnforceMPPOff) {
		t.Fatalf("did not expect %s without joins, got %v", SetVarEnforceMPPOff, candidates)
	}
}

func TestDQPSetVarHintCandidatesSkipAllowMPPWhenDisabled(t *testing.T) {
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

func TestDQPSetVarHintCandidatesSkipEnforceMPPWhenDisabled(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.DisableMPP = true
	state := schema.State{}
	gen := generator.New(cfg, &state, 17)
	candidates := dqpSetVarHintCandidates(gen, 3, true, true, true, true, true, true, nil)
	if containsHint(candidates, SetVarEnforceMPPOn) {
		t.Fatalf("did not expect %s when disable_mpp is true, got %v", SetVarEnforceMPPOn, candidates)
	}
	if containsHint(candidates, SetVarEnforceMPPOff) {
		t.Fatalf("did not expect %s when disable_mpp is true, got %v", SetVarEnforceMPPOff, candidates)
	}
}

func TestDQPShouldRequireMPPSetVar(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	state := schema.State{}
	gen := generator.New(cfg, &state, 31)
	if !dqpShouldRequireMPPSetVar(gen, true) {
		t.Fatalf("expected MPP set-var requirement when TiFlash replicas are enabled")
	}

	cfgNoReplica := cfg
	cfgNoReplica.Oracles.MPPTiFlashReplica = 0
	genNoReplica := generator.New(cfgNoReplica, &state, 32)
	if dqpShouldRequireMPPSetVar(genNoReplica, true) {
		t.Fatalf("did not expect MPP set-var requirement without TiFlash replicas")
	}

	cfgDisableMPP := cfg
	cfgDisableMPP.Oracles.DisableMPP = true
	genDisableMPP := generator.New(cfgDisableMPP, &state, 33)
	if dqpShouldRequireMPPSetVar(genDisableMPP, true) {
		t.Fatalf("did not expect MPP set-var requirement when disable_mpp=true")
	}
	if dqpShouldRequireMPPSetVar(gen, false) {
		t.Fatalf("did not expect MPP set-var requirement without joins")
	}
}

func TestDQPSetVarHintsRequireMPPWhenTiFlashReplicaAvailable(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.DQPSetVarHintPick = 1
	state := schema.State{}
	gen := generator.New(cfg, &state, 41)
	for i := 0; i < 20; i++ {
		hints := dqpSetVarHints(gen, 3, true, true, true, true, true, true, nil)
		if len(hints) == 0 {
			t.Fatalf("expected non-empty set-var hints")
		}
		if !dqpHasSetVarCategory(hints, true) {
			t.Fatalf("expected MPP set-var hint in %v", hints)
		}
		if len(hints) > 1 {
			assertDQPSetVarPair(t, hints[0], hints[1])
		}
	}
}

func TestDQPSetVarHintsKeepMPPAndNonMPPWhenLimitAllows(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.DQPSetVarHintPick = 2
	state := schema.State{}
	gen := generator.New(cfg, &state, 42)
	for i := 0; i < 20; i++ {
		hints := dqpSetVarHints(gen, 3, true, true, true, true, true, true, nil)
		if !dqpHasSetVarCategory(hints, true) {
			t.Fatalf("expected MPP set-var hint in %v", hints)
		}
		if len(hints) > 2 {
			t.Fatalf("expected <=2 set-var hints, got %d (%v)", len(hints), hints)
		}
		if len(hints) == 2 {
			assertDQPSetVarPair(t, hints[0], hints[1])
		}
	}
}

func TestDQPEnsureSetVarTogglePairs(t *testing.T) {
	selected := []string{SetVarEnableHashJoinOn}
	candidates := []string{
		SetVarEnableHashJoinOn,
		SetVarEnableHashJoinOff,
		SetVarAllowMPPOn,
		SetVarAllowMPPOff,
	}
	out := dqpEnsureSetVarTogglePairs(selected, candidates, 2)
	if !containsHint(out, SetVarEnableHashJoinOn) || !containsHint(out, SetVarEnableHashJoinOff) {
		t.Fatalf("expected hash join ON/OFF pair, got %v", out)
	}
	if len(out) != 2 {
		t.Fatalf("expected pair-limited size 2, got %d (%v)", len(out), out)
	}
}

func TestDQPEnsureSetVarTogglePairsEnforceMPPMapsToAllowOff(t *testing.T) {
	selected := []string{SetVarEnforceMPPOn}
	candidates := []string{
		SetVarAllowMPPOn,
		SetVarAllowMPPOff,
		SetVarEnforceMPPOn,
	}
	out := dqpEnsureSetVarTogglePairs(selected, candidates, 2)
	if !containsHint(out, SetVarEnforceMPPOn) || !containsHint(out, SetVarAllowMPPOff) {
		t.Fatalf("expected enforce ON + allow OFF pair, got %v", out)
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

func containsHintPrefix(hints []string, prefix string) bool {
	for _, hint := range hints {
		if strings.HasPrefix(hint, prefix) {
			return true
		}
	}
	return false
}

func dqpHintContainsMPPSetVar(hint string) bool {
	for _, token := range splitTopLevelHintList(hint) {
		if isMPPSetVarHint(token) {
			return true
		}
	}
	return false
}

func assertDQPSetVarPair(t *testing.T, first string, second string) {
	t.Helper()
	name1, value1, ok1 := dqpParseSingleSetVarHint(first)
	name2, value2, ok2 := dqpParseSingleSetVarHint(second)
	if !ok1 || !ok2 {
		t.Fatalf("expected set-var hints, got first=%q second=%q", first, second)
	}
	targets := dqpOppositeSetVarTargets(name1, value1)
	if len(targets) == 0 {
		t.Fatalf("expected opposite targets for %q, got none", first)
	}
	name2 = strings.ToLower(strings.TrimSpace(name2))
	toggle2 := dqpNormalizeToggleValue(value2)
	for _, target := range targets {
		if target.name == name2 && target.value == toggle2 {
			return
		}
	}
	t.Fatalf("expected opposite of %q, got %q", first, second)
}

func TestDQPJoinHintCandidatesGateOuterJoin(t *testing.T) {
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:  generator.JoinLeft,
					Table: "t1",
					On: generator.BinaryExpr{
						Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "k0"}},
						Op:    "=",
						Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "k0"}},
					},
				},
			},
		},
	}
	state := schema.State{
		Tables: []schema.Table{
			{Name: "t0", HasPK: true, Columns: []schema.Column{{Name: "k0", HasIndex: true}}},
			{Name: "t1", HasPK: true, Columns: []schema.Column{{Name: "k0", HasIndex: true}}},
		},
	}
	noArgHints := map[string]struct{}{
		HintStraightJoin: {},
	}
	hints := dqpJoinHintCandidates(query, &state, noArgHints)
	if !containsHint(hints, "HASH_JOIN(t0, t1)") {
		t.Fatalf("expected HASH_JOIN hint, got %v", hints)
	}
	if containsHintPrefix(hints, "LEADING(") {
		t.Fatalf("did not expect LEADING for outer join query, got %v", hints)
	}
	if containsHintPrefix(hints, "INL_JOIN(") || containsHintPrefix(hints, "INL_HASH_JOIN(") || containsHintPrefix(hints, "INL_MERGE_JOIN(") {
		t.Fatalf("did not expect INL join hints for outer join query, got %v", hints)
	}
	if containsHintPrefix(hints, "HASH_JOIN_BUILD(") || containsHintPrefix(hints, "HASH_JOIN_PROBE(") {
		t.Fatalf("did not expect HASH_JOIN_BUILD/PROBE hints for outer join query, got %v", hints)
	}
}

func TestDQPJoinHintCandidatesInnerJoinIndexedTarget(t *testing.T) {
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t1",
					On: generator.BinaryExpr{
						Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "k0"}},
						Op:    "=",
						Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "k0"}},
					},
				},
			},
		},
	}
	state := schema.State{
		Tables: []schema.Table{
			{Name: "t0", Columns: []schema.Column{{Name: "k0"}}},
			{Name: "t1", HasPK: true, Columns: []schema.Column{{Name: "k0", HasIndex: true}}},
		},
	}
	noArgHints := map[string]struct{}{
		HintStraightJoin: {},
	}
	hints := dqpJoinHintCandidates(query, &state, noArgHints)
	for _, expected := range []string{
		"LEADING(t0, t1)",
		"INL_JOIN(t1)",
		"HASH_JOIN_BUILD(t1)",
		"HASH_JOIN_PROBE(t1)",
	} {
		if !containsHint(hints, expected) {
			t.Fatalf("expected %s in hints, got %v", expected, hints)
		}
	}
}

func TestDQPIndexHintCandidatesUseIndexMergeByQueryShape(t *testing.T) {
	state := schema.State{
		Tables: []schema.Table{
			{
				Name:  "t0",
				HasPK: true,
				Columns: []schema.Column{
					{Name: "k0", HasIndex: true},
					{Name: "k1", HasIndex: true},
				},
			},
		},
	}
	orQuery := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{BaseTable: "t0"},
		Where: generator.BinaryExpr{
			Left: generator.BinaryExpr{
				Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "k0"}},
				Op:    "=",
				Right: generator.LiteralExpr{Value: 1},
			},
			Op: "OR",
			Right: generator.BinaryExpr{
				Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "k1"}},
				Op:    "=",
				Right: generator.LiteralExpr{Value: 2},
			},
		},
	}
	orHints := dqpIndexHintCandidates(orQuery, &state)
	if !containsHint(orHints, "USE_INDEX(t0)") {
		t.Fatalf("expected USE_INDEX(t0), got %v", orHints)
	}
	if !containsHint(orHints, "USE_INDEX_MERGE(t0)") {
		t.Fatalf("expected USE_INDEX_MERGE(t0), got %v", orHints)
	}

	andQuery := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{BaseTable: "t0"},
		Where: generator.BinaryExpr{
			Left: generator.BinaryExpr{
				Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "k0"}},
				Op:    "=",
				Right: generator.LiteralExpr{Value: 1},
			},
			Op: "AND",
			Right: generator.BinaryExpr{
				Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "k1"}},
				Op:    "=",
				Right: generator.LiteralExpr{Value: 2},
			},
		},
	}
	andHints := dqpIndexHintCandidates(andQuery, &state)
	if !containsHint(andHints, "USE_INDEX(t0)") {
		t.Fatalf("expected USE_INDEX(t0), got %v", andHints)
	}
	if containsHint(andHints, "USE_INDEX_MERGE(t0)") {
		t.Fatalf("did not expect USE_INDEX_MERGE(t0) without OR predicate, got %v", andHints)
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

func TestDQPFormatHintGroups(t *testing.T) {
	groups := map[string]struct{}{
		dqpVariantGroupCombined: {},
		dqpVariantGroupBaseHint: {},
		"":                      {},
	}
	if got, want := dqpFormatHintGroups(groups), "base_hint,combined_hint"; got != want {
		t.Fatalf("dqpFormatHintGroups()=%q want=%q", got, want)
	}
}

func TestBuildDQPVariantsIncludesMultipleHintGroups(t *testing.T) {
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{
				Expr:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "c1"}},
				Alias: "c1",
			},
		},
		From: generator.FromClause{
			BaseTable: "t1",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t2",
					On: generator.BinaryExpr{
						Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "c1"}},
						Op:    "=",
						Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t2", Name: "c1"}},
					},
				},
			},
		},
	}

	variants, _ := buildDQPVariants(query, nil, true, false, false, false, false, false, nil)
	if len(variants) == 0 {
		t.Fatalf("expected non-empty variants")
	}
	groups := make(map[string]struct{}, 4)
	for _, variant := range variants {
		if strings.TrimSpace(variant.group) == "" {
			t.Fatalf("variant group should not be empty: %+v", variant)
		}
		groups[variant.group] = struct{}{}
	}
	if _, ok := groups[dqpVariantGroupBaseHint]; !ok {
		t.Fatalf("expected %s group, got %v", dqpVariantGroupBaseHint, groups)
	}
	if _, ok := groups[dqpVariantGroupSetVar]; !ok {
		if _, ok := groups[dqpVariantGroupMPP]; !ok {
			t.Fatalf("expected %s or %s group, got %v", dqpVariantGroupSetVar, dqpVariantGroupMPP, groups)
		}
	}
	if _, ok := groups[dqpVariantGroupCombined]; !ok {
		t.Fatalf("expected %s group, got %v", dqpVariantGroupCombined, groups)
	}
}

func TestBuildDQPVariantsMPPOverlapsCombinedHints(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.DQPSetVarHintPick = 1
	state := schema.State{}
	gen := generator.New(cfg, &state, 77)
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{
				Expr:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "c1"}},
				Alias: "c1",
			},
		},
		From: generator.FromClause{
			BaseTable: "t1",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t2",
					On: generator.BinaryExpr{
						Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "c1"}},
						Op:    "=",
						Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t2", Name: "c1"}},
					},
				},
			},
		},
	}

	variants, _ := buildDQPVariants(query, nil, false, false, false, false, false, false, gen)
	hasMPPGroup := false
	hasCombinedMPP := false
	for _, variant := range variants {
		if variant.group == dqpVariantGroupMPP && dqpHintContainsMPPSetVar(variant.hint) {
			hasMPPGroup = true
		}
		if variant.group == dqpVariantGroupCombined && dqpHintContainsMPPSetVar(variant.hint) {
			hasCombinedMPP = true
		}
	}
	if !hasMPPGroup {
		t.Fatalf("expected MPP-specific hint variants, got %#v", variants)
	}
	if !hasCombinedMPP {
		t.Fatalf("expected combined hints to include MPP set-var overlays, got %#v", variants)
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

func TestDQPComplexityGuardReasonSetOpsAndDerived(t *testing.T) {
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
	reason := dqpComplexityGuardReason(query, 2, 3)
	if reason != dqpComplexityConstraintSetOpsDerived {
		t.Fatalf("unexpected complexity reason: %s", reason)
	}
	reason = dqpComplexityGuardReason(query, 4, 4)
	if reason != "" {
		t.Fatalf("expected thresholds to allow query, got %s", reason)
	}
}

func TestDQPComplexityGuardReasonAlwaysFalseJoinChain(t *testing.T) {
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
	reason := dqpComplexityGuardReason(query, 2, 3)
	if reason != dqpComplexityConstraintFalseJoinChain {
		t.Fatalf("unexpected complexity reason: %s", reason)
	}
}

func TestDQPComplexityGuardReasonJoinTablesWithCTE(t *testing.T) {
	cteQuery := &generator.SelectQuery{
		Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t1",
					On: generator.BinaryExpr{
						Left:  generator.LiteralExpr{Value: 1},
						Op:    "=",
						Right: generator.LiteralExpr{Value: 1},
					},
				},
			},
		},
	}
	query := &generator.SelectQuery{
		With: []generator.CTE{{Name: "c1", Query: cteQuery}},
		Items: []generator.SelectItem{
			{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "c1",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t2",
					On: generator.BinaryExpr{
						Left:  generator.LiteralExpr{Value: 1},
						Op:    "=",
						Right: generator.LiteralExpr{Value: 1},
					},
				},
			},
		},
	}
	reason := dqpComplexityGuardReason(query, 10, 10)
	if reason != dqpComplexityConstraintJoinTables {
		t.Fatalf("unexpected complexity reason: %s", reason)
	}
}

func TestDQPComplexityThresholdsFromConfig(t *testing.T) {
	cfg, err := config.Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Oracles.DQPComplexitySetOpsThreshold = 5
	cfg.Oracles.DQPComplexityDerivedThreshold = 6
	state := schema.State{}
	gen := generator.New(cfg, &state, 29)
	if got := dqpComplexitySetOpsThreshold(gen); got != 5 {
		t.Fatalf("unexpected dqp complexity set-ops threshold: %d", got)
	}
	if got := dqpComplexityDerivedThreshold(gen); got != 6 {
		t.Fatalf("unexpected dqp complexity derived threshold: %d", got)
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

func TestDQPVariantMetricsObserveVariant(t *testing.T) {
	metrics := dqpVariantMetrics{}
	hintHashJoin := "HASH_JOIN(t1, t2)"
	hintSetVar := "SET_VAR(tidb_opt_use_toja=OFF)"
	hintCombined := "SET_VAR(tidb_opt_use_toja=ON), HASH_JOIN(t1, t2)"
	metrics.observeVariant(
		"SELECT c1 FROM t1",
		"SELECT /*+ HASH_JOIN(t1, t2) */ c1 FROM t1",
		hintHashJoin,
	)
	metrics.observeVariant(
		"SELECT c1 FROM t1",
		"SELECT c1 FROM t1",
		hintSetVar,
	)
	metrics.observeVariant(
		"SELECT c1 FROM t1",
		"SELECT /*+ SET_VAR(tidb_opt_use_toja=ON), HASH_JOIN(t1, t2) */ c1 FROM t1",
		hintCombined,
	)

	if metrics.hintInjectedTotal != 2 {
		t.Fatalf("hintInjectedTotal=%d want=2", metrics.hintInjectedTotal)
	}
	if metrics.hintFallbackTotal != 1 {
		t.Fatalf("hintFallbackTotal=%d want=1", metrics.hintFallbackTotal)
	}
	if metrics.setVarVariantTotal != 2 {
		t.Fatalf("setVarVariantTotal=%d want=2", metrics.setVarVariantTotal)
	}
	if metrics.hintLengthCount != 3 {
		t.Fatalf("hintLengthCount=%d want=3", metrics.hintLengthCount)
	}
	if metrics.hintLengthMin != int64(len(hintHashJoin)) {
		t.Fatalf("hintLengthMin=%d want=%d", metrics.hintLengthMin, len(hintHashJoin))
	}
	if metrics.hintLengthMax != int64(len(hintCombined)) {
		t.Fatalf("hintLengthMax=%d want=%d", metrics.hintLengthMax, len(hintCombined))
	}
	if metrics.hintLengthSum != int64(len(hintHashJoin)+len(hintSetVar)+len(hintCombined)) {
		t.Fatalf("hintLengthSum=%d want=%d", metrics.hintLengthSum, len(hintHashJoin)+len(hintSetVar)+len(hintCombined))
	}
}

func TestDQPVariantMetricsResultMetrics(t *testing.T) {
	empty := dqpVariantMetrics{}
	if got := empty.resultMetrics(); got != nil {
		t.Fatalf("expected nil resultMetrics for empty stats, got %v", got)
	}
	metrics := dqpVariantMetrics{
		hintInjectedTotal:  4,
		hintFallbackTotal:  1,
		setVarVariantTotal: 3,
		hintLengthMin:      11,
		hintLengthMax:      37,
		hintLengthSum:      99,
		hintLengthCount:    4,
	}
	got := metrics.resultMetrics()
	if got["dqp_hint_injected_total"] != 4 {
		t.Fatalf("dqp_hint_injected_total=%d want=4", got["dqp_hint_injected_total"])
	}
	if got["dqp_hint_fallback_total"] != 1 {
		t.Fatalf("dqp_hint_fallback_total=%d want=1", got["dqp_hint_fallback_total"])
	}
	if got["dqp_set_var_variant_total"] != 3 {
		t.Fatalf("dqp_set_var_variant_total=%d want=3", got["dqp_set_var_variant_total"])
	}
	if got["dqp_hint_length_min"] != 11 {
		t.Fatalf("dqp_hint_length_min=%d want=11", got["dqp_hint_length_min"])
	}
	if got["dqp_hint_length_max"] != 37 {
		t.Fatalf("dqp_hint_length_max=%d want=37", got["dqp_hint_length_max"])
	}
	if got["dqp_hint_length_sum"] != 99 {
		t.Fatalf("dqp_hint_length_sum=%d want=99", got["dqp_hint_length_sum"])
	}
	if got["dqp_hint_length_count"] != 4 {
		t.Fatalf("dqp_hint_length_count=%d want=4", got["dqp_hint_length_count"])
	}
}

func TestDQPWarningSample(t *testing.T) {
	warnings := []string{"w1", "w2", "w3", "w4"}
	sample, omitted := dqpWarningSample(warnings, 2)
	if omitted != 2 {
		t.Fatalf("omitted=%d want=2", omitted)
	}
	if len(sample) != 2 || sample[0] != "w1" || sample[1] != "w2" {
		t.Fatalf("unexpected sample: %#v", sample)
	}

	all, omittedAll := dqpWarningSample(warnings, 10)
	if omittedAll != 0 {
		t.Fatalf("omittedAll=%d want=0", omittedAll)
	}
	if len(all) != 4 {
		t.Fatalf("len(all)=%d want=4", len(all))
	}
}

func TestDQPCompactSQL(t *testing.T) {
	sqlText := "SELECT  \n  *   FROM t0   WHERE id = 1"
	got := dqpCompactSQL(sqlText, 0)
	if got != "SELECT * FROM t0 WHERE id = 1" {
		t.Fatalf("compact sql=%q", got)
	}
	truncated := dqpCompactSQL(sqlText, 10)
	if truncated != "SELECT * F..." {
		t.Fatalf("truncated sql=%q", truncated)
	}
}

func TestDQPHintContainsSetVar(t *testing.T) {
	cases := []struct {
		hint string
		want bool
	}{
		{hint: "SET_VAR(tidb_opt_use_toja=ON)", want: true},
		{hint: "HASH_JOIN(t1, t2), SET_VAR(tidb_opt_use_toja=OFF)", want: true},
		{hint: "HASH_JOIN(t1, t2)", want: false},
		{hint: "", want: false},
	}
	for _, tc := range cases {
		if got := dqpHintContainsSetVar(tc.hint); got != tc.want {
			t.Fatalf("dqpHintContainsSetVar(%q)=%v want=%v", tc.hint, got, tc.want)
		}
	}
}
