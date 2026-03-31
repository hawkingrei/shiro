package runner

import (
	"context"
	"strings"
	"testing"

	"shiro/internal/oracle"
	"shiro/internal/schema"
)

func TestValidatedMergedInsertsRespectsReplay(t *testing.T) {
	inserts := []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
	}
	kept := validatedMergedInserts(inserts, func(stmts []string) bool {
		return len(stmts) > 1
	})
	if len(kept) != len(inserts) {
		t.Fatalf("validatedMergedInserts should keep original when merged replay fails, got=%v", kept)
	}

	merged := validatedMergedInserts(inserts, func(_ []string) bool {
		return true
	})
	if len(merged) >= len(inserts) {
		t.Fatalf("validatedMergedInserts should adopt merged inserts, got=%v", merged)
	}
}

func TestReduceReplaySpecCandidateDropsSetVar(t *testing.T) {
	inserts := []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
	}
	spec := replaySpec{
		kind:        "signature",
		expectedSQL: "SELECT 1",
		actualSQL:   "SELECT 2",
		setVar:      "tidb_enable_index_merge=0",
	}
	gotInserts, gotSpec := reduceReplaySpecCandidate(context.Background(), 4, inserts, spec, true, func(stmts []string, current replaySpec) bool {
		// Keep this test focused on setVar dropping by disallowing insert-count changes.
		if len(stmts) != len(inserts) {
			return false
		}
		return strings.TrimSpace(current.expectedSQL) != "" && strings.TrimSpace(current.actualSQL) != ""
	})
	if len(gotInserts) != len(inserts) {
		t.Fatalf("unexpected insert change: got=%v", gotInserts)
	}
	if gotSpec.setVar != "" {
		t.Fatalf("expected setVar to be dropped, got=%q", gotSpec.setVar)
	}
}

func TestReduceCaseErrorCandidateAlternatesReduction(t *testing.T) {
	inserts := []string{
		"insert_keep",
		"insert_drop_1",
		"insert_drop_2",
	}
	caseSQL := []string{
		"case_drop_1",
		"case_keep",
		"case_drop_2",
	}
	check := func(stmts []string, steps []string) bool {
		return containsLiteral(stmts, "insert_keep") && containsLiteral(steps, "case_keep")
	}

	gotInserts, gotCase := reduceCaseErrorCandidate(context.Background(), 6, inserts, caseSQL, check)
	if !check(gotInserts, gotCase) {
		t.Fatalf("reduced candidate is not reproducible: inserts=%v case=%v", gotInserts, gotCase)
	}
	if len(gotInserts) >= len(inserts) {
		t.Fatalf("expected inserts to shrink, got=%v", gotInserts)
	}
	if len(gotCase) >= len(caseSQL) {
		t.Fatalf("expected case SQL to shrink, got=%v", gotCase)
	}
}

func containsLiteral(sqls []string, target string) bool {
	for _, sqlText := range sqls {
		if sqlText == target {
			return true
		}
	}
	return false
}

func TestReplayConsensus(t *testing.T) {
	results := []bool{true, false, true}
	idx := 0
	ok := replayConsensus(func() bool {
		v := results[idx]
		idx++
		return v
	}, 3, 2)
	if !ok {
		t.Fatalf("expected 2/3 success to pass consensus")
	}

	idx = 0
	failures := []bool{false, true, false}
	ok = replayConsensus(func() bool {
		v := failures[idx]
		idx++
		return v
	}, 3, 2)
	if ok {
		t.Fatalf("expected 1/3 success to fail consensus")
	}

	if !replayConsensus(nil, 0, 0) {
		t.Fatalf("required<=0 should pass immediately")
	}
	if replayConsensus(nil, 3, 1) {
		t.Fatalf("nil callback should fail when success is required")
	}
	if replayConsensus(func() bool { return true }, 1, 2) {
		t.Fatalf("attempts less than required should fail")
	}
	if replayConsensus(func() bool { return true }, 0, 1) {
		t.Fatalf("non-positive attempts should fail when success is required")
	}
}

func TestMinimizeBaseReplayGate(t *testing.T) {
	results := []bool{true, false, true}
	idx := 0
	ok, flaky := minimizeBaseReplayGate(func() bool {
		v := results[idx]
		idx++
		return v
	}, "signature")
	if !ok || flaky {
		t.Fatalf("expected strict base replay gate pass without flaky, got ok=%v flaky=%v", ok, flaky)
	}

	results = []bool{false, true, false, false, false, true}
	idx = 0
	ok, flaky = minimizeBaseReplayGate(func() bool {
		v := results[idx]
		idx++
		return v
	}, "case_error")
	if !ok || !flaky {
		t.Fatalf("expected case_error fallback base replay pass with flaky=true, got ok=%v flaky=%v", ok, flaky)
	}

	results = []bool{false, true, false, false, false, true}
	idx = 0
	ok, flaky = minimizeBaseReplayGate(func() bool {
		v := results[idx]
		idx++
		return v
	}, "signature")
	if ok || !flaky {
		t.Fatalf("expected non-case_error base replay gate failure, got ok=%v flaky=%v", ok, flaky)
	}
}

func TestMinimizeBaseReplayGateDetailedKeepsFailureDiagnostics(t *testing.T) {
	attempts := []replayAttemptResult{
		{
			diag: replayFailureDiagnostic{
				replayKind:           "case_error",
				outcome:              "error_mismatch",
				failureStage:         "exec_case_sql",
				expectedErrorReason:  "eet:missing_column",
				actualErrorReason:    "eet:sql_error_1054",
				actualErrorSignature: "eet:sql_error_1054|unknown_column",
			},
		},
		{
			diag: replayFailureDiagnostic{
				replayKind:          "case_error",
				outcome:             "error_mismatch",
				failureStage:        "exec_case_sql",
				expectedErrorReason: "eet:missing_column",
				actualErrorReason:   "eet:sql_error_1054",
			},
		},
		{
			matched: true,
		},
		{
			diag: replayFailureDiagnostic{
				replayKind:        "case_error",
				outcome:           "error_mismatch",
				failureStage:      "exec_case_sql",
				actualErrorReason: "eet:sql_error_1105",
			},
		},
		{
			diag: replayFailureDiagnostic{
				replayKind:        "case_error",
				outcome:           "error_mismatch",
				failureStage:      "exec_case_sql",
				actualErrorReason: "eet:sql_error_1105",
			},
		},
		{
			matched: true,
		},
	}
	idx := 0
	outcome := minimizeBaseReplayGateDetailed(func() replayAttemptResult {
		current := attempts[idx]
		idx++
		return current
	}, "case_error")
	if !outcome.ok || !outcome.flaky {
		t.Fatalf("expected fallback pass with flaky=true, got ok=%v flaky=%v", outcome.ok, outcome.flaky)
	}
	if outcome.diag.expectedErrorReason != "eet:missing_column" {
		t.Fatalf("expectedErrorReason=%q want eet:missing_column", outcome.diag.expectedErrorReason)
	}
	if outcome.diag.actualErrorReason != "eet:sql_error_1054" {
		t.Fatalf("actualErrorReason=%q want strict-gate failure diag", outcome.diag.actualErrorReason)
	}
}

func TestBuildReproSQLErrorSQLKind(t *testing.T) {
	schemaSQL := []string{"CREATE TABLE t(id INT)"}
	inserts := []string{"INSERT INTO t VALUES (1)"}
	spec := replaySpec{
		kind:        "error_sql",
		setVar:      "tidb_allow_mpp=OFF",
		expectedSQL: "SELECT COUNT(*) FROM (SELECT * FROM t) q",
	}
	out := buildReproSQL(schemaSQL, inserts, nil, spec)
	if len(out) != 4 {
		t.Fatalf("unexpected repro sql length: %d (%v)", len(out), out)
	}
	if out[2] != "SET SESSION tidb_allow_mpp=OFF" {
		t.Fatalf("expected set var before error sql, got %q", out[2])
	}
	if out[3] != "SELECT COUNT(*) FROM (SELECT * FROM t) q" {
		t.Fatalf("unexpected replay sql: %q", out[3])
	}
}

func TestExpandMinimizeTablesForViewDependencies(t *testing.T) {
	r := &Runner{
		state: &schema.State{
			Tables: []schema.Table{
				{Name: "t0"},
				{Name: "t1"},
				{Name: "v0", IsView: true},
			},
		},
	}
	input := map[string]struct{}{"v0": {}}
	got := r.expandMinimizeTablesForViewDependencies(input)
	if len(got) != 3 {
		t.Fatalf("expected all tables to be included when view is referenced, got=%v", got)
	}
	for _, name := range []string{"t0", "t1", "v0"} {
		if _, ok := got[name]; !ok {
			t.Fatalf("expected expanded tables to include %s, got=%v", name, got)
		}
	}
}

func TestExpandMinimizeTablesForViewDependenciesNoViewReference(t *testing.T) {
	r := &Runner{
		state: &schema.State{
			Tables: []schema.Table{
				{Name: "t0"},
				{Name: "v0", IsView: true},
			},
		},
	}
	input := map[string]struct{}{"t0": {}}
	got := r.expandMinimizeTablesForViewDependencies(input)
	if len(got) != 1 {
		t.Fatalf("expected input table set to remain unchanged, got=%v", got)
	}
	if _, ok := got["t0"]; !ok {
		t.Fatalf("expected table t0 to remain in set, got=%v", got)
	}
}

func TestReplayShapePreservedFixMAnyAll(t *testing.T) {
	cases := []struct {
		name     string
		mutation string
		spec     replaySpec
		want     bool
	}{
		{
			name:     "FixMAnyAllU preserved",
			mutation: "FixMAnyAllU",
			spec: replaySpec{
				kind:        "impo_contains",
				expectedSQL: "SELECT * FROM t0 WHERE t0.k1 <= ALL (SELECT t1.k1 FROM t1)",
				actualSQL:   "SELECT * FROM t0 WHERE t0.k1 <= ANY (SELECT t1.k1 FROM t1)",
			},
			want: true,
		},
		{
			name:     "FixMAnyAllU reduced away",
			mutation: "FixMAnyAllU",
			spec: replaySpec{
				kind:        "impo_contains",
				expectedSQL: "SELECT 1 FROM t0",
				actualSQL:   "SELECT 1 FROM t0 JOIN (SELECT 1 FROM t1) AS t1",
			},
			want: false,
		},
		{
			name:     "FixMAnyAllL preserved",
			mutation: "FixMAnyAllL",
			spec: replaySpec{
				kind:        "impo_contains",
				expectedSQL: "SELECT * FROM t0 WHERE t0.k1 <= ANY (SELECT t1.k1 FROM t1)",
				actualSQL:   "SELECT * FROM t0 WHERE t0.k1 <= ALL (SELECT t1.k1 FROM t1)",
			},
			want: true,
		},
		{
			name:     "other mutation bypasses shape validator",
			mutation: "FixMCmpOpU",
			spec: replaySpec{
				kind:        "impo_contains",
				expectedSQL: "SELECT 1 FROM t0",
				actualSQL:   "SELECT 1 FROM t0",
			},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validator := buildReplayShapeValidator(oracle.Result{
				Details: map[string]any{
					"impo_mutation": tc.mutation,
				},
			}, tc.spec.kind)
			if got := validator.preserved(tc.spec); got != tc.want {
				t.Fatalf("validator.preserved()=%v want=%v", got, tc.want)
			}
		})
	}
}
