package runner

import (
	"context"
	"strings"
	"testing"
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
}
