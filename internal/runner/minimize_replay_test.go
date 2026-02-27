package runner

import "testing"

func TestWrapReplayInsertsWithForeignKeyChecks(t *testing.T) {
	got := wrapReplayInsertsWithForeignKeyChecks([]string{
		" INSERT INTO t VALUES (1) ",
		"",
		"\tINSERT INTO t VALUES (2)\n",
	})
	want := []string{
		replayForeignKeyChecksOffSQL,
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		replayForeignKeyChecksOnSQL,
	}
	if len(got) != len(want) {
		t.Fatalf("wrapped len=%d want=%d got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("wrapped[%d]=%q want=%q", i, got[i], want[i])
		}
	}
}

func TestWrapReplayInsertsWithForeignKeyChecksEmpty(t *testing.T) {
	got := wrapReplayInsertsWithForeignKeyChecks(nil)
	want := []string{replayForeignKeyChecksOffSQL, replayForeignKeyChecksOnSQL}
	if len(got) != len(want) {
		t.Fatalf("wrapped len=%d want=%d got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("wrapped[%d]=%q want=%q", i, got[i], want[i])
		}
	}
}
