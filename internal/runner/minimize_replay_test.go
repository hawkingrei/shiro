package runner

import (
	"errors"
	"testing"

	"shiro/internal/oracle"

	"github.com/go-sql-driver/mysql"
)

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

func TestErrorMatchesByMySQLErrorCodeOnly(t *testing.T) {
	expected := &mysql.MySQLError{Number: 1105, Message: "Can't find column Column#123"}
	got := &mysql.MySQLError{Number: 1105, Message: "Can't find column Column#456"}
	if !errorMatches(got, expected) {
		t.Fatalf("expected same mysql error code to match")
	}

	differentCode := &mysql.MySQLError{Number: 1054, Message: "Unknown column"}
	if errorMatches(differentCode, expected) {
		t.Fatalf("expected different mysql error code not to match")
	}
}

func TestErrorMatchesNoTextFallbackForNonMySQLErrors(t *testing.T) {
	expected := errors.New("some runtime failure")
	got := errors.New("some runtime failure")
	if errorMatches(got, expected) {
		t.Fatalf("expected non-mysql errors not to match by text")
	}
}

func TestBuildReplaySpecPrefersDetailsForErrorResult(t *testing.T) {
	spec := buildReplaySpec(oracle.Result{
		Err: errors.New("signature execution failed"),
		Details: map[string]any{
			"replay_kind":         "error_sql",
			"replay_expected_sql": "SELECT COUNT(*) FROM (SELECT 1) q",
			"replay_set_var":      "tidb_allow_mpp=OFF",
		},
	})
	if spec.kind != "error_sql" {
		t.Fatalf("expected replay kind error_sql, got %q", spec.kind)
	}
	if spec.expectedSQL != "SELECT COUNT(*) FROM (SELECT 1) q" {
		t.Fatalf("unexpected expectedSQL: %q", spec.expectedSQL)
	}
	if spec.setVar != "tidb_allow_mpp=OFF" {
		t.Fatalf("unexpected setVar: %q", spec.setVar)
	}
}

func TestBuildReplaySpecFallsBackToCaseErrorWithoutReplayKind(t *testing.T) {
	spec := buildReplaySpec(oracle.Result{
		Err:     errors.New("query failed"),
		Details: map[string]any{"error_reason": "dqp:missing_column"},
	})
	if spec.kind != "case_error" {
		t.Fatalf("expected case_error fallback, got %q", spec.kind)
	}
}
