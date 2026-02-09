package report

import (
	"strings"
	"testing"
)

func TestNormalizeCreateViewStripsDefinerAndDatabase(t *testing.T) {
	input := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `testdb`.`v0` AS SELECT `testdb`.`t0`.`id` FROM `testdb`.`t0`"
	out := normalizeCreateView(input, "testdb")

	if want := "SQL SECURITY INVOKER"; !strings.Contains(out, want) {
		t.Fatalf("expected security invoker, got %s", out)
	}
	if strings.Contains(out, "DEFINER=") {
		t.Fatalf("expected definer to be stripped, got %s", out)
	}
	if strings.Contains(out, "`testdb`.") {
		t.Fatalf("expected database qualifier removed, got %s", out)
	}
	if !strings.Contains(out, "VIEW `v0`") {
		t.Fatalf("expected view name without db qualifier, got %s", out)
	}
	if !strings.Contains(out, "FROM `t0`") {
		t.Fatalf("expected table name without db qualifier, got %s", out)
	}
}
