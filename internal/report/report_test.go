package report

import (
	"encoding/json"
	"os"
	"path/filepath"
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

func TestRecoverInterruptedMinimizeCases(t *testing.T) {
	t.Parallel()
	outputDir := t.TempDir()
	reporter := New(outputDir, 16)

	case1Dir := filepath.Join(outputDir, "case_1")
	if err := os.MkdirAll(case1Dir, 0o755); err != nil {
		t.Fatalf("mkdir case1: %v", err)
	}
	case1Summary := Summary{
		CaseID:         "c1",
		MinimizeStatus: "in_progress",
		Details: map[string]any{
			"oracle_note": "pqs",
		},
	}
	if err := writeSummaryJSON(filepath.Join(case1Dir, "summary.json"), case1Summary); err != nil {
		t.Fatalf("write case1 summary: %v", err)
	}
	if err := writeSummaryJSON(filepath.Join(case1Dir, "report.json"), case1Summary); err != nil {
		t.Fatalf("write case1 report: %v", err)
	}

	case2Dir := filepath.Join(outputDir, "case_2")
	if err := os.MkdirAll(case2Dir, 0o755); err != nil {
		t.Fatalf("mkdir case2: %v", err)
	}
	case2Summary := Summary{
		CaseID:         "c2",
		MinimizeStatus: "success",
	}
	if err := writeSummaryJSON(filepath.Join(case2Dir, "summary.json"), case2Summary); err != nil {
		t.Fatalf("write case2 summary: %v", err)
	}

	case3Dir := filepath.Join(outputDir, "case_3")
	if err := os.MkdirAll(case3Dir, 0o755); err != nil {
		t.Fatalf("mkdir case3: %v", err)
	}
	if err := os.WriteFile(filepath.Join(case3Dir, "summary.json"), []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write case3 summary: %v", err)
	}

	updated, err := reporter.RecoverInterruptedMinimizeCases("runner_recovered_interrupted")
	if err != nil {
		t.Fatalf("recover interrupted: %v", err)
	}
	if updated != 1 {
		t.Fatalf("updated=%d want=1", updated)
	}

	gotCase1Summary, err := readSummaryJSON(filepath.Join(case1Dir, "summary.json"))
	if err != nil {
		t.Fatalf("read case1 summary: %v", err)
	}
	if gotCase1Summary.MinimizeStatus != "interrupted" {
		t.Fatalf("case1 minimize_status=%q want=interrupted", gotCase1Summary.MinimizeStatus)
	}
	if gotCase1Summary.Details["minimize_reason"] != "runner_recovered_interrupted" {
		t.Fatalf("case1 minimize_reason=%v want=runner_recovered_interrupted", gotCase1Summary.Details["minimize_reason"])
	}

	gotCase1Report, err := readSummaryJSON(filepath.Join(case1Dir, "report.json"))
	if err != nil {
		t.Fatalf("read case1 report: %v", err)
	}
	if gotCase1Report.MinimizeStatus != "interrupted" {
		t.Fatalf("case1 report minimize_status=%q want=interrupted", gotCase1Report.MinimizeStatus)
	}

	gotCase2Summary, err := readSummaryJSON(filepath.Join(case2Dir, "summary.json"))
	if err != nil {
		t.Fatalf("read case2 summary: %v", err)
	}
	if gotCase2Summary.MinimizeStatus != "success" {
		t.Fatalf("case2 minimize_status=%q want=success", gotCase2Summary.MinimizeStatus)
	}
}

func writeSummaryJSON(path string, summary Summary) error {
	data, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func readSummaryJSON(path string) (Summary, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Summary{}, err
	}
	var summary Summary
	if err := json.Unmarshal(data, &summary); err != nil {
		return Summary{}, err
	}
	return summary, nil
}
