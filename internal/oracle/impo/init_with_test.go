package impo

import "testing"

func TestInitWithClause(t *testing.T) {
	sql := "WITH cte AS (SELECT 1 AS c) SELECT c FROM cte"
	out, err := InitWithOptions(sql, InitOptions{})
	if err != nil {
		t.Fatalf("expected with clause accepted, got %v", err)
	}
	if out == "" {
		t.Fatalf("expected non-empty output")
	}
}

func TestInitWithRecursiveClause(t *testing.T) {
	sql := "WITH RECURSIVE cte AS (SELECT 1 AS c UNION ALL SELECT c+1 FROM cte WHERE c < 3) SELECT c FROM cte"
	_, err := InitWithOptions(sql, InitOptions{})
	if err == nil {
		t.Fatalf("expected recursive with clause rejected")
	}
}
