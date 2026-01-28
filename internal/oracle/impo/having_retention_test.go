package impo

import (
	"strings"
	"testing"
)

func TestInitKeepsHavingGroupBy(t *testing.T) {
	sql := "SELECT k0, COUNT(1) FROM t0 GROUP BY k0 HAVING COUNT(1) > 0 ORDER BY k0 LIMIT 3"
	out, err := Init(sql)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if !containsSQL(out, "GROUP BY") {
		t.Fatalf("expected GROUP BY retained, got %s", out)
	}
	if !containsSQL(out, "HAVING") {
		t.Fatalf("expected HAVING retained, got %s", out)
	}
	if !containsSQL(out, "COUNT") {
		t.Fatalf("expected aggregate retained, got %s", out)
	}
	if containsSQL(out, "ORDER BY") {
		t.Fatalf("expected ORDER BY removed, got %s", out)
	}
	if !containsSQL(out, "LIMIT") {
		t.Fatalf("expected LIMIT retained, got %s", out)
	}
	if !containsSQL(out, "2147483647") {
		t.Fatalf("expected LIMIT normalized, got %s", out)
	}
}


func containsSQL(sql string, token string) bool {
	return strings.Contains(strings.ToUpper(sql), strings.ToUpper(token))
}
