package impo

import "testing"

func TestLimitExpansionNestedSubquery(t *testing.T) {
	sql := "SELECT * FROM t0 WHERE k0 IN (SELECT k0 FROM t1 ORDER BY k0 LIMIT 1) ORDER BY k0"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}
	if !hasCandidate(v, FixMLimitU) {
		t.Fatalf("expected FixMLimitU candidate")
	}
}

func TestLimitExpansionGuardNoOrder(t *testing.T) {
	sql := "SELECT * FROM t0 WHERE k0 IN (SELECT k0 FROM t1 LIMIT 1) ORDER BY k0"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}
	if hasCandidate(v, FixMLimitU) {
		t.Fatalf("unexpected FixMLimitU without ORDER BY")
	}
}
