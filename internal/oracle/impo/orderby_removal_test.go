package impo

import "testing"

func TestOrderByRemovalNestedSubquery(t *testing.T) {
	sql := "SELECT * FROM t0 WHERE k0 IN (SELECT k0 FROM t1 ORDER BY k0) ORDER BY k0"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}
	if !hasCandidate(v, FixMRmOrderByL) {
		t.Fatalf("expected FixMRmOrderByL candidate")
	}
}

func TestOrderByRemovalGuardLimit(t *testing.T) {
	sql := "SELECT * FROM t0 WHERE k0 IN (SELECT k0 FROM t1 ORDER BY k0 LIMIT 1) ORDER BY k0"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}
	if hasCandidate(v, FixMRmOrderByL) {
		t.Fatalf("unexpected FixMRmOrderByL with LIMIT")
	}
}
