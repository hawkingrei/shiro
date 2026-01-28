package impo

import "testing"

func TestDistinctNestedSubqueryGuard(t *testing.T) {
	sqlSafe := "SELECT * FROM t0 WHERE k0 IN (SELECT k0 FROM t1) ORDER BY k0"
	v, err := CalCandidates(sqlSafe)
	if err != nil {
		t.Fatalf("CalCandidates safe: %v", err)
	}
	if !hasCandidate(v, FixMDistinctL) {
		t.Fatalf("expected FixMDistinctL for safe nested subquery")
	}

	sqlOrder := "SELECT * FROM t0 WHERE k0 IN (SELECT k0 FROM t1 ORDER BY k0) ORDER BY k0"
	v, err = CalCandidates(sqlOrder)
	if err != nil {
		t.Fatalf("CalCandidates order: %v", err)
	}
	if hasCandidate(v, FixMDistinctL) {
		t.Fatalf("unexpected FixMDistinctL for ORDER BY subquery")
	}
}

func TestDistinctTopLevelCandidates(t *testing.T) {
	sqlDistinct := "SELECT DISTINCT k0 FROM t0"
	v, err := CalCandidates(sqlDistinct)
	if err != nil {
		t.Fatalf("CalCandidates distinct: %v", err)
	}
	if !hasCandidate(v, FixMDistinctU) {
		t.Fatalf("expected FixMDistinctU for top-level DISTINCT")
	}

	sqlPlain := "SELECT k0 FROM t0"
	v, err = CalCandidates(sqlPlain)
	if err != nil {
		t.Fatalf("CalCandidates plain: %v", err)
	}
	if !hasCandidate(v, FixMDistinctL) {
		t.Fatalf("expected FixMDistinctL for top-level non-DISTINCT")
	}
}

func TestDistinctNestedSubqueryContexts(t *testing.T) {
	cases := []string{
		"SELECT * FROM t0 WHERE k0 IN (SELECT k0 FROM t1) ORDER BY k0",
		"SELECT * FROM t0 WHERE EXISTS (SELECT k0 FROM t1) ORDER BY k0",
		"SELECT * FROM t0 WHERE k0 > ANY (SELECT k0 FROM t1) ORDER BY k0",
		"SELECT * FROM t0 WHERE k0 > ALL (SELECT k0 FROM t1) ORDER BY k0",
	}
	for _, sql := range cases {
		v, err := CalCandidates(sql)
		if err != nil {
			t.Fatalf("CalCandidates %q: %v", sql, err)
		}
		if !hasCandidate(v, FixMDistinctL) {
			t.Fatalf("expected FixMDistinctL for %q", sql)
		}
	}
}

func hasCandidate(v *MutateVisitor, name string) bool {
	if v == nil {
		return false
	}
	return len(v.Candidates[name]) > 0
}
