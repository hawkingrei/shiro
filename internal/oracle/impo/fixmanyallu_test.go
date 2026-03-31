package impo

import "testing"

func TestFixMAnyAllGuardRejectsPotentiallyEmptySubquery(t *testing.T) {
	sql := "SELECT * FROM t0 WHERE t0.k1 <= ALL (SELECT t1.k1 FROM t1 WHERE t1.k0 = t0.k0)"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}
	if hasCandidate(v, FixMAnyAllU) {
		t.Fatalf("unexpected FixMAnyAllU candidate for potentially empty subquery")
	}
}

func TestFixMAnyAllGuardAllowsGuaranteedNonEmptyAggregate(t *testing.T) {
	sql := "SELECT * FROM t0 WHERE t0.k1 <= ANY (SELECT MAX(t1.k1) FROM t1 WHERE t1.k0 = t0.k0)"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}
	if !hasCandidate(v, FixMAnyAllL) {
		t.Fatalf("expected FixMAnyAllL candidate for guaranteed non-empty aggregate subquery")
	}
}

func TestFixMAnyAllGuardRejectsZeroRowLimit(t *testing.T) {
	sql := "SELECT * FROM t0 WHERE t0.k1 <= ALL (SELECT MAX(t1.k1) FROM t1 WHERE t1.k0 = t0.k0 LIMIT 0)"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}
	if hasCandidate(v, FixMAnyAllU) {
		t.Fatalf("unexpected FixMAnyAllU candidate when LIMIT 0 can force an empty subquery")
	}
}
