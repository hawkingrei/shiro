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

func TestFixMAnyAllGuardAllowsNoFromSubquery(t *testing.T) {
	sql := "SELECT * FROM t0 WHERE t0.k1 <= ALL (SELECT 1)"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}
	if !hasCandidate(v, FixMAnyAllU) {
		t.Fatalf("expected FixMAnyAllU candidate for no-FROM subquery")
	}
}

func TestFixMAnyAllGuardRejectsNestedSubqueryAggregate(t *testing.T) {
	sql := "SELECT * FROM t0 WHERE t0.k1 <= ALL (SELECT (SELECT COUNT(*) FROM t2) FROM t1 WHERE t1.k0 = t0.k0)"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}
	if hasCandidate(v, FixMAnyAllU) {
		t.Fatalf("unexpected FixMAnyAllU candidate when aggregate only appears inside a nested subquery")
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
