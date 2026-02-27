package impo

import "testing"

func TestSetOprExceptFlipsRightBranchFlag(t *testing.T) {
	sql := "SELECT * FROM t0 JOIN t1 ON t0.id = t1.id EXCEPT SELECT * FROM t2 JOIN t3 ON t2.id = t3.id"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}

	candidates := v.Candidates[FixMOn1U]
	if len(candidates) != 2 {
		t.Fatalf("expected 2 FixMOn1U candidates, got %d", len(candidates))
	}

	hasFlagPositive := false
	hasFlagNegative := false
	for _, candidate := range candidates {
		switch candidate.Flag {
		case 1:
			hasFlagPositive = true
		case 0:
			hasFlagNegative = true
		}
	}
	if !hasFlagPositive || !hasFlagNegative {
		t.Fatalf("expected FixMOn1U flags to include both 1(left) and 0(right), got %#v", candidates)
	}
}

func TestSetOprUnionKeepsBranchFlag(t *testing.T) {
	sql := "SELECT * FROM t0 JOIN t1 ON t0.id = t1.id UNION SELECT * FROM t2 JOIN t3 ON t2.id = t3.id"
	v, err := CalCandidates(sql)
	if err != nil {
		t.Fatalf("CalCandidates: %v", err)
	}

	candidates := v.Candidates[FixMOn1U]
	if len(candidates) != 2 {
		t.Fatalf("expected 2 FixMOn1U candidates, got %d", len(candidates))
	}
	for _, candidate := range candidates {
		if candidate.Flag != 1 {
			t.Fatalf("expected UNION branch flag=1, got %d", candidate.Flag)
		}
	}
}
