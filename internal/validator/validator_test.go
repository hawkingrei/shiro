package validator

import "testing"

func TestValidateLateralJoinSQL(t *testing.T) {
	v := New()
	sql := "SELECT * FROM t0 CROSS JOIN LATERAL (SELECT t0.c0) AS dt"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralMergedColumnSQL(t *testing.T) {
	v := New()
	sql := "SELECT id AS merged_id, dt.id AS lateral_id FROM t0 JOIN t1 USING (id) JOIN LATERAL (SELECT t2.id AS id FROM t2 WHERE (t2.id = id)) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected merged-column LATERAL SQL to parse, got %v", err)
	}
}
