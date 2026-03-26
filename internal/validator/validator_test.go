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

func TestValidateLateralAggregateSQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE ((t2.id = t0.id) AND (t2.c2 = t1.c1))) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected aggregate LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralGroupedAggregateSQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT t2.c2 AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY t2.c2 HAVING (t2.c2 = t1.c1)) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected grouped aggregate LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralOuterFilteredGroupedAggregateSQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT t2.c2 AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE ((t2.id = t0.id) AND (t2.v0 >= t1.c1)) GROUP BY t2.c2) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected outer-filtered grouped aggregate LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralMultiFilteredGroupedAggregateSQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT t2.c2 AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (((t2.id = t0.id) AND (t2.c2 = t1.c1)) AND (t2.v0 >= t1.c1)) GROUP BY t2.c2) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected multi-filtered grouped aggregate LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralOuterCorrelatedGroupKeySQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT (t2.c2 + t1.c1) AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY (t2.c2 + t1.c1)) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected outer-correlated grouped key LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralAggregateValuedHavingSQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT t2.c2 AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY t2.c2 HAVING (SUM(t2.v0) >= t1.c1)) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected aggregate-valued HAVING LATERAL SQL to parse, got %v", err)
	}
}
