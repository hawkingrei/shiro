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

func TestValidateLateralGroupedOutputAliasSQL(t *testing.T) {
	v := New()
	sql := "SELECT id AS merged_id, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt FROM t0 JOIN t1 USING (id) JOIN LATERAL (SELECT agg.g0 AS g0, agg.cnt AS cnt FROM (SELECT t2.id AS g0, COUNT(1) AS cnt FROM t2 GROUP BY t2.id) AS agg WHERE (agg.g0 = id) ORDER BY agg.g0) AS dt ON (1 = 1) ORDER BY 1, 2, 3"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected grouped-output-alias LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralGroupedOutputOrderLimitSQL(t *testing.T) {
	v := New()
	sql := "SELECT id AS merged_id, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt FROM t0 JOIN t1 USING (id) JOIN LATERAL (SELECT t2.id AS g0, COUNT(1) AS cnt FROM t2 WHERE (t2.id <> id) GROUP BY t2.id HAVING (ABS(CASE WHEN (t2.id >= id) THEN (COUNT(1) - id) ELSE ((COUNT(1) + t2.id) - id) END) >= 1) ORDER BY g0, cnt DESC, id LIMIT 1) AS dt ON (1 = 1) ORDER BY 1, 2, 3"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected grouped-output-order-limit LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralProjectedOrderLimitSQL(t *testing.T) {
	v := New()
	sql := "SELECT id AS merged_id, dt.score0 AS lateral_score0, dt.tie0 AS lateral_tie0 FROM t0 JOIN t1 USING (id) JOIN LATERAL (SELECT ABS(CASE WHEN (t2.id >= id) THEN CASE WHEN (t2.id >= 0) THEN (t2.id - id) ELSE id END ELSE CASE WHEN (t2.id >= 0) THEN (id - t2.id) ELSE t2.id END END) AS score0, ABS(CASE WHEN (t2.id >= 0) THEN (t2.id + id) ELSE (id - t2.id) END) AS tie0 FROM t2 WHERE (t2.id <> id) ORDER BY score0, tie0 DESC, id LIMIT 1) AS dt ON (1 = 1) ORDER BY 1, 2, 3"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected projected-order-limit LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralScalarSubqueryProjectedOrderLimitSQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.score0 AS lateral_score0, dt.tie0 AS lateral_tie0 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT ABS((t2.c2 - (SELECT sq.v0 AS sv0 FROM t2 AS sq WHERE ((sq.id = t0.id) AND (sq.c2 <> t1.c1)) ORDER BY ABS((sq.c2 - t1.c1)), sq.v0 DESC, t0.c0 LIMIT 1))) AS score0, (SELECT sq.v0 AS sv0 FROM t2 AS sq WHERE ((sq.id = t0.id) AND (sq.c2 <> t1.c1)) ORDER BY ABS((sq.c2 - t1.c1)), sq.v0 DESC, t0.c0 LIMIT 1) AS tie0 FROM t2 WHERE (t2.id = t0.id) ORDER BY score0, tie0 DESC, t0.c0 LIMIT 1) AS dt ON ((dt.tie0 >= t1.c1) AND (dt.score0 <= ABS((t0.c0 - t1.c1)))) ORDER BY ABS((dt.tie0 - t1.c1)), dt.score0, t0.id"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected scalar-subquery projected-order-limit LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralMultiOuterProjectedOrderLimitSQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.score0 AS lateral_score0, dt.tie0 AS lateral_tie0 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT ABS(CASE WHEN (t2.c2 >= t1.c1) THEN CASE WHEN (t2.v0 >= t0.c0) THEN (t2.c2 - t1.c1) ELSE t0.c0 END ELSE CASE WHEN (t2.v0 >= t0.c0) THEN (t1.c1 - t2.c2) ELSE t2.v0 END END) AS score0, ABS(CASE WHEN (t2.v0 >= t0.c0) THEN (t2.v0 + t0.c0) ELSE (t1.c1 - t2.v0) END) AS tie0 FROM t2 WHERE ((t2.id = t0.id) AND (t2.c2 <> t1.c1)) ORDER BY score0, tie0 DESC, t1.c1 LIMIT 1) AS dt ON (1 = 1) ORDER BY 1, 2, 3, 4"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected multi-outer projected-order-limit LATERAL SQL to parse, got %v", err)
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

func TestValidateLateralCaseCorrelatedGroupKeySQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT CASE WHEN (t2.c2 >= t1.c1) THEN t2.c2 ELSE t1.c1 END AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY CASE WHEN (t2.c2 >= t1.c1) THEN t2.c2 ELSE t1.c1 END) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected case-correlated grouped key LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralNestedCaseCorrelatedGroupKeySQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT CASE WHEN (t2.c2 >= t1.c1) THEN CASE WHEN (t2.v0 >= t1.c1) THEN t2.c2 ELSE t1.c1 END ELSE CASE WHEN (t2.v0 >= t1.c1) THEN t1.c1 ELSE t2.c2 END END AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY CASE WHEN (t2.c2 >= t1.c1) THEN CASE WHEN (t2.v0 >= t1.c1) THEN t2.c2 ELSE t1.c1 END ELSE CASE WHEN (t2.v0 >= t1.c1) THEN t1.c1 ELSE t2.c2 END END) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected nested-case-correlated grouped key LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralWrappedNestedCaseCorrelatedGroupKeySQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT ABS(CASE WHEN (t2.c2 >= t1.c1) THEN CASE WHEN (t2.v0 >= t1.c1) THEN (t2.c2 - t1.c1) ELSE (t1.c1 - t2.c2) END ELSE CASE WHEN (t2.v0 >= t1.c1) THEN (t1.c1 - t2.c2) ELSE (t2.c2 - t1.c1) END END) AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY ABS(CASE WHEN (t2.c2 >= t1.c1) THEN CASE WHEN (t2.v0 >= t1.c1) THEN (t2.c2 - t1.c1) ELSE (t1.c1 - t2.c2) END ELSE CASE WHEN (t2.v0 >= t1.c1) THEN (t1.c1 - t2.c2) ELSE (t2.c2 - t1.c1) END END)) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected wrapped-nested-case-correlated grouped key LATERAL SQL to parse, got %v", err)
	}
}

func TestValidateLateralAggregateValuedHavingSQL(t *testing.T) {
	v := New()
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT t2.c2 AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY t2.c2 HAVING (SUM(t2.v0) >= t1.c1)) AS dt ON (1 = 1) ORDER BY 1, 2"
	if err := v.Validate(sql); err != nil {
		t.Fatalf("expected aggregate-valued HAVING LATERAL SQL to parse, got %v", err)
	}
}
