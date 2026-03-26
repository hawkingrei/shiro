package runner

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

func TestSelectCandidatesKeepCTEWhenReferenced(t *testing.T) {
	sql := "WITH cte_0 AS (SELECT 1 AS c0) SELECT c0 FROM cte_0"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	for _, cand := range candidates {
		upper := strings.ToUpper(cand)
		if strings.Contains(upper, "CTE_0") && !strings.Contains(upper, "WITH") {
			t.Fatalf("cte removed while still referenced: %s", cand)
		}
	}
}

func TestSelectCandidatesDropSingleCTEFromWithList(t *testing.T) {
	sql := "WITH cte_0 AS (SELECT 1 AS c0), cte_1 AS (SELECT 2 AS c1) SELECT c0 FROM cte_0"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	found := false
	for _, cand := range candidates {
		candNode, err := p.ParseOneStmt(cand, "", "")
		if err != nil {
			continue
		}
		candSel, ok := candNode.(*ast.SelectStmt)
		if !ok || candSel.With == nil || len(candSel.With.CTEs) != 1 {
			continue
		}
		found = true
		break
	}
	if !found {
		t.Fatalf("expected candidate that drops one CTE from WITH list")
	}
}

func TestSelectCandidatesHandleLateralJoin(t *testing.T) {
	sql := "SELECT * FROM t0 JOIN LATERAL (SELECT t1.c0 FROM t1 WHERE t1.id = t0.id ORDER BY t1.c0 LIMIT 1) AS dt ON (1 = 1)"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleMergedColumnLateralJoin(t *testing.T) {
	sql := "SELECT id AS merged_id, dt.id AS lateral_id FROM t0 JOIN t1 USING (id) JOIN LATERAL (SELECT t2.id AS id FROM t2 WHERE t2.id = id) AS dt ON (1 = 1) ORDER BY 1, 2"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for merged-column LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized merged-column LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleGroupedOutputAliasLateralJoin(t *testing.T) {
	sql := "SELECT id AS merged_id, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt FROM t0 JOIN t1 USING (id) JOIN LATERAL (SELECT agg.g0 AS g0, agg.cnt AS cnt FROM (SELECT t2.id AS g0, COUNT(1) AS cnt FROM t2 GROUP BY t2.id) AS agg WHERE (agg.g0 = id) ORDER BY agg.g0) AS dt ON (1 = 1) ORDER BY 1, 2, 3"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for grouped-output-alias LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized grouped-output-alias LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleGroupedOutputOrderLimitLateralJoin(t *testing.T) {
	sql := "SELECT id AS merged_id, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt FROM t0 JOIN t1 USING (id) JOIN LATERAL (SELECT t2.id AS g0, COUNT(1) AS cnt FROM t2 WHERE (t2.id <> id) GROUP BY t2.id HAVING (ABS(CASE WHEN (t2.id >= id) THEN (COUNT(1) - id) ELSE ((COUNT(1) + t2.id) - id) END) >= 1) ORDER BY g0, cnt DESC, id LIMIT 1) AS dt ON (1 = 1) ORDER BY 1, 2, 3"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for grouped-output-order-limit LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized grouped-output-order-limit LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleProjectedOrderLimitLateralJoin(t *testing.T) {
	sql := "SELECT id AS merged_id, dt.score0 AS lateral_score0, dt.tie0 AS lateral_tie0 FROM t0 JOIN t1 USING (id) JOIN LATERAL (SELECT ABS(CASE WHEN (t2.id >= id) THEN CASE WHEN (t2.id >= 0) THEN (t2.id - id) ELSE id END ELSE CASE WHEN (t2.id >= 0) THEN (id - t2.id) ELSE t2.id END END) AS score0, ABS(CASE WHEN (t2.id >= 0) THEN (t2.id + id) ELSE (id - t2.id) END) AS tie0 FROM t2 WHERE (t2.id <> id) ORDER BY score0, tie0 DESC, id LIMIT 1) AS dt ON (1 = 1) ORDER BY 1, 2, 3"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for projected-order-limit LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized projected-order-limit LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleScalarSubqueryProjectedOrderLimitLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.score0 AS lateral_score0, dt.tie0 AS lateral_tie0 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT ABS((t2.c2 - (SELECT sq.v0 AS sv0 FROM t2 AS sq WHERE ((sq.id = t0.id) AND (sq.c2 <> t1.c1)) ORDER BY ABS((sq.c2 - t1.c1)), sq.v0 DESC, t0.c0 LIMIT 1))) AS score0, (SELECT sq.v0 AS sv0 FROM t2 AS sq WHERE ((sq.id = t0.id) AND (sq.c2 <> t1.c1)) ORDER BY ABS((sq.c2 - t1.c1)), sq.v0 DESC, t0.c0 LIMIT 1) AS tie0 FROM t2 WHERE (t2.id = t0.id) ORDER BY score0, tie0 DESC, t0.c0 LIMIT 1) AS dt ON (1 = 1) ORDER BY 1, 2, 3, 4"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for scalar-subquery projected-order-limit LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized scalar-subquery projected-order-limit LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleMultiOuterProjectedOrderLimitLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.score0 AS lateral_score0, dt.tie0 AS lateral_tie0 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT ABS(CASE WHEN (t2.c2 >= t1.c1) THEN CASE WHEN (t2.v0 >= t0.c0) THEN (t2.c2 - t1.c1) ELSE t0.c0 END ELSE CASE WHEN (t2.v0 >= t0.c0) THEN (t1.c1 - t2.c2) ELSE t2.v0 END END) AS score0, ABS(CASE WHEN (t2.v0 >= t0.c0) THEN (t2.v0 + t0.c0) ELSE (t1.c1 - t2.v0) END) AS tie0 FROM t2 WHERE ((t2.id = t0.id) AND (t2.c2 <> t1.c1)) ORDER BY score0, tie0 DESC, t1.c1 LIMIT 1) AS dt ON (1 = 1) ORDER BY 1, 2, 3, 4"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for multi-outer projected-order-limit LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized multi-outer projected-order-limit LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleAggregateLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE ((t2.id = t0.id) AND (t2.c2 = t1.c1))) AS dt ON (1 = 1) ORDER BY 1, 2"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for aggregate LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized aggregate LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleGroupedAggregateLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT t2.c2 AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY t2.c2 HAVING (t2.c2 = t1.c1)) AS dt ON (1 = 1) ORDER BY 1, 2"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for grouped aggregate LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized grouped aggregate LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleOuterFilteredGroupedAggregateLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT t2.c2 AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE ((t2.id = t0.id) AND (t2.v0 >= t1.c1)) GROUP BY t2.c2) AS dt ON (1 = 1) ORDER BY 1, 2"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for outer-filtered grouped aggregate LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized outer-filtered grouped aggregate LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleMultiFilteredGroupedAggregateLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT t2.c2 AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (((t2.id = t0.id) AND (t2.c2 = t1.c1)) AND (t2.v0 >= t1.c1)) GROUP BY t2.c2) AS dt ON (1 = 1) ORDER BY 1, 2"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for multi-filtered grouped aggregate LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized multi-filtered grouped aggregate LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleOuterCorrelatedGroupKeyLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT (t2.c2 + t1.c1) AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY (t2.c2 + t1.c1)) AS dt ON (1 = 1) ORDER BY 1, 2"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for outer-correlated group-key LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized outer-correlated group-key LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleCaseCorrelatedGroupKeyLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT CASE WHEN (t2.c2 >= t1.c1) THEN t2.c2 ELSE t1.c1 END AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY CASE WHEN (t2.c2 >= t1.c1) THEN t2.c2 ELSE t1.c1 END) AS dt ON (1 = 1) ORDER BY 1, 2"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for case-correlated group-key LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized case-correlated group-key LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleNestedCaseCorrelatedGroupKeyLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT CASE WHEN (t2.c2 >= t1.c1) THEN CASE WHEN (t2.v0 >= t1.c1) THEN t2.c2 ELSE t1.c1 END ELSE CASE WHEN (t2.v0 >= t1.c1) THEN t1.c1 ELSE t2.c2 END END AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY CASE WHEN (t2.c2 >= t1.c1) THEN CASE WHEN (t2.v0 >= t1.c1) THEN t2.c2 ELSE t1.c1 END ELSE CASE WHEN (t2.v0 >= t1.c1) THEN t1.c1 ELSE t2.c2 END END) AS dt ON (1 = 1) ORDER BY 1, 2"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for nested-case-correlated group-key LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized nested-case-correlated group-key LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleWrappedNestedCaseCorrelatedGroupKeyLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT ABS(CASE WHEN (t2.c2 >= t1.c1) THEN CASE WHEN (t2.v0 >= t1.c1) THEN (t2.c2 - t1.c1) ELSE (t1.c1 - t2.c2) END ELSE CASE WHEN (t2.v0 >= t1.c1) THEN (t1.c1 - t2.c2) ELSE (t2.c2 - t1.c1) END END) AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY ABS(CASE WHEN (t2.c2 >= t1.c1) THEN CASE WHEN (t2.v0 >= t1.c1) THEN (t2.c2 - t1.c1) ELSE (t1.c1 - t2.c2) END ELSE CASE WHEN (t2.v0 >= t1.c1) THEN (t1.c1 - t2.c2) ELSE (t2.c2 - t1.c1) END END)) AS dt ON (1 = 1) ORDER BY 1, 2"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for wrapped-nested-case-correlated group-key LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized wrapped-nested-case-correlated group-key LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesHandleAggregateValuedHavingLateralJoin(t *testing.T) {
	sql := "SELECT t0.id AS t0_id, t1.c1 AS t1_c1, dt.g0 AS lateral_g0, dt.cnt AS lateral_cnt, dt.sum1 AS lateral_sum1 FROM t0 JOIN t1 ON (t0.id = t1.id) JOIN LATERAL (SELECT t2.c2 AS g0, COUNT(1) AS cnt, SUM(t2.v0) AS sum1 FROM t2 WHERE (t2.id = t0.id) GROUP BY t2.c2 HAVING (SUM(t2.v0) >= t1.c1)) AS dt ON (1 = 1) ORDER BY 1, 2"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	if len(candidates) == 0 {
		t.Fatalf("expected at least one minimized candidate for aggregate-valued HAVING LATERAL join")
	}
	for _, cand := range candidates {
		if _, err := p.ParseOneStmt(cand, "", ""); err != nil {
			t.Fatalf("expected minimized aggregate-valued HAVING LATERAL candidate to remain parseable: %v\nsql=%s", err, cand)
		}
	}
}

func TestSelectCandidatesReduceNestedBoolPredicateBranch(t *testing.T) {
	sql := "SELECT * FROM t WHERE (a > b OR c > d) AND e > f"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	found := false
	for _, cand := range candidates {
		candNode, err := p.ParseOneStmt(cand, "", "")
		if err != nil {
			continue
		}
		candSel, ok := candNode.(*ast.SelectStmt)
		if !ok || candSel.Where == nil {
			continue
		}
		if isColumnExpression(candSel.Where, "e", "f") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected candidate that keeps nested right branch predicate e > f")
	}
}

func TestSetOprCandidatesDropSingleCTEFromWithList(t *testing.T) {
	sql := "WITH cte_0 AS (SELECT 1 AS c0), cte_1 AS (SELECT 2 AS c0) SELECT c0 FROM cte_0 UNION SELECT c0 FROM cte_0"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	setOpr, ok := node.(*ast.SetOprStmt)
	if !ok {
		t.Fatalf("expected set operation stmt")
	}
	candidates := setOprCandidates(p, setOpr)
	found := false
	for _, cand := range candidates {
		candNode, err := p.ParseOneStmt(cand, "", "")
		if err != nil {
			continue
		}
		candSet, ok := candNode.(*ast.SetOprStmt)
		if !ok || candSet.With == nil || len(candSet.With.CTEs) != 1 {
			continue
		}
		found = true
		break
	}
	if !found {
		t.Fatalf("expected candidate that drops one CTE from WITH list in set operation")
	}
}

func TestSelectCandidatesReduceNestedBoolHavingBranch(t *testing.T) {
	sql := "SELECT a, SUM(b) AS s FROM t GROUP BY a HAVING (SUM(b) > 1 OR SUM(b) < -1) AND a > 0"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	found := false
	for _, cand := range candidates {
		candNode, err := p.ParseOneStmt(cand, "", "")
		if err != nil {
			continue
		}
		candSel, ok := candNode.(*ast.SelectStmt)
		if !ok || candSel.Having == nil {
			continue
		}
		if isColumnExpression(candSel.Having.Expr, "a") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected candidate that keeps nested right branch in HAVING")
	}
}

func TestSelectCandidatesReduceNestedBoolPredicateBranchUnderNot(t *testing.T) {
	sql := "SELECT * FROM t WHERE NOT ((a > b OR c > d) AND e > f)"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	found := false
	for _, cand := range candidates {
		candNode, err := p.ParseOneStmt(cand, "", "")
		if err != nil {
			continue
		}
		candSel, ok := candNode.(*ast.SelectStmt)
		if !ok || candSel.Where == nil {
			continue
		}
		if isColumnExpression(candSel.Where, "e", "f") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected candidate that can reduce NOT-wrapped nested boolean predicate branch")
	}
}

func isColumnExpression(expr ast.ExprNode, columns ...string) bool {
	if expr == nil {
		return false
	}
	expr = unwrapParenthesesExpr(expr)
	bin, ok := expr.(*ast.BinaryOperationExpr)
	if ok && (bin.Op == opcode.LogicAnd || bin.Op == opcode.LogicOr) {
		return false
	}
	expected := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		trimmed := strings.ToLower(strings.TrimSpace(col))
		if trimmed != "" {
			expected[trimmed] = struct{}{}
		}
	}
	if len(expected) == 0 {
		return false
	}
	collector := &columnNameCollector{columns: map[string]struct{}{}}
	expr.Accept(collector)
	for col := range expected {
		if _, ok := collector.columns[col]; !ok {
			return false
		}
	}
	return true
}

type columnNameCollector struct {
	columns map[string]struct{}
}

func (c *columnNameCollector) Enter(in ast.Node) (ast.Node, bool) {
	if c == nil || c.columns == nil {
		return in, false
	}
	col, ok := in.(*ast.ColumnNameExpr)
	if !ok || col.Name.Name.O == "" {
		return in, false
	}
	c.columns[strings.ToLower(col.Name.Name.O)] = struct{}{}
	return in, false
}

func (c *columnNameCollector) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
