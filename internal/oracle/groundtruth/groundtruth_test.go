package groundtruth

import "testing"

func TestEvalJoinChainExactMultiplicity(t *testing.T) {
	truth := NewSchemaTruth(0)
	truth.AddTable("t1")
	truth.AddTable("t2")
	for i := 0; i < 2; i++ {
		truth.AddRowData("t1", map[string]TypedValue{"k0": {Type: "string", Value: "A"}})
	}
	for i := 0; i < 3; i++ {
		truth.AddRowData("t2", map[string]TypedValue{"k0": {Type: "string", Value: "A"}})
	}
	exec := JoinTruthExecutor{Truth: truth}
	edges := []JoinEdge{{
		LeftTable:  "t1",
		RightTable: "t2",
		LeftKey:    "k0",
		RightKey:   "k0",
		JoinType:   JoinInner,
	}}
	count, ok, reason := exec.EvalJoinChainExact("t1", edges, 100, 100)
	if !ok {
		t.Fatalf("expected ok, got reason=%s", reason)
	}
	if count != 6 {
		t.Fatalf("expected count=6, got %d", count)
	}
	_, ok, reason = exec.EvalJoinChainExact("t1", edges, 100, 5)
	if ok || reason != "join_rows_exceeded" {
		t.Fatalf("expected join_rows_exceeded, got ok=%v reason=%s", ok, reason)
	}
}

func TestEvalJoinChainExactLeftRightSemiAnti(t *testing.T) {
	truth := NewSchemaTruth(0)
	truth.AddTable("t1")
	truth.AddTable("t2")
	truth.AddRowData("t1", map[string]TypedValue{"k0": {Type: "string", Value: "A"}})
	truth.AddRowData("t1", map[string]TypedValue{"k0": {Type: "string", Value: "B"}})
	truth.AddRowData("t2", map[string]TypedValue{"k0": {Type: "string", Value: "A"}})
	exec := JoinTruthExecutor{Truth: truth}
	tableCap := 10
	joinCap := 100
	edges := []JoinEdge{{
		LeftTable:  "t1",
		RightTable: "t2",
		LeftKey:    "k0",
		RightKey:   "k0",
	}}
	edges[0].JoinType = JoinLeft
	if count, ok, reason := exec.EvalJoinChainExact("t1", edges, tableCap, joinCap); !ok || count != 2 {
		t.Fatalf("left join expected 2, got ok=%v count=%d reason=%s", ok, count, reason)
	}
	edges[0].JoinType = JoinRight
	if count, ok, reason := exec.EvalJoinChainExact("t1", edges, tableCap, joinCap); !ok || count != 1 {
		t.Fatalf("right join expected 1, got ok=%v count=%d reason=%s", ok, count, reason)
	}
	edges[0].JoinType = JoinSemi
	if count, ok, reason := exec.EvalJoinChainExact("t1", edges, tableCap, joinCap); !ok || count != 1 {
		t.Fatalf("semi join expected 1, got ok=%v count=%d reason=%s", ok, count, reason)
	}
	edges[0].JoinType = JoinAnti
	if count, ok, reason := exec.EvalJoinChainExact("t1", edges, tableCap, joinCap); !ok || count != 1 {
		t.Fatalf("anti join expected 1, got ok=%v count=%d reason=%s", ok, count, reason)
	}
}
