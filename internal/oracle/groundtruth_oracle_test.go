package oracle

import (
	"testing"

	"shiro/internal/generator"
	"shiro/internal/oracle/groundtruth"
)

func TestShouldSkipGroundTruth(t *testing.T) {
	base := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1"},
			},
		},
	}
	if shouldSkipGroundTruth(base) {
		t.Fatalf("expected base query to be eligible for groundtruth")
	}
	withWhere := *base
	withWhere.Where = generator.LiteralExpr{Value: 1}
	if !shouldSkipGroundTruth(&withWhere) {
		t.Fatalf("expected WHERE query to be skipped")
	}
	noJoin := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}},
		},
		From: generator.FromClause{BaseTable: "t0"},
	}
	if !shouldSkipGroundTruth(noJoin) {
		t.Fatalf("expected no-join query to be skipped")
	}
}

func TestJoinSignature(t *testing.T) {
	query := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1"},
				{Type: generator.JoinLeft, Table: "t2"},
			},
		},
	}
	sig := joinSignature(query)
	if sig != "t0->JOIN:t1->LEFT JOIN:t2" {
		t.Fatalf("unexpected join signature: %s", sig)
	}
}

func TestJoinRowsNullAndLimit(t *testing.T) {
	left := []rowData{
		{
			"t0.id": {Val: "1"},
		},
		{
			"t0.id": {Null: true},
		},
	}
	right := []rowData{
		{
			"t1.id": {Val: "1"},
		},
		{
			"t1.id": {Val: "1"},
		},
		{
			"t1.id": {Null: true},
		},
	}
	rows, ok := joinRows(left, right, "t0", []string{"id"}, "t1", []string{"id"}, 0)
	if !ok {
		t.Fatalf("expected join to succeed without limit")
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 joined rows, got %d", len(rows))
	}
	if _, ok := rows[0]["t0.id"]; !ok {
		t.Fatalf("expected merged row to include left table columns")
	}
	if _, ok := rows[0]["t1.id"]; !ok {
		t.Fatalf("expected merged row to include right table columns")
	}
	_, ok = joinRows(left, right, "t0", []string{"id"}, "t1", []string{"id"}, 1)
	if ok {
		t.Fatalf("expected join to exceed maxRows limit")
	}
}

func TestGroundTruthDSGSkipReason(t *testing.T) {
	edges := []groundtruth.JoinEdge{
		{
			LeftTable:  "t0",
			RightTable: "t1",
			LeftKeys:   []string{"k0"},
			RightKeys:  []string{"k1"},
		},
	}
	if skip, reason := groundTruthDSGSkipReason("t0", edges); skip != "" || reason != "" {
		t.Fatalf("expected valid DSG join, got skip=%q reason=%q", skip, reason)
	}

	invalidKey := []groundtruth.JoinEdge{
		{
			LeftTable:  "t0",
			RightTable: "t1",
			LeftKeys:   []string{"k0"},
			RightKeys:  []string{"k9"},
		},
	}
	if skip, reason := groundTruthDSGSkipReason("t0", invalidKey); skip != "groundtruth:dsg_key_mismatch_right_key" || reason != "right_key" {
		t.Fatalf("unexpected dsg mismatch result: skip=%q reason=%q", skip, reason)
	}

	if reason := groundTruthDSGMismatchReason("x0", invalidKey); reason != "base_table" {
		t.Fatalf("expected base_table, got %q", reason)
	}
	if reason := groundTruthDSGMismatchReason("t0", []groundtruth.JoinEdge{{LeftTable: "x1", RightTable: "t1", LeftKeys: []string{"k0"}, RightKeys: []string{"k1"}}}); reason != "left_table" {
		t.Fatalf("expected left_table, got %q", reason)
	}
	if reason := groundTruthDSGMismatchReason("t0", []groundtruth.JoinEdge{{LeftTable: "t0", RightTable: "t1", LeftKeys: nil, RightKeys: []string{"k1"}}}); reason != "left_keys_missing" {
		t.Fatalf("expected left_keys_missing, got %q", reason)
	}
}
