package oracle

import (
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/oracle/groundtruth"
	"shiro/internal/schema"
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
			RightKeys:  []string{"k0"},
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
	if reason := groundTruthDSGMismatchReason("t0", []groundtruth.JoinEdge{{LeftTable: "x1", RightTable: "t1", LeftKeys: []string{"k0"}, RightKeys: []string{"k0"}}}); reason != "left_table" {
		t.Fatalf("expected left_table, got %q", reason)
	}
	if reason := groundTruthDSGMismatchReason("t0", []groundtruth.JoinEdge{{LeftTable: "t0", RightTable: "t1", LeftKeys: nil, RightKeys: []string{"k0"}}}); reason != "left_keys_missing" {
		t.Fatalf("expected left_keys_missing, got %q", reason)
	}
}

func TestGroundTruthDSGPrecheck(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k0", Type: schema.TypeInt},
				},
			},
		},
	}

	valid := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Using: []string{"k0"}},
			},
		},
	}
	if skip, reason := groundTruthDSGPrecheck(valid, state); skip != "" || reason != "" {
		t.Fatalf("expected valid precheck, got skip=%q reason=%q", skip, reason)
	}

	invalidUsing := &generator.SelectQuery{
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Using: []string{"id"}},
			},
		},
	}
	if skip, reason := groundTruthDSGPrecheck(invalidUsing, state); skip != "groundtruth:dsg_key_mismatch_right_key" || reason != "right_key" {
		t.Fatalf("expected right_key mismatch, got skip=%q reason=%q", skip, reason)
	}
}

func TestGroundTruthDSGRightKeysAvailable(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
		},
	}
	edges := []groundtruth.JoinEdge{
		{
			LeftTable:  "t0",
			RightTable: "t1",
			LeftKeys:   []string{"k0"},
			RightKeys:  []string{"k0"},
		},
	}
	if !groundTruthDSGRightKeysAvailable(state, edges) {
		t.Fatalf("expected right keys to be available")
	}
	edges[0].RightKeys = []string{"k9"}
	if groundTruthDSGRightKeysAvailable(state, edges) {
		t.Fatalf("expected missing right keys to be detected")
	}
}

func TestDSGTableKeyName(t *testing.T) {
	key, ok := dsgTableKeyName("t1")
	if !ok || key != "k0" {
		t.Fatalf("t1 key = (%q,%v), want (k0,true)", key, ok)
	}
	key, ok = dsgTableKeyName("t2")
	if !ok || key != "k1" {
		t.Fatalf("t2 key = (%q,%v), want (k1,true)", key, ok)
	}
	if _, ok := dsgTableKeyName("x2"); ok {
		t.Fatalf("expected non-t* table to be invalid")
	}
}

func TestGroundTruthJoinEdgesPrefersSQLASTForAliases(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
		},
	}
	query := &generator.SelectQuery{
		Items: []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}},
		From: generator.FromClause{
			BaseTable: "t0",
			BaseAlias: "a",
			Joins: []generator.Join{
				{
					Type:       generator.JoinInner,
					Table:      "t1",
					TableAlias: "b",
					On: generator.BinaryExpr{
						Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "a", Name: "k0"}},
						Op:    "=",
						Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "b", Name: "k0"}},
					},
				},
			},
		},
	}
	edges := groundTruthJoinEdges(query, state)
	if len(edges) != 1 {
		t.Fatalf("expected one edge, got %d", len(edges))
	}
	if edges[0].LeftTable != "t0" || edges[0].RightTable != "t1" {
		t.Fatalf("unexpected edge tables: left=%q right=%q", edges[0].LeftTable, edges[0].RightTable)
	}
	if len(edges[0].LeftKeyList()) != 1 || edges[0].LeftKeyList()[0] != "k0" {
		t.Fatalf("unexpected left keys: %v", edges[0].LeftKeyList())
	}
	if len(edges[0].RightKeyList()) != 1 || edges[0].RightKeyList()[0] != "k0" {
		t.Fatalf("unexpected right keys: %v", edges[0].RightKeyList())
	}
}

func TestGroundTruthEffectiveMaxRows(t *testing.T) {
	if got := groundTruthEffectiveMaxRows(nil); got != groundTruthDefaultMaxRows {
		t.Fatalf("nil generator maxRows=%d want=%d", got, groundTruthDefaultMaxRows)
	}
	gen := &generator.Generator{
		Config: config.Config{
			MaxRowsPerTable: 60,
			Oracles: config.OracleConfig{
				GroundTruthMaxRows: 40,
			},
		},
	}
	if got := groundTruthEffectiveMaxRows(gen); got != 60 {
		t.Fatalf("base maxRows=%d want=60", got)
	}
	gen.Config.Features.DSG = true
	gen.Config.TQS.Enabled = true
	gen.Config.TQS.WideRows = 80
	if got := groundTruthEffectiveMaxRows(gen); got != 80 {
		t.Fatalf("dsg+tqs maxRows=%d want=80", got)
	}
	gen.Config.Features.DSG = false
	if got := groundTruthEffectiveMaxRows(gen); got != 60 {
		t.Fatalf("non-dsg maxRows=%d want=60", got)
	}
}
