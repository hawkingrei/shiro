package oracle

import (
	"testing"

	"shiro/internal/generator"
	"shiro/internal/schema"
)

func TestQueryColumnsValidChecksSetOperationOperands(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}, Alias: "c0"},
		},
		From: generator.FromClause{BaseTable: "t0"},
		SetOps: []generator.SetOperation{
			{
				Type: generator.SetOperationIntersect,
				Query: &generator.SelectQuery{
					Items: []generator.SelectItem{
						{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "missing"}}, Alias: "c0"},
					},
					From: generator.FromClause{BaseTable: "t0"},
				},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); ok || reason != "unknown_column" {
		t.Fatalf("queryColumnsValid() = (%v, %q), want false/unknown_column", ok, reason)
	}
}

func TestSanitizeQueryColumnsRepairsSetOperationOperands(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}, Alias: "c0"},
		},
		From: generator.FromClause{BaseTable: "t0"},
		SetOps: []generator.SetOperation{
			{
				Type: generator.SetOperationUnion,
				Query: &generator.SelectQuery{
					Items: []generator.SelectItem{
						{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "missing"}}, Alias: "c0"},
					},
					From: generator.FromClause{BaseTable: "t0"},
				},
			},
		},
	}

	if !sanitizeQueryColumns(query, state) {
		t.Fatalf("expected sanitizeQueryColumns to change set-operation operand")
	}
	if ok, reason := queryColumnsValid(query, state, nil); !ok {
		t.Fatalf("queryColumnsValid() after sanitize = (%v, %q), want true", ok, reason)
	}

	got, ok := query.SetOps[0].Query.Items[0].Expr.(generator.ColumnExpr)
	if !ok {
		t.Fatalf("expected sanitized operand expression to remain a column, got %T", query.SetOps[0].Query.Items[0].Expr)
	}
	if got.Ref.Table != "t0" || got.Ref.Name != "id" {
		t.Fatalf("unexpected sanitized operand column: %#v", got.Ref)
	}
}

func TestQueryColumnsValidAllowsUnqualifiedNaturalJoinColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
					{Name: "c1", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Natural: true},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); !ok {
		t.Fatalf("queryColumnsValid() = (%v, %q), want true", ok, reason)
	}
}

func TestSanitizeQueryColumnsKeepsUnqualifiedNaturalJoinColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
					{Name: "c1", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Natural: true},
			},
		},
	}

	if sanitizeQueryColumns(query, state) {
		t.Fatalf("expected sanitizeQueryColumns to keep valid unqualified NATURAL JOIN column")
	}

	col, ok := query.Items[0].Expr.(generator.ColumnExpr)
	if !ok {
		t.Fatalf("expected column expression, got %T", query.Items[0].Expr)
	}
	if col.Ref.Table != "" || col.Ref.Name != "k0" {
		t.Fatalf("unexpected sanitized column ref: %#v", col.Ref)
	}
}

func TestQueryColumnsValidRejectsUnqualifiedColumnAfterUsingRewrite(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
					{Name: "c1", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Using: []string{"k0"}},
			},
		},
	}

	rewriteUsingToOn(query, state)

	if ok, reason := queryColumnsValid(query, state, nil); ok || reason != "ambiguous_column" {
		t.Fatalf("queryColumnsValid() after USING rewrite = (%v, %q), want false/ambiguous_column", ok, reason)
	}
}

func TestSanitizeQueryColumnsRepairsUnqualifiedColumnAfterUsingRewrite(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
					{Name: "c1", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Using: []string{"k0"}},
			},
		},
	}

	rewriteUsingToOn(query, state)

	if !sanitizeQueryColumns(query, state) {
		t.Fatalf("expected sanitizeQueryColumns to repair unqualified column after USING rewrite")
	}

	col, ok := query.Items[0].Expr.(generator.ColumnExpr)
	if !ok {
		t.Fatalf("expected column expression, got %T", query.Items[0].Expr)
	}
	if col.Ref.Table != "t0" || col.Ref.Name != "k0" {
		t.Fatalf("unexpected sanitized column ref after USING rewrite: %#v", col.Ref)
	}
}

func TestQueryColumnsValidRejectsHiddenDerivedProjectionColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "dt", Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			BaseAlias: "dt",
			BaseQuery: &generator.SelectQuery{
				Items: []generator.SelectItem{
					{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}, Alias: "c0"},
				},
				From: generator.FromClause{BaseTable: "t0"},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); ok || reason != "unknown_column" {
		t.Fatalf("queryColumnsValid() = (%v, %q), want false/unknown_column", ok, reason)
	}
}

func TestQueryColumnsValidAllowsProjectedDerivedAliasColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "dt", Name: "c0"}}, Alias: "out0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			BaseAlias: "dt",
			BaseQuery: &generator.SelectQuery{
				Items: []generator.SelectItem{
					{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}, Alias: "c0"},
				},
				From: generator.FromClause{BaseTable: "t0"},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); !ok {
		t.Fatalf("queryColumnsValid() = (%v, %q), want true", ok, reason)
	}
}

func TestQueryColumnsValidRejectsQualifiedDerivedBaseColumnWhenAliased(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "c0"}}, Alias: "out0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			BaseAlias: "dt",
			BaseQuery: &generator.SelectQuery{
				Items: []generator.SelectItem{
					{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t0", Name: "id"}}, Alias: "c0"},
				},
				From: generator.FromClause{BaseTable: "t0"},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); ok || reason != "unknown_table" {
		t.Fatalf("queryColumnsValid() = (%v, %q), want false/unknown_table", ok, reason)
	}
}

func TestQueryColumnsValidRejectsQualifiedJoinBaseColumnWhenAliased(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "id"}}, Alias: "out0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:       generator.JoinInner,
					Table:      "t1",
					TableAlias: "j",
					On: generator.BinaryExpr{
						Left:  generator.LiteralExpr{Value: 1},
						Op:    "=",
						Right: generator.LiteralExpr{Value: 1},
					},
				},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); ok || reason != "unknown_table" {
		t.Fatalf("queryColumnsValid() = (%v, %q), want false/unknown_table", ok, reason)
	}
}

func TestQueryColumnsValidPrefersNearestOuterScopeForCorrelatedColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "v0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"},
		},
		From: generator.FromClause{BaseTable: "t0"},
		Where: generator.ExistsExpr{
			Query: &generator.SelectQuery{
				Items: []generator.SelectItem{
					{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"},
				},
				From: generator.FromClause{BaseTable: "t1"},
				Where: generator.ExistsExpr{
					Query: &generator.SelectQuery{
						Items: []generator.SelectItem{
							{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"},
						},
						From: generator.FromClause{BaseTable: "t2"},
						Where: generator.BinaryExpr{
							Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Name: "id"}},
							Op:    "=",
							Right: generator.LiteralExpr{Value: 1},
						},
					},
				},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); !ok {
		t.Fatalf("queryColumnsValid() = (%v, %q), want true", ok, reason)
	}
}

func TestQueryColumnsValidRejectsAmbiguousCorrelatedOuterColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "v0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t1",
					On: generator.BinaryExpr{
						Left:  generator.LiteralExpr{Value: 1},
						Op:    "=",
						Right: generator.LiteralExpr{Value: 1},
					},
				},
			},
		},
		Where: generator.ExistsExpr{
			Query: &generator.SelectQuery{
				Items: []generator.SelectItem{
					{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"},
				},
				From: generator.FromClause{BaseTable: "t2"},
				Where: generator.BinaryExpr{
					Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Name: "id"}},
					Op:    "=",
					Right: generator.LiteralExpr{Value: 1},
				},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); ok || reason != "ambiguous_column" {
		t.Fatalf("queryColumnsValid() = (%v, %q), want false/ambiguous_column", ok, reason)
	}
}

func TestQueryColumnsValidRejectsAmbiguousUsingColumn(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t1",
					On: generator.BinaryExpr{
						Left:  generator.LiteralExpr{Value: 1},
						Op:    "=",
						Right: generator.LiteralExpr{Value: 1},
					},
				},
				{
					Type:  generator.JoinInner,
					Table: "t2",
					Using: []string{"k0"},
				},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); ok || reason != "ambiguous_using_column" {
		t.Fatalf("queryColumnsValid() = (%v, %q), want false/ambiguous_using_column", ok, reason)
	}
}

func TestQueryColumnsValidRejectsQualifiedMergedColumnInLaterJoin(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
					{Name: "id", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
					{Name: "c1", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
					{Name: "c2", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{Type: generator.JoinInner, Table: "t1", Natural: true},
				{
					Type:  generator.JoinInner,
					Table: "t2",
					On: generator.BinaryExpr{
						Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t1", Name: "k0"}},
						Op:    "=",
						Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: "t2", Name: "k0"}},
					},
				},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); ok || reason != "unknown_column" {
		t.Fatalf("queryColumnsValid() = (%v, %q), want false/unknown_column", ok, reason)
	}
}

func TestQueryColumnsValidKeepsNaturalJoinColumnAmbiguousWhenLeftSideAlreadyAmbiguous(t *testing.T) {
	state := &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeBigInt},
				},
			},
		},
	}

	query := &generator.SelectQuery{
		Items: []generator.SelectItem{
			{Expr: generator.ColumnExpr{Ref: generator.ColumnRef{Name: "k0"}}, Alias: "c0"},
		},
		From: generator.FromClause{
			BaseTable: "t0",
			Joins: []generator.Join{
				{
					Type:  generator.JoinInner,
					Table: "t1",
					On: generator.BinaryExpr{
						Left:  generator.LiteralExpr{Value: 1},
						Op:    "=",
						Right: generator.LiteralExpr{Value: 1},
					},
				},
				{Type: generator.JoinInner, Table: "t2", Natural: true},
			},
		},
	}

	if ok, reason := queryColumnsValid(query, state, nil); ok || reason != "ambiguous_column" {
		t.Fatalf("queryColumnsValid() = (%v, %q), want false/ambiguous_column", ok, reason)
	}
}
