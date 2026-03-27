package generator

import (
	"testing"

	"shiro/internal/schema"
)

func TestValidateQueryScopeJoinOnUsesFutureTable(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
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
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
		}},
	}

	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k0", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}},
					},
				},
				{
					Type:  JoinInner,
					Table: "t2",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k0", Type: schema.TypeInt}},
					},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}}},
		},
	}

	if gen.validateQueryScope(query) {
		t.Fatalf("expected join scope validation to fail when ON uses future table")
	}
}

func TestValidateQueryScopeJoinOnUsesVisibleTables(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
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
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
		}},
	}

	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}},
					},
				},
				{
					Type:  JoinInner,
					Table: "t2",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k0", Type: schema.TypeInt}},
					},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}}},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected join scope validation to pass for visible tables")
	}
}

func TestValidateQueryScopeSetOpTableNotVisibleToMainQuery(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "c0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "c0", Type: schema.TypeInt},
				},
			},
		}},
	}

	query := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}}},
		},
		From: FromClause{BaseTable: "t0"},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c0", Type: schema.TypeInt}},
			Op:    "=",
			Right: LiteralExpr{Value: 1},
		},
		SetOps: []SetOperation{
			{
				Type: SetOperationUnion,
				Query: &SelectQuery{
					Items: []SelectItem{
						{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c0", Type: schema.TypeInt}}},
					},
					From: FromClause{BaseTable: "t1"},
				},
			},
		},
	}

	if gen.validateQueryScope(query) {
		t.Fatalf("expected set-op operand tables to be invisible to main query scope")
	}
}

func TestValidateQueryScopeUsingHidesQualifiedJoinedColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
		}},
	}

	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"k0"},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}}},
		},
	}

	if gen.validateQueryScope(query) {
		t.Fatalf("expected USING column to be hidden for qualified references")
	}
}

func TestValidateQueryScopeUsingKeepsNonUsingQualifiedColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
		}},
	}

	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"k0"},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c1", Type: schema.TypeInt}}},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected non-USING qualified columns to remain visible")
	}
}

func TestValidateQueryScopeLateralJoinAllowsLeftReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
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
		}},
	}

	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}}, Alias: "k0"},
		},
		From: FromClause{BaseTable: "t1"},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}},
			Op:    "=",
			Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}},
		},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{{
				Type:       JoinCross,
				Lateral:    true,
				Table:      "t1",
				TableAlias: "t1",
				TableQuery: lateral,
			}},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}}},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected LATERAL derived table to see left-side tables")
	}
}

func TestValidateQueryScopeLateralJoinAllowsMultiTableAggregateReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
			{Expr: FuncExpr{Name: "SUM", Args: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}}}}, Alias: "sum1"},
		},
		From: FromClause{BaseTable: "t2"},
		Where: BinaryExpr{
			Left: BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
				Op:    "=",
				Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
			},
			Op: "AND",
			Right: BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
				Op:    "=",
				Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k1", Type: schema.TypeInt}},
			},
		},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}}, Alias: "id"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "cnt", Type: schema.TypeBigInt}}, Alias: "cnt"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected LATERAL aggregate query to see both left-side tables")
	}
}

func TestValidateQueryScopeLateralJoinAllowsMultiOuterProjectedOrderLimitReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c2", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	limit := 1
	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: FuncExpr{
				Name: "ABS",
				Args: []Expr{CaseExpr{
					Whens: []CaseWhen{
						{
							When: BinaryExpr{
								Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "c2", Type: schema.TypeInt}},
								Op:    ">=",
								Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
							},
							Then: CaseExpr{
								Whens: []CaseWhen{
									{
										When: BinaryExpr{
											Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
											Op:    ">=",
											Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}},
										},
										Then: BinaryExpr{
											Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "c2", Type: schema.TypeInt}},
											Op:    "-",
											Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
										},
									},
								},
								Else: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}},
							},
						},
					},
					Else: CaseExpr{
						Whens: []CaseWhen{
							{
								When: BinaryExpr{
									Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
									Op:    ">=",
									Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}},
								},
								Then: BinaryExpr{
									Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
									Op:    "-",
									Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "c2", Type: schema.TypeInt}},
								},
							},
						},
						Else: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
					},
				}},
			}, Alias: "score0"},
			{Expr: FuncExpr{
				Name: "ABS",
				Args: []Expr{CaseExpr{
					Whens: []CaseWhen{
						{
							When: BinaryExpr{
								Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
								Op:    ">=",
								Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}},
							},
							Then: BinaryExpr{
								Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
								Op:    "+",
								Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}},
							},
						},
					},
					Else: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
						Op:    "-",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
					},
				}},
			}, Alias: "tie0"},
		},
		From: FromClause{BaseTable: "t2"},
		Where: BinaryExpr{
			Left: BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
				Op:    "=",
				Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
			},
			Op: "AND",
			Right: BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "c2", Type: schema.TypeInt}},
				Op:    "<>",
				Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
			},
		},
		OrderBy: []OrderBy{
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "score0", Type: schema.TypeInt}}},
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "tie0", Type: schema.TypeInt}}, Desc: true},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}}},
		},
		Limit: &limit,
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}}, Alias: "id"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "score0", Type: schema.TypeInt}}, Alias: "score0"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected non-grouped LATERAL ORDER BY/LIMIT to keep multi-table left input visible in projection and order")
	}
}

func TestValidateQueryScopeLateralJoinAllowsScalarSubqueryProjectedOrderLimitReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c2", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	subLimit := 1
	buildScalar := func() *SelectQuery {
		return &SelectQuery{
			Items: []SelectItem{
				{Expr: ColumnExpr{Ref: ColumnRef{Table: "sq", Name: "v0", Type: schema.TypeInt}}, Alias: "sv0"},
			},
			From: FromClause{BaseTable: "t2", BaseAlias: "sq"},
			Where: BinaryExpr{
				Left: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "sq", Name: "id", Type: schema.TypeInt}},
					Op:    "=",
					Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
				},
				Op: "AND",
				Right: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "sq", Name: "c2", Type: schema.TypeInt}},
					Op:    "<>",
					Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
				},
			},
			OrderBy: []OrderBy{
				{
					Expr: FuncExpr{
						Name: "ABS",
						Args: []Expr{BinaryExpr{
							Left:  ColumnExpr{Ref: ColumnRef{Table: "sq", Name: "c2", Type: schema.TypeInt}},
							Op:    "-",
							Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
						}},
					},
				},
				{Expr: ColumnExpr{Ref: ColumnRef{Table: "sq", Name: "v0", Type: schema.TypeInt}}, Desc: true},
				{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}}},
			},
			Limit: &subLimit,
		}
	}

	limit := 1
	lateral := &SelectQuery{
		Items: []SelectItem{
			{
				Expr: FuncExpr{
					Name: "ABS",
					Args: []Expr{BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "c2", Type: schema.TypeInt}},
						Op:    "-",
						Right: SubqueryExpr{Query: buildScalar()},
					}},
				},
				Alias: "score0",
			},
			{
				Expr:  SubqueryExpr{Query: buildScalar()},
				Alias: "tie0",
			},
		},
		From: FromClause{BaseTable: "t2"},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
			Op:    "=",
			Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
		},
		OrderBy: []OrderBy{
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "score0", Type: schema.TypeInt}}},
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "tie0", Type: schema.TypeInt}}, Desc: true},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}}},
		},
		Limit: &limit,
	}
	siblingLimit := 1
	siblingDerived := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: ColumnRef{Table: "postr", Name: "v0", Type: schema.TypeInt}},
				Alias: "match0",
			},
			{
				Expr:  ColumnExpr{Ref: ColumnRef{Table: "postr", Name: "id", Type: schema.TypeInt}},
				Alias: "join0",
			},
			{
				Expr: FuncExpr{
					Name: "ABS",
					Args: []Expr{BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "postr", Name: "c2", Type: schema.TypeInt}},
						Op:    "-",
						Right: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "tie0", Type: schema.TypeInt}},
					}},
				},
				Alias: "probe0",
			},
		},
		From: FromClause{BaseTable: "t2", BaseAlias: "postr"},
		Where: BinaryExpr{
			Left: BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: "postr", Name: "id", Type: schema.TypeInt}},
				Op:    "=",
				Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
			},
			Op: "AND",
			Right: BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: "postr", Name: "c2", Type: schema.TypeInt}},
				Op:    ">=",
				Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
			},
		},
	}
	quantifiedLimit := 2
	quantifiedQuery := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: ColumnRef{Table: "probeq", Name: "probe0", Type: schema.TypeInt}},
				Alias: "mq0",
			},
		},
		From: FromClause{
			BaseTable: "t2",
			BaseAlias: "postq",
			Joins: []Join{
				{
					Type:       JoinInner,
					Table:      "probeq",
					TableAlias: "probeq",
					TableQuery: siblingDerived,
					On: BinaryExpr{
						Left: BinaryExpr{
							Left:  ColumnExpr{Ref: ColumnRef{Table: "postq", Name: "id", Type: schema.TypeInt}},
							Op:    "=",
							Right: ColumnExpr{Ref: ColumnRef{Table: "probeq", Name: "join0", Type: schema.TypeInt}},
						},
						Op: "AND",
						Right: BinaryExpr{
							Left:  ColumnExpr{Ref: ColumnRef{Table: "postq", Name: "v0", Type: schema.TypeInt}},
							Op:    "=",
							Right: ColumnExpr{Ref: ColumnRef{Table: "probeq", Name: "match0", Type: schema.TypeInt}},
						},
					},
				},
			},
		},
		OrderBy: []OrderBy{
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "mq0", Type: schema.TypeInt}}},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "probeq", Name: "match0", Type: schema.TypeInt}}},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "c0", Type: schema.TypeInt}}},
		},
		Limit: &quantifiedLimit,
	}
	siblingLateral := &SelectQuery{
		Items: []SelectItem{
			{
				Expr:  ColumnExpr{Ref: ColumnRef{Table: "postd", Name: "probe0", Type: schema.TypeInt}},
				Alias: "probe0",
			},
		},
		From: FromClause{
			BaseTable: "t2",
			BaseAlias: "post",
			Joins: []Join{
				{
					Type:       JoinInner,
					Table:      "postd",
					TableAlias: "postd",
					TableQuery: siblingDerived,
					On: BinaryExpr{
						Left: BinaryExpr{
							Left:  ColumnExpr{Ref: ColumnRef{Table: "post", Name: "id", Type: schema.TypeInt}},
							Op:    "=",
							Right: ColumnExpr{Ref: ColumnRef{Table: "postd", Name: "join0", Type: schema.TypeInt}},
						},
						Op: "AND",
						Right: BinaryExpr{
							Left:  ColumnExpr{Ref: ColumnRef{Table: "post", Name: "v0", Type: schema.TypeInt}},
							Op:    "=",
							Right: ColumnExpr{Ref: ColumnRef{Table: "postd", Name: "match0", Type: schema.TypeInt}},
						},
					},
				},
			},
		},
		Where: CompareSubqueryExpr{
			Left:       ColumnExpr{Ref: ColumnRef{Table: "postd", Name: "probe0", Type: schema.TypeInt}},
			Op:         ">=",
			Quantifier: "ANY",
			Query:      quantifiedQuery,
		},
		OrderBy: []OrderBy{
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "probe0", Type: schema.TypeInt}}},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "score0", Type: schema.TypeInt}}},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}}},
		},
		Limit: &siblingLimit,
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "sx",
					TableAlias: "sx",
					TableQuery: siblingLateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}}, Alias: "base0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}}, Alias: "sibling0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "score0", Type: schema.TypeInt}}, Alias: "lateral_score0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "tie0", Type: schema.TypeInt}}, Alias: "lateral_tie0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "sx", Name: "probe0", Type: schema.TypeInt}}, Alias: "sibling_probe0"},
		},
		OrderBy: []OrderBy{
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "sibling_probe0", Type: schema.TypeInt}}},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "score0", Type: schema.TypeInt}}},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}}},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected scalar-subquery projected-order-limit LATERAL query to keep outer references visible through the nested scalar subquery and the sibling derived-right quantified consumer")
	}
}

func TestValidateQueryScopeLateralJoinAllowsGroupedAggregateHavingReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
		},
		From:    FromClause{BaseTable: "t2"},
		Where:   BinaryExpr{Left: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}, Op: "=", Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}}},
		GroupBy: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}}},
		Having:  BinaryExpr{Left: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}}, Op: "=", Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k1", Type: schema.TypeInt}}},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "cnt", Type: schema.TypeBigInt}}, Alias: "cnt"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected grouped aggregate HAVING to see outer tables")
	}
}

func TestValidateQueryScopeLateralJoinAllowsOuterFilteredGroupedAggregateReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
			{Expr: FuncExpr{Name: "SUM", Args: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}}}}, Alias: "sum1"},
		},
		From: FromClause{BaseTable: "t2"},
		Where: BinaryExpr{
			Left: BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
				Op:    "=",
				Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
			},
			Op: "AND",
			Right: BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
				Op:    ">=",
				Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
			},
		},
		GroupBy: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}}},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "sum1", Type: schema.TypeInt}}, Alias: "sum1"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected outer-filtered grouped aggregate WHERE to see outer tables")
	}
}

func TestValidateQueryScopeLateralJoinAllowsMultiFilteredGroupedAggregateReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
			{Expr: FuncExpr{Name: "SUM", Args: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}}}}, Alias: "sum1"},
		},
		From: FromClause{BaseTable: "t2"},
		Where: BinaryExpr{
			Left: BinaryExpr{
				Left: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
					Op:    "=",
					Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
				},
				Op: "AND",
				Right: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
					Op:    "=",
					Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
				},
			},
			Op: "AND",
			Right: BinaryExpr{
				Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
				Op:    ">=",
				Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
			},
		},
		GroupBy: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}}},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "sum1", Type: schema.TypeInt}}, Alias: "sum1"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected multi-filtered grouped aggregate WHERE to see outer tables")
	}
}

func TestValidateQueryScopeLateralJoinAllowsOuterCorrelatedGroupKeyReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	groupExpr := BinaryExpr{
		Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
		Op:    "+",
		Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
	}
	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: groupExpr, Alias: "g0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
			{Expr: FuncExpr{Name: "SUM", Args: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}}}}, Alias: "sum1"},
		},
		From:    FromClause{BaseTable: "t2"},
		Where:   BinaryExpr{Left: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}, Op: "=", Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}}},
		GroupBy: []Expr{groupExpr},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "sum1", Type: schema.TypeInt}}, Alias: "sum1"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected outer-correlated grouped key to see outer tables")
	}
}

func TestValidateQueryScopeLateralJoinAllowsCaseCorrelatedGroupKeyReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	groupExpr := CaseExpr{
		Whens: []CaseWhen{
			{
				When: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
					Op:    ">=",
					Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
				},
				Then: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
			},
		},
		Else: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
	}
	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: groupExpr, Alias: "g0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
			{Expr: FuncExpr{Name: "SUM", Args: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}}}}, Alias: "sum1"},
		},
		From:    FromClause{BaseTable: "t2"},
		Where:   BinaryExpr{Left: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}, Op: "=", Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}}},
		GroupBy: []Expr{groupExpr},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "sum1", Type: schema.TypeInt}}, Alias: "sum1"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected case-correlated grouped key to see outer tables")
	}
}

func TestValidateQueryScopeLateralJoinAllowsNestedCaseCorrelatedGroupKeyReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	innerCase := CaseExpr{
		Whens: []CaseWhen{
			{
				When: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
					Op:    ">=",
					Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
				},
				Then: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
			},
		},
		Else: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
	}
	outerElse := CaseExpr{
		Whens: []CaseWhen{
			{
				When: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
					Op:    ">=",
					Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
				},
				Then: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
			},
		},
		Else: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
	}
	groupExpr := CaseExpr{
		Whens: []CaseWhen{
			{
				When: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
					Op:    ">=",
					Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
				},
				Then: innerCase,
			},
		},
		Else: outerElse,
	}
	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: groupExpr, Alias: "g0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
			{Expr: FuncExpr{Name: "SUM", Args: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}}}}, Alias: "sum1"},
		},
		From:    FromClause{BaseTable: "t2"},
		Where:   BinaryExpr{Left: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}, Op: "=", Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}}},
		GroupBy: []Expr{groupExpr},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "sum1", Type: schema.TypeInt}}, Alias: "sum1"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected nested-case-correlated grouped key to see outer tables")
	}
}

func TestValidateQueryScopeLateralJoinAllowsWrappedNestedCaseCorrelatedGroupKeyReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	diffInnerOuter := BinaryExpr{
		Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
		Op:    "-",
		Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
	}
	diffOuterInner := BinaryExpr{
		Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
		Op:    "-",
		Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
	}
	innerCase := CaseExpr{
		Whens: []CaseWhen{
			{
				When: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
					Op:    ">=",
					Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
				},
				Then: diffInnerOuter,
			},
		},
		Else: diffOuterInner,
	}
	outerElse := CaseExpr{
		Whens: []CaseWhen{
			{
				When: BinaryExpr{
					Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}},
					Op:    ">=",
					Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
				},
				Then: diffOuterInner,
			},
		},
		Else: diffInnerOuter,
	}
	groupExpr := FuncExpr{
		Name: "ABS",
		Args: []Expr{
			CaseExpr{
				Whens: []CaseWhen{
					{
						When: BinaryExpr{
							Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}},
							Op:    ">=",
							Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
						},
						Then: innerCase,
					},
				},
				Else: outerElse,
			},
		},
	}
	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: groupExpr, Alias: "g0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
			{Expr: FuncExpr{Name: "SUM", Args: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}}}}, Alias: "sum1"},
		},
		From:    FromClause{BaseTable: "t2"},
		Where:   BinaryExpr{Left: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}, Op: "=", Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}}},
		GroupBy: []Expr{groupExpr},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "sum1", Type: schema.TypeInt}}, Alias: "sum1"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected wrapped-nested-case-correlated grouped key to see outer tables")
	}
}

func TestValidateQueryScopeLateralJoinAllowsAggregateValuedHavingReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k1", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		}},
	}

	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
			{Expr: FuncExpr{Name: "SUM", Args: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}}}}, Alias: "sum1"},
		},
		From:    FromClause{BaseTable: "t2"},
		Where:   BinaryExpr{Left: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}, Op: "=", Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}}},
		GroupBy: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k1", Type: schema.TypeInt}}},
		Having: BinaryExpr{
			Left:  FuncExpr{Name: "SUM", Args: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "v0", Type: schema.TypeInt}}}},
			Op:    ">=",
			Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}},
		},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
					},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "sum1", Type: schema.TypeInt}}, Alias: "sum1"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected aggregate-valued HAVING to see outer tables")
	}
}

func TestValidateQueryScopeLateralJoinRejectsFutureTableReferences(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
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
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
				},
			},
		}},
	}

	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}}, Alias: "k0"},
		},
		From: FromClause{BaseTable: "t1"},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}},
			Op:    "=",
			Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k0", Type: schema.TypeInt}},
		},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "t1",
					TableAlias: "t1",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
				{
					Type:  JoinInner,
					Table: "t2",
					On: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}},
						Op:    "=",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "k0", Type: schema.TypeInt}},
					},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}}},
		},
	}

	if gen.validateQueryScope(query) {
		t.Fatalf("expected LATERAL derived table to reject future/right-side references")
	}
}

func TestValidateQueryScopeLateralJoinUsingAllowsUnqualifiedMergedColumn(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
		}},
	}

	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}, Alias: "id"},
		},
		From: FromClause{BaseTable: "t2"},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
			Op:    "=",
			Right: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
		},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"id"},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}}, Alias: "id"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected LATERAL derived table to allow unqualified merged USING column")
	}
}

func TestValidateQueryScopeLateralJoinAllowsGroupedOutputAliasVisibility(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
		}},
	}

	agg := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
		},
		From:    FromClause{BaseTable: "t2"},
		GroupBy: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}},
	}
	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "agg", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "agg", Name: "cnt", Type: schema.TypeBigInt}}, Alias: "cnt"},
		},
		From: FromClause{
			BaseTable: "agg",
			BaseAlias: "agg",
			BaseQuery: agg,
		},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "agg", Name: "g0", Type: schema.TypeInt}},
			Op:    "=",
			Right: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
		},
		OrderBy: []OrderBy{{
			Expr: ColumnExpr{Ref: ColumnRef{Table: "agg", Name: "g0", Type: schema.TypeInt}},
		}},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"id"},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}}, Alias: "id"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected LATERAL grouped derived output alias to remain visible with merged outer column")
	}
}

func TestValidateQueryScopeLateralJoinAllowsGroupedOutputOrderLimitVisibility(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
		}},
	}

	limit := 1
	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}, Alias: "g0"},
			{Expr: FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}}, Alias: "cnt"},
		},
		From: FromClause{BaseTable: "t2"},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
			Op:    "<>",
			Right: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
		},
		GroupBy: []Expr{ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}},
		Having: BinaryExpr{
			Left: FuncExpr{
				Name: "ABS",
				Args: []Expr{CaseExpr{
					Whens: []CaseWhen{
						{
							When: BinaryExpr{
								Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
								Op:    ">=",
								Right: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
							},
							Then: BinaryExpr{
								Left:  FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}},
								Op:    "-",
								Right: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
							},
						},
					},
					Else: BinaryExpr{
						Left: BinaryExpr{
							Left:  FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}},
							Op:    "+",
							Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
						},
						Op:    "-",
						Right: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
					},
				}},
			},
			Op:    ">=",
			Right: LiteralExpr{Value: 1},
		},
		OrderBy: []OrderBy{
			{
				Expr: ColumnExpr{Ref: ColumnRef{Name: "g0", Type: schema.TypeInt}},
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{Name: "cnt", Type: schema.TypeBigInt}},
				Desc: true,
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
			},
		},
		Limit: &limit,
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"id"},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}}, Alias: "id"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "g0", Type: schema.TypeInt}}, Alias: "g0"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected LATERAL grouped output ORDER BY/LIMIT to see merged outer column and grouped aliases")
	}
}

func TestValidateQueryScopeLateralJoinAllowsProjectedOrderLimitVisibility(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
		}},
	}

	limit := 1
	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: FuncExpr{
				Name: "ABS",
				Args: []Expr{CaseExpr{
					Whens: []CaseWhen{
						{
							When: BinaryExpr{
								Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
								Op:    ">=",
								Right: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
							},
							Then: CaseExpr{
								Whens: []CaseWhen{
									{
										When: BinaryExpr{
											Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
											Op:    ">=",
											Right: LiteralExpr{Value: 0},
										},
										Then: BinaryExpr{
											Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
											Op:    "-",
											Right: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
										},
									},
								},
								Else: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
							},
						},
					},
					Else: CaseExpr{
						Whens: []CaseWhen{
							{
								When: BinaryExpr{
									Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
									Op:    ">=",
									Right: LiteralExpr{Value: 0},
								},
								Then: BinaryExpr{
									Left:  ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
									Op:    "-",
									Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
								},
							},
						},
						Else: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
					},
				}},
			}, Alias: "score0"},
			{Expr: FuncExpr{
				Name: "ABS",
				Args: []Expr{CaseExpr{
					Whens: []CaseWhen{
						{
							When: BinaryExpr{
								Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
								Op:    ">=",
								Right: LiteralExpr{Value: 0},
							},
							Then: BinaryExpr{
								Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
								Op:    "+",
								Right: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
							},
						},
					},
					Else: BinaryExpr{
						Left:  ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
						Op:    "-",
						Right: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
					},
				}},
			}, Alias: "tie0"},
		},
		From: FromClause{BaseTable: "t2"},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
			Op:    "<>",
			Right: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
		},
		OrderBy: []OrderBy{
			{
				Expr: ColumnExpr{Ref: ColumnRef{Name: "score0", Type: schema.TypeInt}},
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{Name: "tie0", Type: schema.TypeInt}},
				Desc: true,
			},
			{
				Expr: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}},
			},
		},
		Limit: &limit,
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"id"},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}}, Alias: "id"},
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "dt", Name: "score0", Type: schema.TypeInt}}, Alias: "score0"},
		},
	}

	if !gen.validateQueryScope(query) {
		t.Fatalf("expected non-grouped LATERAL ORDER BY/LIMIT to see merged outer column and projected score alias")
	}
}

func TestValidateQueryScopeLateralJoinUsingRejectsQualifiedMergedColumn(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
				},
			},
		}},
	}

	lateral := &SelectQuery{
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}}, Alias: "id"},
		},
		From: FromClause{BaseTable: "t2"},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t2", Name: "id", Type: schema.TypeInt}},
			Op:    "=",
			Right: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "id", Type: schema.TypeInt}},
		},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"id"},
				},
				{
					Type:       JoinInner,
					Lateral:    true,
					Table:      "dt",
					TableAlias: "dt",
					TableQuery: lateral,
					On:         BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 1}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Name: "id", Type: schema.TypeInt}}, Alias: "id"},
		},
	}

	if gen.validateQueryScope(query) {
		t.Fatalf("expected LATERAL derived table to reject qualified merged USING column")
	}
}

func TestValidateQueryScopeNestedDerivedUsingHidesQualifiedColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
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
		}},
	}

	derived := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:  JoinInner,
					Table: "t1",
					Using: []string{"k0"},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t0", Name: "k0", Type: schema.TypeInt}}, Alias: "k0"},
		},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseAlias: "d0",
			BaseQuery: derived,
		},
		Items: []SelectItem{
			{Expr: LiteralExpr{Value: 1}, Alias: "c0"},
		},
	}

	if gen.validateQueryScope(query) {
		t.Fatalf("expected nested derived query scope validation to hide USING-qualified column")
	}
}

func TestValidateQueryScopeNaturalJoinHidesQualifiedColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "k0", Type: schema.TypeInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
		}},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:    JoinInner,
					Table:   "t1",
					Natural: true,
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "k0", Type: schema.TypeInt}}},
		},
	}
	if gen.validateQueryScope(query) {
		t.Fatalf("expected NATURAL join common column to be hidden for qualified references")
	}
}

func TestValidateQueryScopeNaturalJoinKeepsNonCommonColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
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
					{Name: "c1", Type: schema.TypeInt},
				},
			},
		}},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t0",
			Joins: []Join{
				{
					Type:    JoinInner,
					Table:   "t1",
					Natural: true,
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "c1", Type: schema.TypeInt}}},
		},
	}
	if !gen.validateQueryScope(query) {
		t.Fatalf("expected NATURAL join non-common columns to remain visible")
	}
}

func TestValidateQueryScopeNaturalRightJoinHidesQualifiedCommonColumns(t *testing.T) {
	gen := &Generator{
		State: &schema.State{Tables: []schema.Table{
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k0", Type: schema.TypeVarchar},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeInt},
					{Name: "k0", Type: schema.TypeVarchar},
				},
			},
			{
				Name: "v0",
				Columns: []schema.Column{
					{Name: "g0", Type: schema.TypeInt},
				},
			},
		}},
	}
	query := &SelectQuery{
		From: FromClause{
			BaseTable: "t1",
			Joins: []Join{
				{
					Type:    JoinRight,
					Table:   "t2",
					Natural: true,
				},
				{
					Type:  JoinInner,
					Table: "v0",
					On:    BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 0}},
				},
			},
		},
		Items: []SelectItem{
			{Expr: ColumnExpr{Ref: ColumnRef{Table: "v0", Name: "g0", Type: schema.TypeInt}}},
		},
		Where: BinaryExpr{
			Left:  ColumnExpr{Ref: ColumnRef{Table: "t1", Name: "id", Type: schema.TypeInt}},
			Op:    ">",
			Right: LiteralExpr{Value: 0},
		},
	}
	if gen.validateQueryScope(query) {
		t.Fatalf("expected NATURAL RIGHT JOIN common column to be hidden for qualified references")
	}
}
