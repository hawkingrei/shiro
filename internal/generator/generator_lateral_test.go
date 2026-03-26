package generator

import (
	"testing"

	"shiro/internal/schema"
	"shiro/internal/validator"
)

func TestBuildCorrelatedOrderLimitLateralQuery(t *testing.T) {
	gen := newTestGenerator(t)
	gen.Config.Features.Limit = true
	query := gen.buildCorrelatedOrderLimitLateralQuery([]schema.Table{gen.State.Tables[0]}, gen.State.Tables[1])
	if query == nil {
		t.Fatalf("expected correlated lateral order-limit query")
	}
	if query.Where == nil {
		t.Fatalf("expected correlation predicate in lateral query")
	}
	if len(query.OrderBy) == 0 {
		t.Fatalf("expected ORDER BY in lateral query")
	}
	if query.Limit == nil {
		t.Fatalf("expected LIMIT in lateral query")
	}
}

func TestGenerateSelectQueryExercisesLateralOrderLimitHook(t *testing.T) {
	gen := newTestGenerator(t)
	gen.Config.Features.LateralJoins = true
	gen.Config.Features.Limit = true
	gen.Config.Features.CTE = false
	gen.Config.Features.Aggregates = false
	gen.Config.Features.GroupBy = false
	gen.Config.Features.Having = false
	gen.Config.Features.Distinct = false
	gen.Config.Features.WindowFuncs = false
	gen.Config.Features.SetOperations = false
	gen.Config.Features.DerivedTables = false
	gen.Config.Features.FullJoinEmulation = false
	gen.Config.Features.NaturalJoins = false
	gen.Config.MaxJoinTables = 2
	gen.SetMinJoinTables(2)
	gen.SetJoinTypeOverride(JoinInner)

	v := validator.New()
	for i := 0; i < 400; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasCorrelatedLateralOrderLimitHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise correlated lateral ORDER BY + LIMIT hook")
}

func TestBuildCorrelatedAggregateLateralHookQuery(t *testing.T) {
	gen := newAggregateLateralTestGenerator(t)
	query := gen.buildCorrelatedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2])
	if query == nil {
		t.Fatalf("expected correlated aggregate LATERAL hook query")
	}
	if len(query.From.Joins) != 2 {
		t.Fatalf("expected join plus LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in aggregate hook query")
	}
	if !selectItemsContainAggregate(lateral.TableQuery.Items) {
		t.Fatalf("expected aggregate select list inside lateral query")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) < 2 {
		t.Fatalf("expected lateral aggregate predicate to reference two left-side tables")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected correlated aggregate LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedAggregateLateralHookQueryGroupBy(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], groupedAggregateLateralModeWhere)
	if query == nil {
		t.Fatalf("expected grouped aggregate LATERAL hook query")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped aggregate hook query")
	}
	if len(lateral.TableQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped aggregate hook")
	}
	if lateral.TableQuery.Having != nil {
		t.Fatalf("expected pure GROUP BY variant without HAVING")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) < 2 {
		t.Fatalf("expected grouped aggregate WHERE to reference two left-side tables")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped aggregate LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedAggregateLateralHookQueryHaving(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], groupedAggregateLateralModeGroupKeyHaving)
	if query == nil {
		t.Fatalf("expected grouped aggregate LATERAL HAVING hook query")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped aggregate HAVING hook query")
	}
	if len(lateral.TableQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped aggregate HAVING hook")
	}
	if lateral.TableQuery.Having == nil {
		t.Fatalf("expected HAVING inside grouped aggregate hook")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if countVisibleTables(lateral.TableQuery.Having, visible) == 0 {
		t.Fatalf("expected HAVING to reference left-side tables")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped aggregate HAVING LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedAggregateLateralHookQueryOuterFilteredWhere(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], groupedAggregateLateralModeOuterFilteredWhere)
	if query == nil {
		t.Fatalf("expected grouped aggregate LATERAL outer-filtered WHERE hook query")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped aggregate outer-filtered WHERE hook query")
	}
	if len(lateral.TableQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped aggregate outer-filtered WHERE hook")
	}
	if lateral.TableQuery.Having != nil {
		t.Fatalf("expected grouped filter variant to keep correlation in WHERE only")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) < 2 {
		t.Fatalf("expected grouped aggregate outer-filtered WHERE to reference two left-side tables")
	}
	if !exprUsesAggregateSourceColumn(lateral.TableQuery.Where, lateral.TableQuery.Items) {
		t.Fatalf("expected grouped aggregate outer-filtered WHERE to reference an aggregate source column")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped aggregate outer-filtered WHERE LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedAggregateLateralHookQueryMultiFilteredWhere(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], groupedAggregateLateralModeMultiFilteredWhere)
	if query == nil {
		t.Fatalf("expected grouped aggregate LATERAL multi-filtered WHERE hook query")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped aggregate multi-filtered WHERE hook query")
	}
	if len(lateral.TableQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped aggregate multi-filtered WHERE hook")
	}
	if lateral.TableQuery.Having != nil {
		t.Fatalf("expected multi-filter variant to keep correlation in WHERE only")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) < 2 {
		t.Fatalf("expected grouped aggregate multi-filtered WHERE to reference two left-side tables")
	}
	if !exprUsesAggregateSourceColumn(lateral.TableQuery.Where, lateral.TableQuery.Items) {
		t.Fatalf("expected grouped aggregate multi-filtered WHERE to reference an aggregate source column")
	}
	if !exprUsesGroupByColumn(lateral.TableQuery.Where, lateral.TableQuery.GroupBy) {
		t.Fatalf("expected grouped aggregate multi-filtered WHERE to reference a grouped column")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped aggregate multi-filtered WHERE LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedAggregateLateralHookQueryOuterCorrelatedGroupKey(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], groupedAggregateLateralModeOuterCorrelatedGroupKey)
	if query == nil {
		t.Fatalf("expected grouped aggregate LATERAL outer-correlated group-key hook query")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped aggregate outer-correlated group-key hook query")
	}
	if len(lateral.TableQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped aggregate outer-correlated group-key hook")
	}
	if lateral.TableQuery.Having != nil {
		t.Fatalf("expected outer-correlated group-key variant to keep correlation out of HAVING")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) == 0 {
		t.Fatalf("expected grouped aggregate outer-correlated group-key hook to keep base correlation in WHERE")
	}
	if countVisibleTables(lateral.TableQuery.GroupBy[0], visible) == 0 {
		t.Fatalf("expected grouped aggregate outer-correlated group key to reference left-side tables")
	}
	if !exprUsesTable(lateral.TableQuery.GroupBy[0], lateral.TableQuery.From.baseName()) {
		t.Fatalf("expected grouped aggregate outer-correlated group key to still reference the inner table")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped aggregate outer-correlated group-key LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedAggregateLateralHookQueryCaseCorrelatedGroupKey(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], groupedAggregateLateralModeCaseCorrelatedGroupKey)
	if query == nil {
		t.Fatalf("expected grouped aggregate LATERAL case-correlated group-key hook query")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped aggregate case-correlated group-key hook query")
	}
	if len(lateral.TableQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped aggregate case-correlated group-key hook")
	}
	if lateral.TableQuery.Having != nil {
		t.Fatalf("expected case-correlated group-key variant to keep correlation out of HAVING")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) == 0 {
		t.Fatalf("expected grouped aggregate case-correlated group-key hook to keep base correlation in WHERE")
	}
	if !exprContainsCase(lateral.TableQuery.GroupBy[0]) {
		t.Fatalf("expected grouped aggregate case-correlated group key to use CASE")
	}
	if countVisibleTables(lateral.TableQuery.GroupBy[0], visible) == 0 {
		t.Fatalf("expected grouped aggregate case-correlated group key to reference left-side tables")
	}
	if !exprUsesTable(lateral.TableQuery.GroupBy[0], lateral.TableQuery.From.baseName()) {
		t.Fatalf("expected grouped aggregate case-correlated group key to still reference the inner table")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped aggregate case-correlated group-key LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedAggregateLateralHookQueryNestedCaseCorrelatedGroupKey(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], groupedAggregateLateralModeNestedCaseCorrelatedGroupKey)
	if query == nil {
		t.Fatalf("expected grouped aggregate LATERAL nested-case-correlated group-key hook query")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped aggregate nested-case-correlated group-key hook query")
	}
	if len(lateral.TableQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped aggregate nested-case-correlated group-key hook")
	}
	if lateral.TableQuery.Having != nil {
		t.Fatalf("expected nested-case-correlated group-key variant to keep correlation out of HAVING")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) == 0 {
		t.Fatalf("expected grouped aggregate nested-case-correlated group-key hook to keep base correlation in WHERE")
	}
	if caseExprDepth(lateral.TableQuery.GroupBy[0]) < 2 {
		t.Fatalf("expected grouped aggregate nested-case-correlated group key to use nested CASE")
	}
	if countVisibleTables(lateral.TableQuery.GroupBy[0], visible) == 0 {
		t.Fatalf("expected grouped aggregate nested-case-correlated group key to reference left-side tables")
	}
	if !exprUsesTable(lateral.TableQuery.GroupBy[0], lateral.TableQuery.From.baseName()) {
		t.Fatalf("expected grouped aggregate nested-case-correlated group key to still reference the inner table")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped aggregate nested-case-correlated group-key LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedAggregateLateralHookQueryWrappedNestedCaseCorrelatedGroupKey(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], groupedAggregateLateralModeWrappedNestedCaseCorrelatedGroupKey)
	if query == nil {
		t.Fatalf("expected grouped aggregate LATERAL wrapped-nested-case-correlated group-key hook query")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped aggregate wrapped-nested-case-correlated group-key hook query")
	}
	if len(lateral.TableQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped aggregate wrapped-nested-case-correlated group-key hook")
	}
	if lateral.TableQuery.Having != nil {
		t.Fatalf("expected wrapped-nested-case-correlated group-key variant to keep correlation out of HAVING")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) == 0 {
		t.Fatalf("expected grouped aggregate wrapped-nested-case-correlated group-key hook to keep base correlation in WHERE")
	}
	if caseExprDepth(lateral.TableQuery.GroupBy[0]) < 2 {
		t.Fatalf("expected grouped aggregate wrapped-nested-case-correlated group key to use nested CASE")
	}
	if !exprContainsFuncName(lateral.TableQuery.GroupBy[0], "ABS") {
		t.Fatalf("expected grouped aggregate wrapped-nested-case-correlated group key to use ABS")
	}
	if countVisibleTables(lateral.TableQuery.GroupBy[0], visible) == 0 {
		t.Fatalf("expected grouped aggregate wrapped-nested-case-correlated group key to reference left-side tables")
	}
	if !exprUsesTable(lateral.TableQuery.GroupBy[0], lateral.TableQuery.From.baseName()) {
		t.Fatalf("expected grouped aggregate wrapped-nested-case-correlated group key to still reference the inner table")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped aggregate wrapped-nested-case-correlated group-key LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedAggregateLateralHookQueryAggregateValuedHaving(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], groupedAggregateLateralModeAggregateValueHaving)
	if query == nil {
		t.Fatalf("expected grouped aggregate LATERAL aggregate-valued HAVING hook query")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped aggregate aggregate-valued HAVING hook query")
	}
	if len(lateral.TableQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped aggregate aggregate-valued HAVING hook")
	}
	if lateral.TableQuery.Having == nil {
		t.Fatalf("expected HAVING inside grouped aggregate aggregate-valued hook")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if !exprContainsAggregate(lateral.TableQuery.Having) {
		t.Fatalf("expected aggregate-valued HAVING expression")
	}
	if countVisibleTables(lateral.TableQuery.Having, visible) == 0 {
		t.Fatalf("expected aggregate-valued HAVING to reference left-side tables")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped aggregate aggregate-valued HAVING LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestGenerateSelectQueryExercisesCorrelatedAggregateLateralHook(t *testing.T) {
	gen := newAggregateLateralTestGenerator(t)
	v := validator.New()
	for i := 0; i < 400; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasCorrelatedAggregateLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated aggregate lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise correlated aggregate LATERAL hook")
}

func TestGenerateSelectQueryExercisesGroupedAggregateLateralHook(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	v := validator.New()
	for i := 0; i < 400; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasGroupedAggregateLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated grouped aggregate lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise grouped aggregate LATERAL hook")
}

func TestGenerateSelectQueryExercisesOuterFilteredGroupedAggregateLateralHook(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	v := validator.New()
	for i := 0; i < 800; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasOuterFilteredGroupedAggregateLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated outer-filtered grouped aggregate lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise outer-filtered grouped aggregate lateral hook")
}

func TestGenerateSelectQueryExercisesMultiFilteredGroupedAggregateLateralHook(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	v := validator.New()
	for i := 0; i < 800; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasMultiFilteredGroupedAggregateLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated multi-filtered grouped aggregate lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise multi-filtered grouped aggregate lateral hook")
}

func TestGenerateSelectQueryExercisesOuterCorrelatedGroupKeyLateralHook(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	v := validator.New()
	for i := 0; i < 800; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasOuterCorrelatedGroupKeyLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated outer-correlated grouped-key lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise outer-correlated grouped-key lateral hook")
}

func TestGenerateSelectQueryExercisesCaseCorrelatedGroupKeyLateralHook(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	v := validator.New()
	for i := 0; i < 800; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasCaseCorrelatedGroupKeyLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated case-correlated grouped-key lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise case-correlated grouped-key lateral hook")
}

func TestGenerateSelectQueryExercisesNestedCaseCorrelatedGroupKeyLateralHook(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	v := validator.New()
	for i := 0; i < 800; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasNestedCaseCorrelatedGroupKeyLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated nested-case-correlated grouped-key lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise nested-case-correlated grouped-key lateral hook")
}

func TestGenerateSelectQueryExercisesWrappedNestedCaseCorrelatedGroupKeyLateralHook(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	v := validator.New()
	for i := 0; i < 800; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasWrappedNestedCaseCorrelatedGroupKeyLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated wrapped-nested-case-correlated grouped-key lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise wrapped-nested-case-correlated grouped-key lateral hook")
}

func TestGenerateSelectQueryExercisesAggregateValuedHavingLateralHook(t *testing.T) {
	gen := newGroupedAggregateLateralTestGenerator(t)
	v := validator.New()
	for i := 0; i < 800; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasAggregateValuedHavingLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated aggregate-valued HAVING lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise aggregate-valued HAVING lateral hook")
}

func newGroupedAggregateLateralTestGenerator(t *testing.T) *Generator {
	t.Helper()
	gen := newTestGenerator(t)
	gen.Config.Features.LateralJoins = true
	gen.Config.Features.Limit = false
	gen.Config.Features.CTE = false
	gen.Config.Features.Aggregates = true
	gen.Config.Features.GroupBy = true
	gen.Config.Features.Having = true
	gen.Config.Features.Distinct = false
	gen.Config.Features.WindowFuncs = false
	gen.Config.Features.SetOperations = false
	gen.Config.Features.DerivedTables = false
	gen.Config.Features.FullJoinEmulation = false
	gen.Config.Features.NaturalJoins = false
	gen.Config.MaxJoinTables = 3
	gen.SetMinJoinTables(3)
	gen.SetJoinTypeOverride(JoinInner)
	gen.State = &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c2", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		},
	}
	return gen
}

func TestBuildMergedColumnVisibilityLateralHookQueryUsing(t *testing.T) {
	gen := newMergedVisibilityTestGenerator(t)
	gen.Config.Features.NaturalJoins = false
	query := gen.buildMergedColumnVisibilityLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], false)
	if query == nil {
		t.Fatalf("expected merged-column LATERAL hook query for USING")
	}
	if len(query.From.Joins) != 2 || len(query.From.Joins[0].Using) == 0 {
		t.Fatalf("expected USING join before LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in hook query")
	}
	usingCol := query.From.Joins[0].Using[0]
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.Where, usingCol) {
		t.Fatalf("expected lateral predicate to reference merged USING column %q unqualified", usingCol)
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected merged-column USING LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildMergedColumnVisibilityLateralHookQueryNatural(t *testing.T) {
	gen := newMergedVisibilityTestGenerator(t)
	query := gen.buildMergedColumnVisibilityLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], true)
	if query == nil {
		t.Fatalf("expected merged-column LATERAL hook query for NATURAL join")
	}
	if len(query.From.Joins) != 2 || !query.From.Joins[0].Natural {
		t.Fatalf("expected NATURAL join before LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in NATURAL hook query")
	}
	if !exprHasUnqualifiedColumn(lateral.TableQuery.Where) {
		t.Fatalf("expected lateral predicate to reference a merged NATURAL column unqualified")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected merged-column NATURAL LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestGenerateSelectQueryExercisesMergedColumnLateralHook(t *testing.T) {
	gen := newMergedVisibilityTestGenerator(t)
	v := validator.New()
	for i := 0; i < 400; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasMergedColumnLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated merged-column lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise merged-column LATERAL hook")
}

func newAggregateLateralTestGenerator(t *testing.T) *Generator {
	t.Helper()
	gen := newTestGenerator(t)
	gen.Config.Features.LateralJoins = true
	gen.Config.Features.Limit = false
	gen.Config.Features.CTE = false
	gen.Config.Features.Aggregates = true
	gen.Config.Features.GroupBy = false
	gen.Config.Features.Having = false
	gen.Config.Features.Distinct = false
	gen.Config.Features.WindowFuncs = false
	gen.Config.Features.SetOperations = false
	gen.Config.Features.DerivedTables = false
	gen.Config.Features.FullJoinEmulation = false
	gen.Config.Features.NaturalJoins = false
	gen.Config.MaxJoinTables = 3
	gen.SetMinJoinTables(3)
	gen.SetJoinTypeOverride(JoinInner)
	gen.State = &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c2", Type: schema.TypeInt},
					{Name: "v0", Type: schema.TypeInt},
				},
			},
		},
	}
	return gen
}

func newMergedVisibilityTestGenerator(t *testing.T) *Generator {
	t.Helper()
	gen := newTestGenerator(t)
	gen.Config.Features.LateralJoins = true
	gen.Config.Features.Limit = false
	gen.Config.Features.CTE = false
	gen.Config.Features.Aggregates = false
	gen.Config.Features.GroupBy = false
	gen.Config.Features.Having = false
	gen.Config.Features.Distinct = false
	gen.Config.Features.WindowFuncs = false
	gen.Config.Features.SetOperations = false
	gen.Config.Features.DerivedTables = false
	gen.Config.Features.FullJoinEmulation = false
	gen.Config.Features.NaturalJoins = true
	gen.Config.MaxJoinTables = 3
	gen.SetMinJoinTables(3)
	gen.SetJoinTypeOverride(JoinInner)
	gen.State = &schema.State{
		Tables: []schema.Table{
			{
				Name: "t0",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c0", Type: schema.TypeInt},
				},
			},
			{
				Name: "t1",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c1", Type: schema.TypeInt},
				},
			},
			{
				Name: "t2",
				Columns: []schema.Column{
					{Name: "id", Type: schema.TypeBigInt},
					{Name: "c2", Type: schema.TypeInt},
				},
			},
		},
	}
	return gen
}

func queryHasCorrelatedLateralOrderLimitHook(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Lateral && join.TableQuery != nil && join.TableQuery.Limit != nil && len(join.TableQuery.OrderBy) > 0 && exprUsesVisibleTable(join.TableQuery.Where, visible) {
			return true
		}
		if name := join.tableName(); name != "" {
			visible[name] = struct{}{}
		}
	}
	return false
}

func exprUsesVisibleTable(expr Expr, visible map[string]struct{}) bool {
	if expr == nil {
		return false
	}
	for _, col := range expr.Columns() {
		if _, ok := visible[col.Table]; ok {
			return true
		}
	}
	return false
}

func queryHasCorrelatedAggregateLateralHook(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Lateral && join.TableQuery != nil && selectItemsContainAggregate(join.TableQuery.Items) && countVisibleTables(join.TableQuery.Where, visible) >= 2 {
			return true
		}
		if name := join.tableName(); name != "" {
			visible[name] = struct{}{}
		}
	}
	return false
}

func queryHasGroupedAggregateLateralHook(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Lateral && join.TableQuery != nil && len(join.TableQuery.GroupBy) > 0 && selectItemsContainAggregate(join.TableQuery.Items) {
			if join.TableQuery.Having != nil && countVisibleTables(join.TableQuery.Having, visible) > 0 {
				return true
			}
			if countVisibleTables(join.TableQuery.Where, visible) >= 2 {
				return true
			}
		}
		if name := join.tableName(); name != "" {
			visible[name] = struct{}{}
		}
	}
	return false
}

func queryHasOuterFilteredGroupedAggregateLateralHook(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Lateral && join.TableQuery != nil && len(join.TableQuery.GroupBy) > 0 && join.TableQuery.Having == nil {
			if countVisibleTables(join.TableQuery.Where, visible) >= 2 && exprUsesAggregateSourceColumn(join.TableQuery.Where, join.TableQuery.Items) {
				return true
			}
		}
		if name := join.tableName(); name != "" {
			visible[name] = struct{}{}
		}
	}
	return false
}

func queryHasMultiFilteredGroupedAggregateLateralHook(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Lateral && join.TableQuery != nil && len(join.TableQuery.GroupBy) > 0 && join.TableQuery.Having == nil {
			if countVisibleTables(join.TableQuery.Where, visible) >= 2 &&
				exprUsesAggregateSourceColumn(join.TableQuery.Where, join.TableQuery.Items) &&
				exprUsesGroupByColumn(join.TableQuery.Where, join.TableQuery.GroupBy) {
				return true
			}
		}
		if name := join.tableName(); name != "" {
			visible[name] = struct{}{}
		}
	}
	return false
}

func queryHasOuterCorrelatedGroupKeyLateralHook(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Lateral && join.TableQuery != nil && len(join.TableQuery.GroupBy) > 0 && join.TableQuery.Having == nil {
			outerGroupKey := false
			innerGroupKey := false
			for _, expr := range join.TableQuery.GroupBy {
				if countVisibleTables(expr, visible) > 0 {
					outerGroupKey = true
				}
				if exprUsesTable(expr, join.TableQuery.From.baseName()) {
					innerGroupKey = true
				}
			}
			if outerGroupKey && innerGroupKey {
				return true
			}
		}
		if name := join.tableName(); name != "" {
			visible[name] = struct{}{}
		}
	}
	return false
}

func queryHasCaseCorrelatedGroupKeyLateralHook(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Lateral && join.TableQuery != nil && len(join.TableQuery.GroupBy) > 0 && join.TableQuery.Having == nil {
			caseGroupKey := false
			outerGroupKey := false
			innerGroupKey := false
			for _, expr := range join.TableQuery.GroupBy {
				if exprContainsCase(expr) {
					caseGroupKey = true
				}
				if countVisibleTables(expr, visible) > 0 {
					outerGroupKey = true
				}
				if exprUsesTable(expr, join.TableQuery.From.baseName()) {
					innerGroupKey = true
				}
			}
			if caseGroupKey && outerGroupKey && innerGroupKey {
				return true
			}
		}
		if name := join.tableName(); name != "" {
			visible[name] = struct{}{}
		}
	}
	return false
}

func queryHasNestedCaseCorrelatedGroupKeyLateralHook(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Lateral && join.TableQuery != nil && len(join.TableQuery.GroupBy) > 0 && join.TableQuery.Having == nil {
			nestedCaseGroupKey := false
			outerGroupKey := false
			innerGroupKey := false
			for _, expr := range join.TableQuery.GroupBy {
				if caseExprDepth(expr) >= 2 {
					nestedCaseGroupKey = true
				}
				if countVisibleTables(expr, visible) > 0 {
					outerGroupKey = true
				}
				if exprUsesTable(expr, join.TableQuery.From.baseName()) {
					innerGroupKey = true
				}
			}
			if nestedCaseGroupKey && outerGroupKey && innerGroupKey {
				return true
			}
		}
		if name := join.tableName(); name != "" {
			visible[name] = struct{}{}
		}
	}
	return false
}

func queryHasWrappedNestedCaseCorrelatedGroupKeyLateralHook(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Lateral && join.TableQuery != nil && len(join.TableQuery.GroupBy) > 0 && join.TableQuery.Having == nil {
			wrappedNestedCaseGroupKey := false
			outerGroupKey := false
			innerGroupKey := false
			for _, expr := range join.TableQuery.GroupBy {
				if caseExprDepth(expr) >= 2 && exprContainsFuncName(expr, "ABS") {
					wrappedNestedCaseGroupKey = true
				}
				if countVisibleTables(expr, visible) > 0 {
					outerGroupKey = true
				}
				if exprUsesTable(expr, join.TableQuery.From.baseName()) {
					innerGroupKey = true
				}
			}
			if wrappedNestedCaseGroupKey && outerGroupKey && innerGroupKey {
				return true
			}
		}
		if name := join.tableName(); name != "" {
			visible[name] = struct{}{}
		}
	}
	return false
}

func queryHasAggregateValuedHavingLateralHook(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Lateral && join.TableQuery != nil && len(join.TableQuery.GroupBy) > 0 && join.TableQuery.Having != nil {
			if exprContainsAggregate(join.TableQuery.Having) && countVisibleTables(join.TableQuery.Having, visible) > 0 {
				return true
			}
		}
		if name := join.tableName(); name != "" {
			visible[name] = struct{}{}
		}
	}
	return false
}

func queryHasMergedColumnLateralHook(query *SelectQuery) bool {
	if query == nil || len(query.From.Joins) < 2 {
		return false
	}
	mergedJoin := query.From.Joins[0]
	if !mergedJoin.Natural && len(mergedJoin.Using) == 0 {
		return false
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		return false
	}
	if mergedJoin.Natural {
		return exprHasUnqualifiedColumn(lateral.TableQuery.Where)
	}
	for _, name := range mergedJoin.Using {
		if exprUsesUnqualifiedColumnName(lateral.TableQuery.Where, name) {
			return true
		}
	}
	return false
}

func selectItemsContainAggregate(items []SelectItem) bool {
	for _, item := range items {
		fn, ok := item.Expr.(FuncExpr)
		if ok && isAggregateFunc(fn.Name) {
			return true
		}
	}
	return false
}

func countVisibleTables(expr Expr, visible map[string]struct{}) int {
	if expr == nil {
		return 0
	}
	seen := make(map[string]struct{}, len(visible))
	for _, col := range expr.Columns() {
		if _, ok := visible[col.Table]; !ok {
			continue
		}
		seen[col.Table] = struct{}{}
	}
	return len(seen)
}

func exprHasUnqualifiedColumn(expr Expr) bool {
	if expr == nil {
		return false
	}
	for _, col := range expr.Columns() {
		if col.Table == "" && col.Name != "" {
			return true
		}
	}
	return false
}

func exprUsesUnqualifiedColumnName(expr Expr, name string) bool {
	if expr == nil || name == "" {
		return false
	}
	for _, col := range expr.Columns() {
		if col.Table == "" && col.Name == name {
			return true
		}
	}
	return false
}

func exprContainsAggregate(expr Expr) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case FuncExpr:
		if isAggregateFunc(e.Name) {
			return true
		}
		for _, arg := range e.Args {
			if exprContainsAggregate(arg) {
				return true
			}
		}
		return false
	case UnaryExpr:
		return exprContainsAggregate(e.Expr)
	case BinaryExpr:
		return exprContainsAggregate(e.Left) || exprContainsAggregate(e.Right)
	case GroupByOrdinalExpr:
		return exprContainsAggregate(e.Expr)
	case CaseExpr:
		for _, when := range e.Whens {
			if exprContainsAggregate(when.When) || exprContainsAggregate(when.Then) {
				return true
			}
		}
		return exprContainsAggregate(e.Else)
	case InExpr:
		if exprContainsAggregate(e.Left) {
			return true
		}
		for _, item := range e.List {
			if exprContainsAggregate(item) {
				return true
			}
		}
		return false
	case CompareSubqueryExpr:
		return exprContainsAggregate(e.Left)
	case WindowExpr:
		for _, arg := range e.Args {
			if exprContainsAggregate(arg) {
				return true
			}
		}
		for _, expr := range e.PartitionBy {
			if exprContainsAggregate(expr) {
				return true
			}
		}
		for _, ob := range e.OrderBy {
			if exprContainsAggregate(ob.Expr) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func exprUsesAggregateSourceColumn(expr Expr, items []SelectItem) bool {
	if expr == nil {
		return false
	}
	aggCols := aggregateSourceColumns(items)
	if len(aggCols) == 0 {
		return false
	}
	for _, used := range expr.Columns() {
		for _, aggCol := range aggCols {
			if used.Table == aggCol.Table && used.Name == aggCol.Name {
				return true
			}
		}
	}
	return false
}

func exprUsesGroupByColumn(expr Expr, groupBy []Expr) bool {
	if expr == nil || len(groupBy) == 0 {
		return false
	}
	groupCols := make([]ColumnRef, 0, len(groupBy))
	for _, item := range groupBy {
		groupCols = append(groupCols, item.Columns()...)
	}
	if len(groupCols) == 0 {
		return false
	}
	for _, used := range expr.Columns() {
		for _, groupCol := range groupCols {
			if used.Table == groupCol.Table && used.Name == groupCol.Name {
				return true
			}
		}
	}
	return false
}

func exprUsesTable(expr Expr, table string) bool {
	if expr == nil || table == "" {
		return false
	}
	for _, col := range expr.Columns() {
		if col.Table == table {
			return true
		}
	}
	return false
}

func exprContainsCase(expr Expr) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case CaseExpr:
		return true
	case UnaryExpr:
		return exprContainsCase(e.Expr)
	case BinaryExpr:
		return exprContainsCase(e.Left) || exprContainsCase(e.Right)
	case FuncExpr:
		for _, arg := range e.Args {
			if exprContainsCase(arg) {
				return true
			}
		}
		return false
	case GroupByOrdinalExpr:
		return exprContainsCase(e.Expr)
	default:
		return false
	}
}

func exprContainsFuncName(expr Expr, name string) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case FuncExpr:
		if e.Name == name {
			return true
		}
		for _, arg := range e.Args {
			if exprContainsFuncName(arg, name) {
				return true
			}
		}
		return false
	case UnaryExpr:
		return exprContainsFuncName(e.Expr, name)
	case BinaryExpr:
		return exprContainsFuncName(e.Left, name) || exprContainsFuncName(e.Right, name)
	case CaseExpr:
		for _, when := range e.Whens {
			if exprContainsFuncName(when.When, name) || exprContainsFuncName(when.Then, name) {
				return true
			}
		}
		return exprContainsFuncName(e.Else, name)
	case GroupByOrdinalExpr:
		return exprContainsFuncName(e.Expr, name)
	default:
		return false
	}
}

func caseExprDepth(expr Expr) int {
	switch e := expr.(type) {
	case nil:
		return 0
	case CaseExpr:
		depth := 1
		for _, when := range e.Whens {
			if child := caseExprDepth(when.When); child+1 > depth {
				depth = child + 1
			}
			if child := caseExprDepth(when.Then); child+1 > depth {
				depth = child + 1
			}
		}
		if child := caseExprDepth(e.Else); child+1 > depth {
			depth = child + 1
		}
		return depth
	case UnaryExpr:
		return caseExprDepth(e.Expr)
	case BinaryExpr:
		left := caseExprDepth(e.Left)
		right := caseExprDepth(e.Right)
		if left > right {
			return left
		}
		return right
	case FuncExpr:
		depth := 0
		for _, arg := range e.Args {
			if child := caseExprDepth(arg); child > depth {
				depth = child
			}
		}
		return depth
	case GroupByOrdinalExpr:
		return caseExprDepth(e.Expr)
	default:
		return 0
	}
}

func aggregateSourceColumns(items []SelectItem) []ColumnRef {
	cols := make([]ColumnRef, 0, len(items))
	for _, item := range items {
		cols = append(cols, aggregateSourceColumnsFromExpr(item.Expr)...)
	}
	return cols
}

func aggregateSourceColumnsFromExpr(expr Expr) []ColumnRef {
	switch e := expr.(type) {
	case nil:
		return nil
	case FuncExpr:
		if isAggregateFunc(e.Name) {
			cols := make([]ColumnRef, 0, len(e.Args))
			for _, arg := range e.Args {
				cols = append(cols, arg.Columns()...)
			}
			return cols
		}
		cols := make([]ColumnRef, 0, len(e.Args))
		for _, arg := range e.Args {
			cols = append(cols, aggregateSourceColumnsFromExpr(arg)...)
		}
		return cols
	case UnaryExpr:
		return aggregateSourceColumnsFromExpr(e.Expr)
	case BinaryExpr:
		cols := aggregateSourceColumnsFromExpr(e.Left)
		return append(cols, aggregateSourceColumnsFromExpr(e.Right)...)
	case GroupByOrdinalExpr:
		return aggregateSourceColumnsFromExpr(e.Expr)
	case CaseExpr:
		cols := make([]ColumnRef, 0, len(e.Whens)*2)
		for _, when := range e.Whens {
			cols = append(cols, aggregateSourceColumnsFromExpr(when.When)...)
			cols = append(cols, aggregateSourceColumnsFromExpr(when.Then)...)
		}
		return append(cols, aggregateSourceColumnsFromExpr(e.Else)...)
	case InExpr:
		cols := aggregateSourceColumnsFromExpr(e.Left)
		for _, item := range e.List {
			cols = append(cols, aggregateSourceColumnsFromExpr(item)...)
		}
		return cols
	case CompareSubqueryExpr:
		return aggregateSourceColumnsFromExpr(e.Left)
	case WindowExpr:
		cols := make([]ColumnRef, 0, len(e.Args)+len(e.PartitionBy)+len(e.OrderBy))
		for _, arg := range e.Args {
			cols = append(cols, aggregateSourceColumnsFromExpr(arg)...)
		}
		for _, item := range e.PartitionBy {
			cols = append(cols, aggregateSourceColumnsFromExpr(item)...)
		}
		for _, ob := range e.OrderBy {
			cols = append(cols, aggregateSourceColumnsFromExpr(ob.Expr)...)
		}
		return cols
	default:
		return nil
	}
}
