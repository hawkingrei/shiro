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
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], false)
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
	query := gen.buildGroupedAggregateLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], true)
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
