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

func TestBuildScalarSubqueryProjectedOrderLimitLateralHookQuery(t *testing.T) {
	gen := newScalarSubqueryOrderLimitTestGenerator(t)
	query := gen.buildScalarSubqueryProjectedOrderLimitLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2])
	if query == nil {
		t.Fatalf("expected scalar-subquery projected-order-limit LATERAL hook query")
	}
	if len(query.From.Joins) != 2 {
		t.Fatalf("expected join plus LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in scalar-subquery projected-order-limit hook query")
	}
	if lateral.TableQuery.From.BaseQuery != nil {
		t.Fatalf("expected scalar-subquery projected-order-limit hook to stay direct inside the LATERAL body")
	}
	if len(lateral.TableQuery.GroupBy) != 0 || lateral.TableQuery.Having != nil {
		t.Fatalf("expected scalar-subquery projected-order-limit hook to avoid GROUP BY/HAVING")
	}
	if !selectItemsHaveAliases(lateral.TableQuery.Items, "score0", "tie0") {
		t.Fatalf("expected scalar-subquery projected-order-limit hook to expose score0/tie0 aliases")
	}
	scoreExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "score0")
	if !ok {
		t.Fatalf("expected scalar-subquery projected-order-limit hook to project score0")
	}
	tieExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "tie0")
	if !ok {
		t.Fatalf("expected scalar-subquery projected-order-limit hook to project tie0")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if !exprContainsFuncName(scoreExpr, "ABS") || !exprContainsScalarSubquery(scoreExpr) {
		t.Fatalf("expected scalar-subquery projected-order-limit score0 to wrap a scalar subquery")
	}
	if !exprHasOrderedLimitedScalarSubquery(scoreExpr) || !exprHasScalarSubqueryUsingVisibleTables(scoreExpr, visible, 2) {
		t.Fatalf("expected scalar-subquery projected-order-limit score0 to keep correlated ORDER BY + LIMIT visibility")
	}
	if !exprContainsScalarSubquery(tieExpr) {
		t.Fatalf("expected scalar-subquery projected-order-limit tie0 to project the scalar subquery directly")
	}
	if !exprHasOrderedLimitedScalarSubquery(tieExpr) || !exprHasScalarSubqueryUsingVisibleTables(tieExpr, visible, 2) {
		t.Fatalf("expected scalar-subquery projected-order-limit tie0 to keep correlated ORDER BY + LIMIT visibility")
	}
	scoreSubs := scalarSubquerySignatures(scoreExpr)
	tieSubs := scalarSubquerySignatures(tieExpr)
	if len(scoreSubs) == 0 || len(tieSubs) != 1 || scoreSubs[0] != tieSubs[0] {
		t.Fatalf("expected scalar-subquery projected-order-limit score0 and tie0 to share the same correlated scalar subquery")
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) == 0 {
		t.Fatalf("expected scalar-subquery projected-order-limit WHERE to keep a direct left-side anchor")
	}
	if lateral.TableQuery.Limit == nil || *lateral.TableQuery.Limit != 1 {
		t.Fatalf("expected scalar-subquery projected-order-limit hook to keep LIMIT 1 inside LATERAL")
	}
	if len(lateral.TableQuery.OrderBy) < 3 {
		t.Fatalf("expected scalar-subquery projected-order-limit hook to keep ORDER BY inside LATERAL")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, "score0") {
		t.Fatalf("expected scalar-subquery projected-order-limit ORDER BY to rank by score0 alias")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[1].Expr, "tie0") || !lateral.TableQuery.OrderBy[1].Desc {
		t.Fatalf("expected scalar-subquery projected-order-limit ORDER BY to rank by descending tie0 alias")
	}
	if countVisibleTables(lateral.TableQuery.OrderBy[2].Expr, visible) == 0 {
		t.Fatalf("expected scalar-subquery projected-order-limit ORDER BY tie breaker to keep left-side visibility")
	}
	outerVisible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
		query.From.Joins[1].tableName(): {},
	}
	if query.Where == nil || !exprUsesQualifiedColumnName(query.Where, "dt", "tie0") || countVisibleTables(query.Where, outerVisible) < 2 {
		t.Fatalf("expected outer query WHERE to consume the reused scalar-subquery value after lateral projection")
	}
	if len(query.OrderBy) < 3 {
		t.Fatalf("expected outer query ORDER BY to consume post-lateral projected values")
	}
	if !exprContainsFuncName(query.OrderBy[0].Expr, "ABS") || !exprUsesQualifiedColumnName(query.OrderBy[0].Expr, "dt", "tie0") || countVisibleTables(query.OrderBy[0].Expr, outerVisible) < 2 {
		t.Fatalf("expected outer query ORDER BY to rank by a post-lateral expression over tie0 and the sibling outer table")
	}
	if !exprUsesQualifiedColumnName(query.OrderBy[1].Expr, "dt", "score0") {
		t.Fatalf("expected outer query ORDER BY to keep consuming the lateral projected score alias")
	}
	if countVisibleTables(query.OrderBy[2].Expr, outerVisible) == 0 {
		t.Fatalf("expected outer query ORDER BY tie breaker to keep base-table visibility")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected scalar-subquery projected-order-limit LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestGenerateSelectQueryExercisesScalarSubqueryProjectedOrderLimitLateralHook(t *testing.T) {
	gen := newScalarSubqueryOrderLimitTestGenerator(t)
	v := validator.New()
	for i := 0; i < 400; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasScalarSubqueryProjectedOrderLimitLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated scalar-subquery projected-order-limit lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise scalar-subquery projected-order-limit LATERAL hook")
}

func TestBuildMultiOuterProjectedOrderLimitLateralHookQuery(t *testing.T) {
	gen := newMultiOuterOrderLimitTestGenerator(t)
	query := gen.buildMultiOuterProjectedOrderLimitLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2])
	if query == nil {
		t.Fatalf("expected multi-outer projected-order-limit LATERAL hook query")
	}
	if len(query.From.Joins) != 2 {
		t.Fatalf("expected join plus LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in multi-outer projected-order-limit hook query")
	}
	if lateral.TableQuery.From.BaseQuery != nil {
		t.Fatalf("expected multi-outer projected-order-limit hook to stay direct inside the LATERAL body")
	}
	if len(lateral.TableQuery.GroupBy) != 0 || lateral.TableQuery.Having != nil {
		t.Fatalf("expected multi-outer projected-order-limit hook to avoid GROUP BY/HAVING")
	}
	if !selectItemsHaveAliases(lateral.TableQuery.Items, "score0", "tie0") {
		t.Fatalf("expected multi-outer projected-order-limit hook to expose score0/tie0 aliases")
	}
	scoreExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "score0")
	if !ok {
		t.Fatalf("expected multi-outer projected-order-limit hook to project score0")
	}
	tieExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "tie0")
	if !ok {
		t.Fatalf("expected multi-outer projected-order-limit hook to project tie0")
	}
	visible := map[string]struct{}{
		query.From.BaseTable:            {},
		query.From.Joins[0].tableName(): {},
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) < 2 {
		t.Fatalf("expected multi-outer projected-order-limit WHERE to reference both left-side tables")
	}
	if countVisibleTables(scoreExpr, visible) < 2 {
		t.Fatalf("expected multi-outer projected-order-limit score0 to reference both left-side tables")
	}
	if countVisibleTables(tieExpr, visible) < 2 {
		t.Fatalf("expected multi-outer projected-order-limit tie0 to reference both left-side tables")
	}
	if !exprContainsCase(scoreExpr) || !exprContainsFuncName(scoreExpr, "ABS") || caseExprDepth(scoreExpr) < 2 {
		t.Fatalf("expected multi-outer projected-order-limit score0 to use ABS-wrapped nested CASE correlation")
	}
	if !exprContainsCase(tieExpr) || !exprContainsFuncName(tieExpr, "ABS") {
		t.Fatalf("expected multi-outer projected-order-limit tie0 to use wrapped CASE correlation")
	}
	if lateral.TableQuery.Limit == nil || *lateral.TableQuery.Limit != 1 {
		t.Fatalf("expected multi-outer projected-order-limit hook to keep LIMIT 1 inside LATERAL")
	}
	if len(lateral.TableQuery.OrderBy) < 3 {
		t.Fatalf("expected multi-outer projected-order-limit hook to keep ORDER BY inside LATERAL")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, "score0") {
		t.Fatalf("expected multi-outer projected-order-limit ORDER BY to rank by score0 alias")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[1].Expr, "tie0") || !lateral.TableQuery.OrderBy[1].Desc {
		t.Fatalf("expected multi-outer projected-order-limit ORDER BY to rank by descending tie0 alias")
	}
	if countVisibleTables(lateral.TableQuery.OrderBy[2].Expr, visible) == 0 {
		t.Fatalf("expected multi-outer projected-order-limit ORDER BY tie breaker to keep left-side visibility")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected multi-outer projected-order-limit LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestGenerateSelectQueryExercisesMultiOuterProjectedOrderLimitLateralHook(t *testing.T) {
	gen := newMultiOuterOrderLimitTestGenerator(t)
	v := validator.New()
	for i := 0; i < 400; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasMultiOuterProjectedOrderLimitLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated multi-outer projected-order-limit lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise multi-outer projected-order-limit LATERAL hook")
}

func TestBuildProjectedOrderLimitLateralHookQueryUsing(t *testing.T) {
	gen := newProjectedOrderLimitTestGenerator(t)
	gen.Config.Features.NaturalJoins = false
	query := gen.buildProjectedOrderLimitLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], false)
	if query == nil {
		t.Fatalf("expected projected-order-limit LATERAL hook query for USING")
	}
	if len(query.From.Joins) != 2 || len(query.From.Joins[0].Using) == 0 {
		t.Fatalf("expected USING join before projected-order-limit LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in projected-order-limit hook query")
	}
	if lateral.TableQuery.From.BaseQuery != nil {
		t.Fatalf("expected projected-order-limit hook to stay direct inside the LATERAL body")
	}
	if len(lateral.TableQuery.GroupBy) != 0 {
		t.Fatalf("expected projected-order-limit hook to avoid GROUP BY")
	}
	if lateral.TableQuery.Having != nil {
		t.Fatalf("expected projected-order-limit hook to avoid HAVING")
	}
	if !selectItemsHaveAliases(lateral.TableQuery.Items, "score0", "tie0") {
		t.Fatalf("expected projected-order-limit hook to expose score0/tie0 aliases")
	}
	scoreExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "score0")
	if !ok {
		t.Fatalf("expected projected-order-limit hook to project score0")
	}
	if !exprContainsCase(scoreExpr) || !exprContainsFuncName(scoreExpr, "ABS") || caseExprDepth(scoreExpr) < 2 {
		t.Fatalf("expected projected-order-limit score0 to use ABS-wrapped nested CASE correlation")
	}
	tieExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "tie0")
	if !ok {
		t.Fatalf("expected projected-order-limit hook to project tie0")
	}
	if !exprContainsCase(tieExpr) || !exprContainsFuncName(tieExpr, "ABS") {
		t.Fatalf("expected projected-order-limit tie0 to keep an outer-sensitive wrapped CASE signal")
	}
	if lateral.TableQuery.Limit == nil || *lateral.TableQuery.Limit != 1 {
		t.Fatalf("expected projected-order-limit hook to keep LIMIT 1 inside LATERAL")
	}
	usingCol := query.From.Joins[0].Using[0]
	if !exprUsesUnqualifiedColumnName(scoreExpr, usingCol) {
		t.Fatalf("expected projected-order-limit score0 to reference merged USING column %q", usingCol)
	}
	if !exprUsesUnqualifiedColumnName(tieExpr, usingCol) {
		t.Fatalf("expected projected-order-limit tie0 to reference merged USING column %q", usingCol)
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.Where, usingCol) {
		t.Fatalf("expected projected-order-limit WHERE to reference merged USING column %q", usingCol)
	}
	if len(lateral.TableQuery.OrderBy) < 3 {
		t.Fatalf("expected ORDER BY with projected score plus tie breakers")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, "score0") {
		t.Fatalf("expected projected-order-limit ORDER BY to rank by score0 alias")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[1].Expr, "tie0") || !lateral.TableQuery.OrderBy[1].Desc {
		t.Fatalf("expected projected-order-limit ORDER BY to use descending tie0 secondary key")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[2].Expr, usingCol) {
		t.Fatalf("expected projected-order-limit ORDER BY to keep merged USING column %q visible", usingCol)
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected projected-order-limit USING LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildProjectedOrderLimitLateralHookQueryNatural(t *testing.T) {
	gen := newProjectedOrderLimitTestGenerator(t)
	query := gen.buildProjectedOrderLimitLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], true)
	if query == nil {
		t.Fatalf("expected projected-order-limit LATERAL hook query for NATURAL join")
	}
	if len(query.From.Joins) != 2 || !query.From.Joins[0].Natural {
		t.Fatalf("expected NATURAL join before projected-order-limit LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in projected-order-limit NATURAL hook query")
	}
	if lateral.TableQuery.From.BaseQuery != nil {
		t.Fatalf("expected projected-order-limit NATURAL hook to stay direct inside the LATERAL body")
	}
	if len(lateral.TableQuery.GroupBy) != 0 {
		t.Fatalf("expected projected-order-limit NATURAL hook to avoid GROUP BY")
	}
	if lateral.TableQuery.Having != nil {
		t.Fatalf("expected projected-order-limit NATURAL hook to avoid HAVING")
	}
	scoreExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "score0")
	if !ok {
		t.Fatalf("expected projected-order-limit NATURAL hook to project score0")
	}
	if !exprContainsCase(scoreExpr) || !exprContainsFuncName(scoreExpr, "ABS") || caseExprDepth(scoreExpr) < 2 {
		t.Fatalf("expected projected-order-limit NATURAL score0 to use ABS-wrapped nested CASE correlation")
	}
	if !exprHasUnqualifiedColumn(scoreExpr) {
		t.Fatalf("expected projected-order-limit NATURAL score0 to reference merged column unqualified")
	}
	tieExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "tie0")
	if !ok {
		t.Fatalf("expected projected-order-limit NATURAL hook to project tie0")
	}
	if !exprContainsCase(tieExpr) || !exprContainsFuncName(tieExpr, "ABS") {
		t.Fatalf("expected projected-order-limit NATURAL tie0 to keep a wrapped CASE signal")
	}
	if !exprHasUnqualifiedColumn(tieExpr) {
		t.Fatalf("expected projected-order-limit NATURAL tie0 to reference merged column unqualified")
	}
	if lateral.TableQuery.Limit == nil || *lateral.TableQuery.Limit != 1 {
		t.Fatalf("expected projected-order-limit NATURAL hook to keep LIMIT 1 inside LATERAL")
	}
	if !exprHasUnqualifiedColumn(lateral.TableQuery.Where) {
		t.Fatalf("expected projected-order-limit NATURAL hook to reference merged column unqualified in WHERE")
	}
	if len(lateral.TableQuery.OrderBy) < 3 {
		t.Fatalf("expected ORDER BY with projected score plus NATURAL tie breakers")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, "score0") {
		t.Fatalf("expected projected-order-limit NATURAL hook to rank by score0 alias")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[1].Expr, "tie0") || !lateral.TableQuery.OrderBy[1].Desc {
		t.Fatalf("expected projected-order-limit NATURAL hook to use descending tie0 secondary key")
	}
	if !exprHasUnqualifiedColumn(lateral.TableQuery.OrderBy[2].Expr) {
		t.Fatalf("expected projected-order-limit NATURAL hook to keep merged column unqualified in ORDER BY")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected projected-order-limit NATURAL LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestGenerateSelectQueryExercisesProjectedOrderLimitLateralHook(t *testing.T) {
	gen := newProjectedOrderLimitTestGenerator(t)
	v := validator.New()
	for i := 0; i < 400; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasProjectedOrderLimitLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated projected-order-limit lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise projected-order-limit LATERAL hook")
}

func TestBuildGroupedOutputOrderLimitLateralHookQueryUsing(t *testing.T) {
	gen := newGroupedOutputOrderLimitTestGenerator(t)
	gen.Config.Features.NaturalJoins = false
	query := gen.buildGroupedOutputOrderLimitLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], false)
	if query == nil {
		t.Fatalf("expected grouped-output-order-limit LATERAL hook query for USING")
	}
	if len(query.From.Joins) != 2 || len(query.From.Joins[0].Using) == 0 {
		t.Fatalf("expected USING join before grouped-output-order-limit LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped-output-order-limit hook query")
	}
	if lateral.TableQuery.From.BaseQuery != nil {
		t.Fatalf("expected grouped-output-order-limit hook to group directly inside the LATERAL body")
	}
	if len(lateral.TableQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped-output-order-limit LATERAL hook")
	}
	if !exprUsesTable(lateral.TableQuery.GroupBy[0], lateral.TableQuery.From.baseName()) {
		t.Fatalf("expected grouped-output-order-limit GROUP BY to depend on the inner grouped source")
	}
	if exprHasUnqualifiedColumn(lateral.TableQuery.GroupBy[0]) {
		t.Fatalf("expected grouped-output-order-limit GROUP BY to stop depending on merged USING columns directly")
	}
	if !selectItemsHaveAliases(lateral.TableQuery.Items, "g0", "cnt") {
		t.Fatalf("expected grouped-output-order-limit LATERAL hook to expose g0/cnt aliases directly")
	}
	if lateral.TableQuery.Limit == nil || *lateral.TableQuery.Limit != 1 {
		t.Fatalf("expected grouped-output-order-limit hook to keep LIMIT 1 inside LATERAL")
	}
	usingCol := query.From.Joins[0].Using[0]
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.Where, usingCol) {
		t.Fatalf("expected grouped-output-order-limit WHERE to reference merged USING column %q before aggregation", usingCol)
	}
	if lateral.TableQuery.Having == nil {
		t.Fatalf("expected grouped-output-order-limit HAVING to carry merged USING visibility")
	}
	if !exprContainsCase(lateral.TableQuery.Having) {
		t.Fatalf("expected grouped-output-order-limit HAVING to use a branchy CASE-correlated filter")
	}
	if !exprContainsAggregate(lateral.TableQuery.Having) {
		t.Fatalf("expected grouped-output-order-limit HAVING to include aggregate visibility before TopN")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.Having, usingCol) {
		t.Fatalf("expected grouped-output-order-limit HAVING to reference merged USING column %q", usingCol)
	}
	if len(lateral.TableQuery.OrderBy) == 0 {
		t.Fatalf("expected ORDER BY inside grouped-output-order-limit hook")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, "g0") {
		t.Fatalf("expected grouped-output-order-limit ORDER BY to reference grouped output alias")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[1].Expr, "cnt") {
		t.Fatalf("expected grouped-output-order-limit ORDER BY to rank by grouped count alias")
	}
	if len(lateral.TableQuery.OrderBy) < 3 || !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[2].Expr, usingCol) {
		t.Fatalf("expected grouped-output-order-limit ORDER BY to keep merged USING column %q visible as a tie breaker", usingCol)
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped-output-order-limit USING LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedOutputOrderLimitLateralHookQueryNatural(t *testing.T) {
	gen := newGroupedOutputOrderLimitTestGenerator(t)
	query := gen.buildGroupedOutputOrderLimitLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], true)
	if query == nil {
		t.Fatalf("expected grouped-output-order-limit LATERAL hook query for NATURAL join")
	}
	if len(query.From.Joins) != 2 || !query.From.Joins[0].Natural {
		t.Fatalf("expected NATURAL join before grouped-output-order-limit LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped-output-order-limit NATURAL hook query")
	}
	if lateral.TableQuery.From.BaseQuery != nil {
		t.Fatalf("expected grouped-output-order-limit NATURAL hook to group directly inside the LATERAL body")
	}
	if len(lateral.TableQuery.GroupBy) == 0 || !exprUsesTable(lateral.TableQuery.GroupBy[0], lateral.TableQuery.From.baseName()) {
		t.Fatalf("expected grouped-output-order-limit NATURAL hook to keep a direct inner grouped key")
	}
	if lateral.TableQuery.Having == nil {
		t.Fatalf("expected grouped-output-order-limit NATURAL hook to use HAVING before TopN")
	}
	if !exprContainsCase(lateral.TableQuery.Having) {
		t.Fatalf("expected grouped-output-order-limit NATURAL hook to use a branchy CASE HAVING")
	}
	if !exprContainsAggregate(lateral.TableQuery.Having) {
		t.Fatalf("expected grouped-output-order-limit NATURAL hook to include aggregate visibility in HAVING")
	}
	if !exprHasUnqualifiedColumn(lateral.TableQuery.Having) {
		t.Fatalf("expected grouped-output-order-limit NATURAL hook to reference merged column unqualified in HAVING")
	}
	if lateral.TableQuery.Limit == nil || *lateral.TableQuery.Limit != 1 {
		t.Fatalf("expected grouped-output-order-limit NATURAL hook to keep LIMIT 1 inside LATERAL")
	}
	if !exprHasUnqualifiedColumn(lateral.TableQuery.Where) {
		t.Fatalf("expected grouped-output-order-limit NATURAL hook to reference merged column unqualified before aggregation")
	}
	if len(lateral.TableQuery.OrderBy) == 0 {
		t.Fatalf("expected ORDER BY inside grouped-output-order-limit NATURAL hook")
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, "g0") {
		t.Fatalf("expected grouped-output-order-limit NATURAL hook to reference grouped output alias in ORDER BY")
	}
	if len(lateral.TableQuery.OrderBy) < 3 || !exprHasUnqualifiedColumn(lateral.TableQuery.OrderBy[2].Expr) {
		t.Fatalf("expected grouped-output-order-limit NATURAL hook to keep merged column unqualified in ORDER BY tie breaker")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped-output-order-limit NATURAL LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestGenerateSelectQueryExercisesGroupedOutputOrderLimitLateralHook(t *testing.T) {
	gen := newGroupedOutputOrderLimitTestGenerator(t)
	v := validator.New()
	for i := 0; i < 400; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasGroupedOutputOrderLimitLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated grouped-output-order-limit lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise grouped-output-order-limit LATERAL hook")
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

func TestBuildGroupedOutputAliasLateralHookQueryUsing(t *testing.T) {
	gen := newGroupedOutputAliasLateralTestGenerator(t)
	gen.Config.Features.NaturalJoins = false
	query := gen.buildGroupedOutputAliasLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], false)
	if query == nil {
		t.Fatalf("expected grouped-output-alias LATERAL hook query for USING")
	}
	if len(query.From.Joins) != 2 || len(query.From.Joins[0].Using) == 0 {
		t.Fatalf("expected USING join before grouped-output-alias LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped-output-alias hook query")
	}
	if lateral.TableQuery.From.BaseQuery == nil {
		t.Fatalf("expected grouped derived table inside grouped-output-alias LATERAL hook")
	}
	agg := lateral.TableQuery.From.BaseQuery
	if len(agg.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped derived table")
	}
	if !selectItemsHaveAliases(agg.Items, "g0", "cnt") {
		t.Fatalf("expected grouped derived table to expose g0/cnt aliases")
	}
	usingCol := query.From.Joins[0].Using[0]
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.Where, usingCol) {
		t.Fatalf("expected grouped-output-alias LATERAL predicate to reference merged USING column %q", usingCol)
	}
	if !exprUsesQualifiedColumnName(lateral.TableQuery.Where, lateral.TableQuery.From.baseName(), "g0") {
		t.Fatalf("expected grouped-output-alias LATERAL predicate to reference grouped output alias")
	}
	if len(lateral.TableQuery.OrderBy) == 0 || !exprUsesQualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, lateral.TableQuery.From.baseName(), "g0") {
		t.Fatalf("expected grouped-output-alias LATERAL hook to order by grouped output alias")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped-output-alias USING LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestBuildGroupedOutputAliasLateralHookQueryNatural(t *testing.T) {
	gen := newGroupedOutputAliasLateralTestGenerator(t)
	query := gen.buildGroupedOutputAliasLateralHookQueryForTables(gen.State.Tables[0], gen.State.Tables[1], gen.State.Tables[2], true)
	if query == nil {
		t.Fatalf("expected grouped-output-alias LATERAL hook query for NATURAL join")
	}
	if len(query.From.Joins) != 2 || !query.From.Joins[0].Natural {
		t.Fatalf("expected NATURAL join before grouped-output-alias LATERAL hook")
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil {
		t.Fatalf("expected LATERAL derived table in grouped-output-alias NATURAL hook query")
	}
	if lateral.TableQuery.From.BaseQuery == nil {
		t.Fatalf("expected grouped derived table inside grouped-output-alias NATURAL hook")
	}
	if len(lateral.TableQuery.From.BaseQuery.GroupBy) == 0 {
		t.Fatalf("expected GROUP BY inside grouped derived table for NATURAL hook")
	}
	if !exprHasUnqualifiedColumn(lateral.TableQuery.Where) {
		t.Fatalf("expected grouped-output-alias NATURAL hook to reference merged column unqualified")
	}
	if !exprUsesQualifiedColumnName(lateral.TableQuery.Where, lateral.TableQuery.From.baseName(), "g0") {
		t.Fatalf("expected grouped-output-alias NATURAL hook to reference grouped output alias")
	}
	if err := validator.New().Validate(query.SQLString()); err != nil {
		t.Fatalf("expected grouped-output-alias NATURAL LATERAL SQL to parse: %v\nsql=%s", err, query.SQLString())
	}
}

func TestGenerateSelectQueryExercisesGroupedOutputAliasLateralHook(t *testing.T) {
	gen := newGroupedOutputAliasLateralTestGenerator(t)
	v := validator.New()
	for i := 0; i < 400; i++ {
		query := gen.GenerateSelectQuery()
		if !queryHasGroupedOutputAliasLateralHook(query) {
			continue
		}
		if err := v.Validate(query.SQLString()); err != nil {
			t.Fatalf("expected generated grouped-output-alias lateral hook SQL to parse: %v\nsql=%s", err, query.SQLString())
		}
		return
	}

	t.Fatalf("expected generator to exercise grouped-output-alias LATERAL hook")
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

func newGroupedOutputAliasLateralTestGenerator(t *testing.T) *Generator {
	t.Helper()
	gen := newMergedVisibilityTestGenerator(t)
	gen.Config.Features.Aggregates = true
	gen.Config.Features.GroupBy = true
	return gen
}

func newGroupedOutputOrderLimitTestGenerator(t *testing.T) *Generator {
	t.Helper()
	gen := newGroupedOutputAliasLateralTestGenerator(t)
	gen.Config.Features.Limit = true
	gen.Config.Features.OrderBy = true
	return gen
}

func newProjectedOrderLimitTestGenerator(t *testing.T) *Generator {
	t.Helper()
	gen := newMergedVisibilityTestGenerator(t)
	gen.Config.Features.Limit = true
	gen.Config.Features.OrderBy = true
	return gen
}

func newMultiOuterOrderLimitTestGenerator(t *testing.T) *Generator {
	t.Helper()
	gen := newGroupedAggregateLateralTestGenerator(t)
	gen.Config.Features.Aggregates = false
	gen.Config.Features.GroupBy = false
	gen.Config.Features.Having = false
	gen.Config.Features.Limit = true
	gen.Config.Features.OrderBy = true
	return gen
}

func newScalarSubqueryOrderLimitTestGenerator(t *testing.T) *Generator {
	t.Helper()
	gen := newMultiOuterOrderLimitTestGenerator(t)
	gen.Config.Features.Subqueries = true
	gen.SetDisallowScalarSubquery(false)
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

func queryHasGroupedOutputAliasLateralHook(query *SelectQuery) bool {
	if query == nil || len(query.From.Joins) < 2 {
		return false
	}
	mergedJoin := query.From.Joins[0]
	if !mergedJoin.Natural && len(mergedJoin.Using) == 0 {
		return false
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil || lateral.TableQuery.From.BaseQuery == nil {
		return false
	}
	agg := lateral.TableQuery.From.BaseQuery
	if len(agg.GroupBy) == 0 || !selectItemsHaveAliases(agg.Items, "g0", "cnt") {
		return false
	}
	if !exprUsesTable(lateral.TableQuery.Where, lateral.TableQuery.From.baseName()) {
		return false
	}
	if !exprHasUnqualifiedColumn(lateral.TableQuery.Where) {
		return false
	}
	if len(lateral.TableQuery.OrderBy) == 0 || !exprUsesTable(lateral.TableQuery.OrderBy[0].Expr, lateral.TableQuery.From.baseName()) {
		return false
	}
	return true
}

func queryHasScalarSubqueryProjectedOrderLimitLateralHook(query *SelectQuery) bool {
	if query == nil || len(query.From.Joins) < 2 {
		return false
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil || lateral.TableQuery.From.BaseQuery != nil {
		return false
	}
	if len(lateral.TableQuery.GroupBy) != 0 || lateral.TableQuery.Having != nil || lateral.TableQuery.Where == nil || lateral.TableQuery.Limit == nil {
		return false
	}
	if !selectItemsHaveAliases(lateral.TableQuery.Items, "score0", "tie0") {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	if name := query.From.Joins[0].tableName(); name != "" {
		visible[name] = struct{}{}
	}
	scoreExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "score0")
	if !ok || !exprContainsFuncName(scoreExpr, "ABS") || !exprContainsScalarSubquery(scoreExpr) {
		return false
	}
	if !exprHasOrderedLimitedScalarSubquery(scoreExpr) || !exprHasScalarSubqueryUsingVisibleTables(scoreExpr, visible, 2) {
		return false
	}
	tieExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "tie0")
	if !ok || !exprContainsScalarSubquery(tieExpr) {
		return false
	}
	if !exprHasOrderedLimitedScalarSubquery(tieExpr) || !exprHasScalarSubqueryUsingVisibleTables(tieExpr, visible, 2) {
		return false
	}
	scoreSubs := scalarSubquerySignatures(scoreExpr)
	tieSubs := scalarSubquerySignatures(tieExpr)
	if len(scoreSubs) == 0 || len(tieSubs) != 1 || scoreSubs[0] != tieSubs[0] {
		return false
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) == 0 || len(lateral.TableQuery.OrderBy) < 3 {
		return false
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, "score0") {
		return false
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[1].Expr, "tie0") || !lateral.TableQuery.OrderBy[1].Desc {
		return false
	}
	if countVisibleTables(lateral.TableQuery.OrderBy[2].Expr, visible) == 0 {
		return false
	}
	outerVisible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		outerVisible[base] = struct{}{}
	}
	if name := query.From.Joins[0].tableName(); name != "" {
		outerVisible[name] = struct{}{}
	}
	if name := query.From.Joins[1].tableName(); name != "" {
		outerVisible[name] = struct{}{}
	}
	if query.Where == nil || !exprUsesQualifiedColumnName(query.Where, "dt", "tie0") || countVisibleTables(query.Where, outerVisible) < 2 {
		return false
	}
	if len(query.OrderBy) < 3 {
		return false
	}
	if !exprContainsFuncName(query.OrderBy[0].Expr, "ABS") || !exprUsesQualifiedColumnName(query.OrderBy[0].Expr, "dt", "tie0") || countVisibleTables(query.OrderBy[0].Expr, outerVisible) < 2 {
		return false
	}
	if !exprUsesQualifiedColumnName(query.OrderBy[1].Expr, "dt", "score0") {
		return false
	}
	return countVisibleTables(query.OrderBy[2].Expr, outerVisible) > 0
}

func queryHasMultiOuterProjectedOrderLimitLateralHook(query *SelectQuery) bool {
	if query == nil || len(query.From.Joins) < 2 {
		return false
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil || lateral.TableQuery.From.BaseQuery != nil {
		return false
	}
	if len(lateral.TableQuery.GroupBy) != 0 || lateral.TableQuery.Having != nil || lateral.TableQuery.Where == nil || lateral.TableQuery.Limit == nil {
		return false
	}
	if !selectItemsHaveAliases(lateral.TableQuery.Items, "score0", "tie0") {
		return false
	}
	visible := map[string]struct{}{}
	if base := query.From.baseName(); base != "" {
		visible[base] = struct{}{}
	}
	if name := query.From.Joins[0].tableName(); name != "" {
		visible[name] = struct{}{}
	}
	scoreExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "score0")
	if !ok || !exprContainsCase(scoreExpr) || !exprContainsFuncName(scoreExpr, "ABS") || countVisibleTables(scoreExpr, visible) < 2 {
		return false
	}
	tieExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "tie0")
	if !ok || !exprContainsCase(tieExpr) || !exprContainsFuncName(tieExpr, "ABS") || countVisibleTables(tieExpr, visible) < 2 {
		return false
	}
	if countVisibleTables(lateral.TableQuery.Where, visible) < 2 {
		return false
	}
	if len(lateral.TableQuery.OrderBy) < 3 {
		return false
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, "score0") {
		return false
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[1].Expr, "tie0") || !lateral.TableQuery.OrderBy[1].Desc {
		return false
	}
	return countVisibleTables(lateral.TableQuery.OrderBy[2].Expr, visible) > 0
}

func queryHasProjectedOrderLimitLateralHook(query *SelectQuery) bool {
	if query == nil || len(query.From.Joins) < 2 {
		return false
	}
	mergedJoin := query.From.Joins[0]
	if !mergedJoin.Natural && len(mergedJoin.Using) == 0 {
		return false
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil || lateral.TableQuery.From.BaseQuery != nil {
		return false
	}
	if len(lateral.TableQuery.GroupBy) != 0 || lateral.TableQuery.Having != nil || lateral.TableQuery.Where == nil || lateral.TableQuery.Limit == nil {
		return false
	}
	if !selectItemsHaveAliases(lateral.TableQuery.Items, "score0", "tie0") {
		return false
	}
	scoreExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "score0")
	if !ok || !exprContainsCase(scoreExpr) || !exprContainsFuncName(scoreExpr, "ABS") {
		return false
	}
	tieExpr, ok := selectItemExprByAlias(lateral.TableQuery.Items, "tie0")
	if !ok || !exprContainsCase(tieExpr) || !exprContainsFuncName(tieExpr, "ABS") {
		return false
	}
	if len(lateral.TableQuery.OrderBy) < 3 {
		return false
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, "score0") {
		return false
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[1].Expr, "tie0") || !lateral.TableQuery.OrderBy[1].Desc {
		return false
	}
	if mergedJoin.Natural {
		return exprUsesUnqualifiedColumnOtherThan(lateral.TableQuery.Where) &&
			exprUsesUnqualifiedColumnOtherThan(scoreExpr, "score0", "tie0") &&
			exprUsesUnqualifiedColumnOtherThan(tieExpr, "score0", "tie0") &&
			exprHasUnqualifiedColumn(lateral.TableQuery.OrderBy[2].Expr)
	}
	for _, name := range mergedJoin.Using {
		if exprUsesUnqualifiedColumnName(lateral.TableQuery.Where, name) &&
			exprUsesUnqualifiedColumnName(scoreExpr, name) &&
			exprUsesUnqualifiedColumnName(tieExpr, name) &&
			exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[2].Expr, name) {
			return true
		}
	}
	return false
}

func queryHasGroupedOutputOrderLimitLateralHook(query *SelectQuery) bool {
	if query == nil || len(query.From.Joins) < 2 {
		return false
	}
	mergedJoin := query.From.Joins[0]
	if !mergedJoin.Natural && len(mergedJoin.Using) == 0 {
		return false
	}
	lateral := query.From.Joins[1]
	if !lateral.Lateral || lateral.TableQuery == nil || lateral.TableQuery.From.BaseQuery != nil {
		return false
	}
	if len(lateral.TableQuery.GroupBy) == 0 || !selectItemsHaveAliases(lateral.TableQuery.Items, "g0", "cnt") {
		return false
	}
	if exprHasUnqualifiedColumn(lateral.TableQuery.GroupBy[0]) {
		return false
	}
	if !exprUsesTable(lateral.TableQuery.GroupBy[0], lateral.TableQuery.From.baseName()) {
		return false
	}
	if lateral.TableQuery.Limit == nil || lateral.TableQuery.Where == nil || lateral.TableQuery.Having == nil {
		return false
	}
	if !exprContainsCase(lateral.TableQuery.Having) || !exprContainsAggregate(lateral.TableQuery.Having) {
		return false
	}
	if len(lateral.TableQuery.OrderBy) == 0 {
		return false
	}
	if !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[0].Expr, "g0") {
		return false
	}
	if len(lateral.TableQuery.OrderBy) > 1 && !exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[1].Expr, "cnt") {
		return false
	}
	if mergedJoin.Natural {
		return exprUsesUnqualifiedColumnOtherThan(lateral.TableQuery.Where) &&
			exprUsesUnqualifiedColumnOtherThan(lateral.TableQuery.Having) &&
			len(lateral.TableQuery.OrderBy) > 2 &&
			exprHasUnqualifiedColumn(lateral.TableQuery.OrderBy[2].Expr)
	}
	for _, name := range mergedJoin.Using {
		if exprUsesUnqualifiedColumnName(lateral.TableQuery.Where, name) &&
			exprUsesUnqualifiedColumnName(lateral.TableQuery.Having, name) &&
			len(lateral.TableQuery.OrderBy) > 2 &&
			exprUsesUnqualifiedColumnName(lateral.TableQuery.OrderBy[2].Expr, name) {
			return true
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

func countVisibleTablesRecursive(query *SelectQuery, visible map[string]struct{}) int {
	if query == nil {
		return 0
	}
	seen := make(map[string]struct{}, len(visible))
	collectVisibleTablesInQuery(query, visible, seen)
	return len(seen)
}

func exprUsesQualifiedColumnName(expr Expr, table string, name string) bool {
	if expr == nil || table == "" || name == "" {
		return false
	}
	for _, col := range expr.Columns() {
		if col.Table == table && col.Name == name {
			return true
		}
	}
	return false
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

func exprUsesUnqualifiedColumnOtherThan(expr Expr, excluded ...string) bool {
	if expr == nil {
		return false
	}
	skip := make(map[string]struct{}, len(excluded))
	for _, name := range excluded {
		skip[name] = struct{}{}
	}
	for _, col := range expr.Columns() {
		if col.Table != "" {
			continue
		}
		if _, ok := skip[col.Name]; ok {
			continue
		}
		return true
	}
	return false
}

func selectItemsHaveAliases(items []SelectItem, names ...string) bool {
	if len(names) == 0 {
		return true
	}
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item.Alias == "" {
			continue
		}
		seen[item.Alias] = struct{}{}
	}
	for _, name := range names {
		if _, ok := seen[name]; !ok {
			return false
		}
	}
	return true
}

func selectItemExprByAlias(items []SelectItem, name string) (Expr, bool) {
	for _, item := range items {
		if item.Alias == name {
			return item.Expr, true
		}
	}
	return nil, false
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

func exprContainsScalarSubquery(expr Expr) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case SubqueryExpr:
		return true
	case UnaryExpr:
		return exprContainsScalarSubquery(e.Expr)
	case BinaryExpr:
		return exprContainsScalarSubquery(e.Left) || exprContainsScalarSubquery(e.Right)
	case FuncExpr:
		for _, arg := range e.Args {
			if exprContainsScalarSubquery(arg) {
				return true
			}
		}
		return false
	case GroupByOrdinalExpr:
		return exprContainsScalarSubquery(e.Expr)
	case CaseExpr:
		for _, when := range e.Whens {
			if exprContainsScalarSubquery(when.When) || exprContainsScalarSubquery(when.Then) {
				return true
			}
		}
		return exprContainsScalarSubquery(e.Else)
	case InExpr:
		if exprContainsScalarSubquery(e.Left) {
			return true
		}
		for _, item := range e.List {
			if exprContainsScalarSubquery(item) {
				return true
			}
		}
		return false
	case WindowExpr:
		for _, arg := range e.Args {
			if exprContainsScalarSubquery(arg) {
				return true
			}
		}
		for _, expr := range e.PartitionBy {
			if exprContainsScalarSubquery(expr) {
				return true
			}
		}
		for _, ob := range e.OrderBy {
			if exprContainsScalarSubquery(ob.Expr) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func exprHasOrderedLimitedScalarSubquery(expr Expr) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case SubqueryExpr:
		return e.Query != nil && len(e.Query.OrderBy) > 0 && e.Query.Limit != nil
	case UnaryExpr:
		return exprHasOrderedLimitedScalarSubquery(e.Expr)
	case BinaryExpr:
		return exprHasOrderedLimitedScalarSubquery(e.Left) || exprHasOrderedLimitedScalarSubquery(e.Right)
	case FuncExpr:
		for _, arg := range e.Args {
			if exprHasOrderedLimitedScalarSubquery(arg) {
				return true
			}
		}
		return false
	case GroupByOrdinalExpr:
		return exprHasOrderedLimitedScalarSubquery(e.Expr)
	case CaseExpr:
		for _, when := range e.Whens {
			if exprHasOrderedLimitedScalarSubquery(when.When) || exprHasOrderedLimitedScalarSubquery(when.Then) {
				return true
			}
		}
		return exprHasOrderedLimitedScalarSubquery(e.Else)
	case InExpr:
		if exprHasOrderedLimitedScalarSubquery(e.Left) {
			return true
		}
		for _, item := range e.List {
			if exprHasOrderedLimitedScalarSubquery(item) {
				return true
			}
		}
		return false
	case WindowExpr:
		for _, arg := range e.Args {
			if exprHasOrderedLimitedScalarSubquery(arg) {
				return true
			}
		}
		for _, expr := range e.PartitionBy {
			if exprHasOrderedLimitedScalarSubquery(expr) {
				return true
			}
		}
		for _, ob := range e.OrderBy {
			if exprHasOrderedLimitedScalarSubquery(ob.Expr) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func exprHasScalarSubqueryUsingVisibleTables(expr Expr, visible map[string]struct{}, minCount int) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case SubqueryExpr:
		return countVisibleTablesRecursive(e.Query, visible) >= minCount
	case UnaryExpr:
		return exprHasScalarSubqueryUsingVisibleTables(e.Expr, visible, minCount)
	case BinaryExpr:
		return exprHasScalarSubqueryUsingVisibleTables(e.Left, visible, minCount) ||
			exprHasScalarSubqueryUsingVisibleTables(e.Right, visible, minCount)
	case FuncExpr:
		for _, arg := range e.Args {
			if exprHasScalarSubqueryUsingVisibleTables(arg, visible, minCount) {
				return true
			}
		}
		return false
	case GroupByOrdinalExpr:
		return exprHasScalarSubqueryUsingVisibleTables(e.Expr, visible, minCount)
	case CaseExpr:
		for _, when := range e.Whens {
			if exprHasScalarSubqueryUsingVisibleTables(when.When, visible, minCount) ||
				exprHasScalarSubqueryUsingVisibleTables(when.Then, visible, minCount) {
				return true
			}
		}
		return exprHasScalarSubqueryUsingVisibleTables(e.Else, visible, minCount)
	case InExpr:
		if exprHasScalarSubqueryUsingVisibleTables(e.Left, visible, minCount) {
			return true
		}
		for _, item := range e.List {
			if exprHasScalarSubqueryUsingVisibleTables(item, visible, minCount) {
				return true
			}
		}
		return false
	case WindowExpr:
		for _, arg := range e.Args {
			if exprHasScalarSubqueryUsingVisibleTables(arg, visible, minCount) {
				return true
			}
		}
		for _, expr := range e.PartitionBy {
			if exprHasScalarSubqueryUsingVisibleTables(expr, visible, minCount) {
				return true
			}
		}
		for _, ob := range e.OrderBy {
			if exprHasScalarSubqueryUsingVisibleTables(ob.Expr, visible, minCount) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func scalarSubquerySignatures(expr Expr) []string {
	out := make([]string, 0, 2)
	collectScalarSubquerySignatures(expr, &out)
	return out
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

func collectVisibleTablesInQuery(query *SelectQuery, visible map[string]struct{}, seen map[string]struct{}) {
	if query == nil {
		return
	}
	for _, cte := range query.With {
		collectVisibleTablesInQuery(cte.Query, visible, seen)
	}
	for _, op := range query.SetOps {
		collectVisibleTablesInQuery(op.Query, visible, seen)
	}
	collectVisibleTablesInQuery(query.From.BaseQuery, visible, seen)
	for _, item := range query.Items {
		collectVisibleTablesInExpr(item.Expr, visible, seen)
	}
	collectVisibleTablesInExpr(query.Where, visible, seen)
	for _, expr := range query.GroupBy {
		collectVisibleTablesInExpr(expr, visible, seen)
	}
	collectVisibleTablesInExpr(query.Having, visible, seen)
	for _, def := range query.WindowDefs {
		for _, expr := range def.PartitionBy {
			collectVisibleTablesInExpr(expr, visible, seen)
		}
		for _, ob := range def.OrderBy {
			collectVisibleTablesInExpr(ob.Expr, visible, seen)
		}
	}
	for _, ob := range query.OrderBy {
		collectVisibleTablesInExpr(ob.Expr, visible, seen)
	}
	for _, join := range query.From.Joins {
		collectVisibleTablesInExpr(join.On, visible, seen)
		collectVisibleTablesInQuery(join.TableQuery, visible, seen)
	}
}

func collectVisibleTablesInExpr(expr Expr, visible map[string]struct{}, seen map[string]struct{}) {
	switch e := expr.(type) {
	case nil:
		return
	case ColumnExpr:
		if _, ok := visible[e.Ref.Table]; ok {
			seen[e.Ref.Table] = struct{}{}
		}
	case UnaryExpr:
		collectVisibleTablesInExpr(e.Expr, visible, seen)
	case BinaryExpr:
		collectVisibleTablesInExpr(e.Left, visible, seen)
		collectVisibleTablesInExpr(e.Right, visible, seen)
	case FuncExpr:
		for _, arg := range e.Args {
			collectVisibleTablesInExpr(arg, visible, seen)
		}
	case GroupByOrdinalExpr:
		collectVisibleTablesInExpr(e.Expr, visible, seen)
	case CaseExpr:
		for _, when := range e.Whens {
			collectVisibleTablesInExpr(when.When, visible, seen)
			collectVisibleTablesInExpr(when.Then, visible, seen)
		}
		collectVisibleTablesInExpr(e.Else, visible, seen)
	case InExpr:
		collectVisibleTablesInExpr(e.Left, visible, seen)
		for _, item := range e.List {
			collectVisibleTablesInExpr(item, visible, seen)
		}
	case CompareSubqueryExpr:
		collectVisibleTablesInExpr(e.Left, visible, seen)
		collectVisibleTablesInQuery(e.Query, visible, seen)
	case SubqueryExpr:
		collectVisibleTablesInQuery(e.Query, visible, seen)
	case ExistsExpr:
		collectVisibleTablesInQuery(e.Query, visible, seen)
	case WindowExpr:
		for _, arg := range e.Args {
			collectVisibleTablesInExpr(arg, visible, seen)
		}
		for _, expr := range e.PartitionBy {
			collectVisibleTablesInExpr(expr, visible, seen)
		}
		for _, ob := range e.OrderBy {
			collectVisibleTablesInExpr(ob.Expr, visible, seen)
		}
	}
}

func collectScalarSubquerySignatures(expr Expr, out *[]string) {
	switch e := expr.(type) {
	case nil:
		return
	case UnaryExpr:
		collectScalarSubquerySignatures(e.Expr, out)
	case BinaryExpr:
		collectScalarSubquerySignatures(e.Left, out)
		collectScalarSubquerySignatures(e.Right, out)
	case FuncExpr:
		for _, arg := range e.Args {
			collectScalarSubquerySignatures(arg, out)
		}
	case GroupByOrdinalExpr:
		collectScalarSubquerySignatures(e.Expr, out)
	case CaseExpr:
		for _, when := range e.Whens {
			collectScalarSubquerySignatures(when.When, out)
			collectScalarSubquerySignatures(when.Then, out)
		}
		collectScalarSubquerySignatures(e.Else, out)
	case InExpr:
		collectScalarSubquerySignatures(e.Left, out)
		for _, item := range e.List {
			collectScalarSubquerySignatures(item, out)
		}
	case CompareSubqueryExpr:
		collectScalarSubquerySignatures(e.Left, out)
	case WindowExpr:
		for _, arg := range e.Args {
			collectScalarSubquerySignatures(arg, out)
		}
		for _, expr := range e.PartitionBy {
			collectScalarSubquerySignatures(expr, out)
		}
		for _, ob := range e.OrderBy {
			collectScalarSubquerySignatures(ob.Expr, out)
		}
	case SubqueryExpr:
		if e.Query != nil {
			*out = append(*out, e.Query.SQLString())
		}
	}
}
