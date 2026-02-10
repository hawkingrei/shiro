package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
	"shiro/internal/util"
)

// PQS implements a basic Pivoted Query Synthesis oracle (single-table and simple joins).
type PQS struct{}

// Name returns the oracle identifier.
func (o PQS) Name() string { return "PQS" }

const (
	pqsPivotPickTries   = 6
	pqsPredicateMaxCols = 3
	pqsJoinPickTries    = 4
	pqsPivotIDColumn    = "id"
	pqsJoinOnProb       = 55
	pqsJoinRectifyProb  = 60
	pqsSubqueryProb     = 35
	pqsDerivedTableProb = 35
)

type pqsPivotValue struct {
	Column schema.Column
	Raw    string
	Null   bool
}

type pqsPivotRow struct {
	Tables []schema.Table
	Values map[string]map[string]pqsPivotValue
}

type pqsAliasColumn struct {
	Table  string
	Column schema.Column
	Alias  string
}

type pqsSelectColumn struct {
	TableName string
	Column    schema.Column
	SQL       string
}

type pqsSubqueryMeta struct {
	Kind      string
	Reason    string
	SQL       string
	Predicate string
	Table     string
	Column    string
}

// Run selects a pivot row, builds a predicate guaranteed true for that row,
// and checks whether the row is contained in the query result.
func (o PQS) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	if state == nil || !state.HasBaseTables() {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "pqs:no_tables"}}
	}
	pivot, details, err := pickPQSPivotRow(ctx, exec, gen, state)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), Err: err, Details: details}
	}
	if pivot == nil {
		if details == nil {
			details = map[string]any{"skip_reason": "pqs:no_rows"}
		}
		return Result{OK: true, Oracle: o.Name(), Details: details}
	}
	query, aliases := buildPQSQuery(pivot)
	derivedTables := pqsApplyDerivedTables(gen, pivot, query)
	joinOn := len(pivot.Tables) > 1 && gen != nil && gen.Config.Features.Joins && util.Chance(gen.Rand, pqsJoinOnProb)
	var joinMeta *pqsJoinPredicateMeta
	if joinOn {
		meta, ok := pqsApplyJoinOn(gen, pivot, query)
		if ok {
			joinMeta = &meta
		} else {
			joinOn = false
		}
	}
	basePredicate, predMeta := pqsBuildPredicate(gen, pivot)
	subqueryPredicate, subqueryMeta := pqsMaybeBuildSubqueryPredicate(gen, pivot)
	predicate := pqsCombinePredicates(basePredicate, subqueryPredicate)
	hasBasePredicate := basePredicate != nil
	updateBandit := func(ok bool, err error, skipped bool) {
		if !predMeta.BanditEnabled {
			return
		}
		if !hasBasePredicate {
			return
		}
		reward := pqsBanditReward(ok, err, skipped)
		pqsUpdatePredicateArm(gen, pqsPredicateArm(predMeta.PredicateArm), reward)
	}
	attachBandit := func(details map[string]any) map[string]any {
		if details == nil {
			details = map[string]any{}
		}
		details["pqs_bandit_enabled"] = predMeta.BanditEnabled
		details["pqs_bandit_arm"] = predMeta.PredicateArm
		details["pqs_bandit_arm_name"] = predMeta.PredicateArmID
		return details
	}
	if predicate == nil {
		reason := "pqs:predicate_empty"
		if predMeta.Reason != "" {
			reason = "pqs:" + predMeta.Reason
		}
		updateBandit(true, nil, true)
		details := pqsAttachMeta(map[string]any{"skip_reason": reason}, predMeta, subqueryMeta, joinMeta, joinOn, derivedTables)
		return Result{OK: true, Oracle: o.Name(), Details: attachBandit(details)}
	}
	query.Where = predicate

	querySQL := query.SQLString()
	matchExpr := pqsMatchExpr(pivot, aliases)
	matchSQL := buildExpr(matchExpr)
	containSQL := fmt.Sprintf("SELECT 1 FROM (%s) pqs WHERE %s LIMIT 1", querySQL, matchSQL)
	row := exec.QueryRowContext(ctx, containSQL)
	var marker int
	err = row.Scan(&marker)
	present := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		reason, code := sqlErrorReason("pqs", err)
		details := pqsAttachMeta(map[string]any{
			"error_reason":            reason,
			"pqs_predicate":           predMeta.Rectified,
			"pqs_predicate_original":  predMeta.Original,
			"pqs_predicate_rectified": predMeta.Rectified,
			"pqs_rectify_reason":      predMeta.Reason,
			"pqs_rectify_fallback":    predMeta.Fallback,
		}, predMeta, subqueryMeta, joinMeta, joinOn, derivedTables)
		if code != 0 {
			details["error_code"] = int(code)
		}
		updateBandit(true, err, false)
		return Result{OK: true, Oracle: o.Name(), SQL: []string{querySQL, containSQL}, Err: err, Details: attachBandit(details)}
	}
	if !present {
		replayExpected := fmt.Sprintf("SELECT 1 FROM (%s) pqs WHERE %s LIMIT 1", querySQL, matchSQL)
		tableNames := pqsPivotTableNames(pivot)
		updateBandit(false, nil, false)
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{querySQL, containSQL},
			Expected: "pivot_row_present",
			Actual:   "pivot_row_missing",
			Details: attachBandit(pqsAttachMeta(map[string]any{
				"pqs_table":               pqsSingleTableName(tableNames),
				"pqs_tables":              tableNames,
				"pqs_predicate":           predMeta.Rectified,
				"pqs_predicate_original":  predMeta.Original,
				"pqs_predicate_rectified": predMeta.Rectified,
				"pqs_rectify_reason":      predMeta.Reason,
				"pqs_rectify_fallback":    predMeta.Fallback,
				"pqs_match":               matchSQL,
				"pivot_values":            pqsPivotValueMap(pivot),
				"rectified_predicates":    []string{predMeta.Rectified},
				"containment_query":       containSQL,
				"replay_kind":             "exists",
				"replay_expected_sql":     replayExpected,
				"replay_actual_sql":       "SELECT 1",
				"replay_expected_note":    "pqs_contains",
			}, predMeta, subqueryMeta, joinMeta, joinOn, derivedTables)),
		}
	}
	updateBandit(true, nil, false)
	return Result{OK: true, Oracle: o.Name(), SQL: []string{querySQL, containSQL}}
}

func pickPQSPivotRow(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) (*pqsPivotRow, map[string]any, error) {
	tables := state.BaseTables()
	if len(tables) == 0 {
		return nil, map[string]any{"skip_reason": "pqs:no_tables"}, nil
	}
	if len(tables) >= 2 {
		pivot, err := fetchPQSJoinPivotRow(ctx, exec, gen, tables)
		if err != nil {
			return nil, map[string]any{"skip_reason": "pqs:pivot_error"}, err
		}
		if pivot != nil {
			return pivot, nil, nil
		}
	}
	for i := 0; i < pqsPivotPickTries; i++ {
		tbl := tables[gen.Rand.Intn(len(tables))]
		pivot, err := fetchPQSPivotRow(ctx, exec, gen, tbl)
		if err != nil {
			return nil, map[string]any{"skip_reason": "pqs:pivot_error"}, err
		}
		if pivot == nil {
			continue
		}
		return pivot, nil, nil
	}
	return nil, map[string]any{"skip_reason": "pqs:no_rows"}, nil
}

func fetchPQSPivotRow(ctx context.Context, exec *db.DB, gen *generator.Generator, tbl schema.Table) (*pqsPivotRow, error) {
	if len(tbl.Columns) == 0 {
		return nil, nil
	}
	if pqsTableHasColumn(tbl, pqsPivotIDColumn) {
		pivot, err := fetchPQSPivotRowByID(ctx, exec, gen, tbl)
		if err != nil || pivot != nil {
			return pivot, err
		}
	}
	return fetchPQSPivotRowByRand(ctx, exec, tbl)
}

func fetchPQSPivotRowByRand(ctx context.Context, exec *db.DB, tbl schema.Table) (*pqsPivotRow, error) {
	cols := pqsSelectColumns([]schema.Table{tbl})
	if len(cols) == 0 {
		return nil, nil
	}
	colNames := make([]string, 0, len(cols))
	for _, col := range cols {
		colNames = append(colNames, col.SQL)
	}
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", strings.Join(colNames, ", "), tbl.Name)
	rows, err := exec.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer util.CloseWithErr(rows, "pqs pivot rows")
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	raw := make([]sql.RawBytes, len(colNames))
	scanArgs := make([]any, len(raw))
	for i := range raw {
		scanArgs[i] = &raw[i]
	}
	if err := rows.Scan(scanArgs...); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pqsPivotRowFromRaw([]schema.Table{tbl}, cols, raw), nil
}

func fetchPQSPivotRowByID(ctx context.Context, exec *db.DB, gen *generator.Generator, tbl schema.Table) (*pqsPivotRow, error) {
	minID, maxID, ok, err := fetchPQSTableIDRange(ctx, exec, tbl)
	if err != nil || !ok {
		return nil, err
	}
	candidate := minID
	if maxID > minID {
		candidate = minID + gen.Rand.Int63n(maxID-minID+1)
	}
	where := fmt.Sprintf("%s.%s >= ?", tbl.Name, pqsPivotIDColumn)
	pivot, err := fetchPQSPivotRowByQuery(ctx, exec, []schema.Table{tbl}, where, candidate)
	if err != nil || pivot != nil {
		return pivot, err
	}
	return fetchPQSPivotRowByQuery(ctx, exec, []schema.Table{tbl}, "")
}

func buildPQSQuery(pivot *pqsPivotRow) (*generator.SelectQuery, []pqsAliasColumn) {
	if pivot == nil || len(pivot.Tables) == 0 {
		return &generator.SelectQuery{}, nil
	}
	query := &generator.SelectQuery{
		From: generator.FromClause{BaseTable: pivot.Tables[0].Name},
	}
	for i := 1; i < len(pivot.Tables); i++ {
		query.From.Joins = append(query.From.Joins, generator.Join{
			Type:  generator.JoinInner,
			Table: pivot.Tables[i].Name,
			Using: []string{pqsPivotIDColumn},
		})
	}
	useIDOnly := true
	for _, tbl := range pivot.Tables {
		if _, ok := pqsTableColumn(tbl, pqsPivotIDColumn); !ok {
			useIDOnly = false
			break
		}
	}
	aliases := make([]pqsAliasColumn, 0, len(pivot.Tables))
	items := make([]generator.SelectItem, 0, len(pivot.Tables))
	multiTable := len(pivot.Tables) > 1
	for tIdx, tbl := range pivot.Tables {
		cols := tbl.Columns
		if useIDOnly {
			if idCol, ok := pqsTableColumn(tbl, pqsPivotIDColumn); ok {
				cols = []schema.Column{idCol}
			}
		}
		for _, col := range cols {
			alias := col.Name
			if multiTable {
				alias = fmt.Sprintf("t%d_%s", tIdx, col.Name)
			}
			items = append(items, generator.SelectItem{
				Expr:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}},
				Alias: alias,
			})
			aliases = append(aliases, pqsAliasColumn{Table: tbl.Name, Column: col, Alias: alias})
		}
	}
	query.Items = items
	return query, aliases
}

func pqsApplyDerivedTables(gen *generator.Generator, pivot *pqsPivotRow, query *generator.SelectQuery) []string {
	if gen == nil || pivot == nil || query == nil {
		return nil
	}
	if !gen.Config.Features.DerivedTables {
		return nil
	}
	wrapped := make([]string, 0, len(query.From.Joins)+1)
	if util.Chance(gen.Rand, pqsDerivedTableProb) {
		if baseTbl, ok := pqsPivotTable(pivot, query.From.BaseTable); ok {
			query.From.BaseQuery = pqsDerivedTableQuery(gen, pivot, baseTbl)
			query.From.BaseAlias = baseTbl.Name
			wrapped = append(wrapped, baseTbl.Name)
		}
	}
	for i := range query.From.Joins {
		if !util.Chance(gen.Rand, pqsDerivedTableProb) {
			continue
		}
		join := &query.From.Joins[i]
		if join.Table == "" || join.TableQuery != nil {
			continue
		}
		if tbl, ok := pqsPivotTable(pivot, join.Table); ok {
			join.TableQuery = pqsDerivedTableQuery(gen, pivot, tbl)
			join.TableAlias = tbl.Name
			wrapped = append(wrapped, tbl.Name)
		}
	}
	if len(wrapped) == 0 {
		return nil
	}
	return wrapped
}

func pqsDerivedTableQuery(gen *generator.Generator, pivot *pqsPivotRow, tbl schema.Table) *generator.SelectQuery {
	items := make([]generator.SelectItem, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		items = append(items, generator.SelectItem{
			Expr:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}},
			Alias: col.Name,
		})
	}
	predicate := pqsPredicateForTableWithRange(gen, pivot, tbl.Name, 1, 1, false)
	return &generator.SelectQuery{
		Items: items,
		From:  generator.FromClause{BaseTable: tbl.Name},
		Where: predicate,
	}
}

func pqsApplyJoinOn(gen *generator.Generator, pivot *pqsPivotRow, query *generator.SelectQuery) (pqsJoinPredicateMeta, bool) {
	if gen == nil || pivot == nil || query == nil || len(query.From.Joins) == 0 {
		return pqsJoinPredicateMeta{}, false
	}
	base, ok := pqsPivotTable(pivot, query.From.BaseTable)
	if !ok {
		return pqsJoinPredicateMeta{Reason: "join_base_missing"}, false
	}
	meta := pqsJoinPredicateMeta{Fallback: true, Reason: "join_rectify_skip"}
	preds := make([]string, 0, len(query.From.Joins))
	onExprs := make([]generator.Expr, 0, len(query.From.Joins))
	leftTables := []schema.Table{base}
	for i := range query.From.Joins {
		join := &query.From.Joins[i]
		right, ok := pqsPivotTable(pivot, join.Table)
		if !ok {
			return pqsJoinPredicateMeta{Reason: "join_table_missing"}, false
		}
		var rectified generator.Expr
		if util.Chance(gen.Rand, pqsJoinRectifyProb) {
			joinTables := append([]schema.Table{}, leftTables...)
			joinTables = append(joinTables, right)
			rectified, meta = pqsBuildJoinPredicateForTables(gen, pivot, joinTables)
		}
		leftTable := leftTables[len(leftTables)-1]
		onExpr, ok := pqsJoinOnExpr(gen, pivot, leftTable, right, rectified)
		if !ok {
			return pqsJoinPredicateMeta{Reason: "join_on_missing"}, false
		}
		preds = append(preds, buildExpr(onExpr))
		onExprs = append(onExprs, onExpr)
		leftTables = append(leftTables, right)
	}
	for i := range query.From.Joins {
		query.From.Joins[i].Using = nil
		query.From.Joins[i].On = onExprs[i]
	}
	meta.Predicates = preds
	return meta, true
}

func pqsJoinOnExpr(gen *generator.Generator, pivot *pqsPivotRow, base schema.Table, right schema.Table, rectified generator.Expr) (generator.Expr, bool) {
	eq, ok := pqsJoinEqualityExpr(base, right)
	if !ok {
		return nil, false
	}
	expr := eq
	rightPred := pqsPredicateForTableWithRange(gen, pivot, right.Name, 1, 1, true)
	expr = pqsCombinePredicates(expr, rightPred)
	expr = pqsCombinePredicates(expr, rectified)
	return expr, true
}

func pqsJoinEqualityExpr(left schema.Table, right schema.Table) (generator.Expr, bool) {
	leftCol, ok := pqsTableColumn(left, pqsPivotIDColumn)
	if !ok {
		return nil, false
	}
	rightCol, ok := pqsTableColumn(right, pqsPivotIDColumn)
	if !ok {
		return nil, false
	}
	return generator.BinaryExpr{
		Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: left.Name, Name: leftCol.Name, Type: leftCol.Type}},
		Op:    "=",
		Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: right.Name, Name: rightCol.Name, Type: rightCol.Type}},
	}, true
}

func pqsPredicateForTableWithRange(gen *generator.Generator, pivot *pqsPivotRow, tableName string, minCols, maxCols int, preferNonID bool) generator.Expr {
	if pivot == nil || tableName == "" {
		return nil
	}
	tbl, ok := pqsPivotTable(pivot, tableName)
	if !ok {
		return nil
	}
	cols := make([]schema.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		if !pqsPredicateColumnAllowed(col) {
			continue
		}
		if preferNonID && strings.EqualFold(col.Name, pqsPivotIDColumn) {
			continue
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 && preferNonID {
		for _, col := range tbl.Columns {
			if !pqsPredicateColumnAllowed(col) {
				continue
			}
			cols = append(cols, col)
		}
	}
	if len(cols) == 0 {
		return nil
	}
	indices := make([]int, len(cols))
	for i := range cols {
		indices[i] = i
	}
	if gen != nil {
		gen.Rand.Shuffle(len(indices), func(i, j int) {
			indices[i], indices[j] = indices[j], indices[i]
		})
	}
	maxAllowed := pqsMin(maxCols, len(indices))
	if maxAllowed <= 0 {
		return nil
	}
	minAllowed := minCols
	if minAllowed < 1 {
		minAllowed = 1
	}
	if minAllowed > maxAllowed {
		minAllowed = maxAllowed
	}
	useCols := minAllowed
	if gen != nil && maxAllowed > minAllowed {
		useCols = minAllowed + gen.Rand.Intn(maxAllowed-minAllowed+1)
	}
	var expr generator.Expr
	for _, idx := range indices[:useCols] {
		col := cols[idx]
		val, ok := pqsPivotValueFor(pivot, tbl.Name, col.Name)
		if !ok {
			continue
		}
		ref := generator.ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}
		part := pqsPredicateExprForValue(ref, val)
		expr = pqsCombinePredicates(expr, part)
	}
	return expr
}

func pqsPivotTable(pivot *pqsPivotRow, tableName string) (schema.Table, bool) {
	if pivot == nil {
		return schema.Table{}, false
	}
	for _, tbl := range pivot.Tables {
		if tbl.Name == tableName {
			return tbl, true
		}
	}
	return schema.Table{}, false
}

func pqsMaybeBuildSubqueryPredicate(gen *generator.Generator, pivot *pqsPivotRow) (generator.Expr, pqsSubqueryMeta) {
	return pqsBuildSubqueryPredicate(gen, pivot, false)
}

func pqsBuildSubqueryPredicate(gen *generator.Generator, pivot *pqsPivotRow, force bool) (generator.Expr, pqsSubqueryMeta) {
	meta := pqsSubqueryMeta{}
	if gen == nil || pivot == nil || len(pivot.Tables) == 0 {
		meta.Reason = "subquery_disabled"
		return nil, meta
	}
	if !gen.Config.Features.Subqueries {
		meta.Reason = "subquery_disabled"
		return nil, meta
	}
	if !force && !util.Chance(gen.Rand, pqsSubqueryProb) {
		meta.Reason = "subquery_skip"
		return nil, meta
	}
	tbl, col, val, ok := pqsPickSubqueryColumn(gen, pivot)
	if !ok {
		meta.Reason = "subquery_no_columns"
		return nil, meta
	}
	kind := "exists"
	if !val.Null {
		kinds := []string{"exists", "in"}
		if gen.Config.Features.QuantifiedSubqueries {
			kinds = append(kinds, "any", "all")
		}
		kind = kinds[gen.Rand.Intn(len(kinds))]
	}
	return pqsBuildSubqueryPredicateWithKind(gen, pivot, tbl, col, val, kind)
}

func pqsBuildSubqueryPredicateForKind(gen *generator.Generator, pivot *pqsPivotRow, kind string) (generator.Expr, pqsSubqueryMeta) {
	tbl, col, val, ok := pqsPickSubqueryColumn(gen, pivot)
	if !ok {
		return nil, pqsSubqueryMeta{Reason: "subquery_no_columns"}
	}
	return pqsBuildSubqueryPredicateWithKind(gen, pivot, tbl, col, val, kind)
}

func pqsBuildSubqueryPredicateWithKind(gen *generator.Generator, pivot *pqsPivotRow, tbl schema.Table, col schema.Column, val pqsPivotValue, kind string) (generator.Expr, pqsSubqueryMeta) {
	meta := pqsSubqueryMeta{Kind: kind, Table: tbl.Name, Column: col.Name}
	if val.Null && kind != "exists" {
		kind = "exists"
		meta.Kind = kind
		meta.Reason = "subquery_null_fallback"
	}
	ref := generator.ColumnRef{Table: tbl.Name, Name: col.Name, Type: col.Type}
	pivotPred := pqsPredicateExprForValue(ref, val)
	selectExpr := generator.Expr(generator.LiteralExpr{Value: 1})
	if kind != "exists" {
		selectExpr = generator.ColumnExpr{Ref: ref}
	}
	subquery := pqsBuildSubqueryQuery(tbl, selectExpr, pivotPred)
	meta.SQL = subquery.SQLString()
	left := generator.ColumnExpr{Ref: ref}
	switch kind {
	case "in":
		expr := generator.InExpr{Left: left, List: []generator.Expr{generator.SubqueryExpr{Query: subquery}}}
		meta.Predicate = buildExpr(expr)
		return expr, meta
	case "any":
		expr := generator.CompareSubqueryExpr{Left: left, Op: "=", Quantifier: "ANY", Query: subquery}
		meta.Predicate = buildExpr(expr)
		return expr, meta
	case "all":
		expr := generator.CompareSubqueryExpr{Left: left, Op: "=", Quantifier: "ALL", Query: subquery}
		meta.Predicate = buildExpr(expr)
		return expr, meta
	default:
		expr := generator.ExistsExpr{Query: subquery}
		meta.Predicate = buildExpr(expr)
		return expr, meta
	}
}

func pqsBuildSubqueryQuery(tbl schema.Table, selectExpr generator.Expr, predicate generator.Expr) *generator.SelectQuery {
	if selectExpr == nil {
		selectExpr = generator.LiteralExpr{Value: 1}
	}
	return &generator.SelectQuery{
		Items: []generator.SelectItem{{
			Expr:  selectExpr,
			Alias: "c0",
		}},
		From:  generator.FromClause{BaseTable: tbl.Name},
		Where: predicate,
	}
}

func pqsPickSubqueryColumn(gen *generator.Generator, pivot *pqsPivotRow) (schema.Table, schema.Column, pqsPivotValue, bool) {
	if pivot == nil {
		return schema.Table{}, schema.Column{}, pqsPivotValue{}, false
	}
	type candidate struct {
		Table schema.Table
		Col   schema.Column
		Val   pqsPivotValue
	}
	candidates := make([]candidate, 0, 8)
	for _, tbl := range pivot.Tables {
		for _, col := range tbl.Columns {
			if !pqsPredicateColumnAllowed(col) {
				continue
			}
			val, ok := pqsPivotValueFor(pivot, tbl.Name, col.Name)
			if !ok {
				continue
			}
			candidates = append(candidates, candidate{Table: tbl, Col: col, Val: val})
		}
	}
	if len(candidates) == 0 {
		return schema.Table{}, schema.Column{}, pqsPivotValue{}, false
	}
	idx := 0
	if gen != nil {
		idx = gen.Rand.Intn(len(candidates))
	}
	chosen := candidates[idx]
	return chosen.Table, chosen.Col, chosen.Val, true
}

func pqsCombinePredicates(left, right generator.Expr) generator.Expr {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return generator.BinaryExpr{Left: left, Op: "AND", Right: right}
}

func pqsAttachMeta(details map[string]any, predMeta pqsPredicateMeta, subqueryMeta pqsSubqueryMeta, joinMeta *pqsJoinPredicateMeta, joinOn bool, derivedTables []string) map[string]any {
	if details == nil {
		details = map[string]any{}
	}
	if joinOn {
		details["pqs_join_on"] = true
	}
	if joinMeta != nil {
		if joinMeta.Original != "" {
			details["pqs_join_predicate_original"] = joinMeta.Original
		}
		if joinMeta.Rectified != "" {
			details["pqs_join_predicate"] = joinMeta.Rectified
		}
		if joinMeta.Reason != "" {
			details["pqs_join_rectify_reason"] = joinMeta.Reason
		}
		details["pqs_join_rectify_fallback"] = joinMeta.Fallback
		if len(joinMeta.Predicates) > 0 {
			details["pqs_join_predicates"] = joinMeta.Predicates
		}
	}
	if subqueryMeta.Kind != "" {
		details["pqs_subquery_kind"] = subqueryMeta.Kind
	}
	if subqueryMeta.Reason != "" {
		details["pqs_subquery_reason"] = subqueryMeta.Reason
	}
	if subqueryMeta.SQL != "" {
		details["pqs_subquery_sql"] = subqueryMeta.SQL
	}
	if subqueryMeta.Predicate != "" {
		details["pqs_subquery_predicate"] = subqueryMeta.Predicate
	}
	if subqueryMeta.Table != "" {
		details["pqs_subquery_table"] = subqueryMeta.Table
	}
	if subqueryMeta.Column != "" {
		details["pqs_subquery_column"] = subqueryMeta.Column
	}
	if len(derivedTables) > 0 {
		details["pqs_derived_tables"] = derivedTables
	}
	return details
}

func pqsPredicateForPivot(gen *generator.Generator, pivot *pqsPivotRow) generator.Expr {
	return pqsPredicateForPivotWithRange(gen, pivot, 1, pqsPredicateMaxCols)
}

func pqsHasSafePredicateColumns(pivot *pqsPivotRow) bool {
	if pivot == nil {
		return false
	}
	for _, tbl := range pivot.Tables {
		for _, col := range tbl.Columns {
			if pqsPredicateColumnAllowed(col) {
				return true
			}
		}
	}
	return false
}

func pqsPredicateColumnAllowed(col schema.Column) bool {
	switch col.Type {
	case schema.TypeFloat, schema.TypeDouble:
		return false
	default:
		return true
	}
}

func pqsPredicateForPivotWithRange(gen *generator.Generator, pivot *pqsPivotRow, minCols, maxCols int) generator.Expr {
	if pivot == nil || len(pivot.Tables) == 0 {
		return nil
	}
	type pqsColumnRef struct {
		Table  string
		Column schema.Column
	}
	allowedCols := 0
	for _, tbl := range pivot.Tables {
		for _, col := range tbl.Columns {
			if !pqsPredicateColumnAllowed(col) {
				continue
			}
			allowedCols++
		}
	}
	cols := make([]pqsColumnRef, 0, allowedCols)
	for _, tbl := range pivot.Tables {
		for _, col := range tbl.Columns {
			if !pqsPredicateColumnAllowed(col) {
				continue
			}
			cols = append(cols, pqsColumnRef{Table: tbl.Name, Column: col})
		}
	}
	if len(cols) == 0 {
		return nil
	}
	indices := make([]int, len(cols))
	for i := range cols {
		indices[i] = i
	}
	gen.Rand.Shuffle(len(indices), func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})
	maxAllowed := pqsMin(maxCols, len(indices))
	if maxAllowed <= 0 {
		return nil
	}
	minAllowed := minCols
	if minAllowed < 1 {
		minAllowed = 1
	}
	if minAllowed > maxAllowed {
		minAllowed = maxAllowed
	}
	useCols := minAllowed
	if maxAllowed > minAllowed {
		useCols = minAllowed + gen.Rand.Intn(maxAllowed-minAllowed+1)
	}
	var expr generator.Expr
	for _, idx := range indices[:useCols] {
		col := cols[idx]
		val, ok := pqsPivotValueFor(pivot, col.Table, col.Column.Name)
		if !ok {
			continue
		}
		ref := generator.ColumnRef{Table: col.Table, Name: col.Column.Name, Type: col.Column.Type}
		part := pqsPredicateExprForValue(ref, val)
		if expr == nil {
			expr = part
		} else {
			expr = generator.BinaryExpr{Left: expr, Op: "AND", Right: part}
		}
	}
	return expr
}

func pqsMatchExpr(pivot *pqsPivotRow, aliases []pqsAliasColumn) generator.Expr {
	var expr generator.Expr
	for _, alias := range aliases {
		val, ok := pqsPivotValueFor(pivot, alias.Table, alias.Column.Name)
		if !ok {
			continue
		}
		ref := generator.ColumnRef{Name: alias.Alias, Type: alias.Column.Type}
		part := pqsPredicateExprForValue(ref, val)
		if expr == nil {
			expr = part
		} else {
			expr = generator.BinaryExpr{Left: expr, Op: "AND", Right: part}
		}
	}
	return expr
}

func pqsPredicateExprForValue(ref generator.ColumnRef, val pqsPivotValue) generator.Expr {
	if val.Null {
		return generator.BinaryExpr{
			Left:  generator.ColumnExpr{Ref: ref},
			Op:    "IS",
			Right: generator.LiteralExpr{Value: nil},
		}
	}
	return generator.BinaryExpr{
		Left:  generator.ColumnExpr{Ref: ref},
		Op:    "=",
		Right: generator.LiteralExpr{Value: pqsLiteralValue(val.Column, val.Raw)},
	}
}

func pqsLiteralValue(col schema.Column, raw string) any {
	switch col.Type {
	case schema.TypeInt, schema.TypeBigInt:
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return v
		}
	case schema.TypeFloat, schema.TypeDouble:
		return raw
	case schema.TypeBool:
		lower := strings.ToLower(raw)
		if lower == "true" || lower == "1" {
			return true
		}
		if lower == "false" || lower == "0" {
			return false
		}
	case schema.TypeDecimal:
		return raw
	}
	return raw
}

func pqsPivotValueMap(pivot *pqsPivotRow) map[string]any {
	if pivot == nil {
		return nil
	}
	out := make(map[string]any, len(pivot.Values))
	for _, tbl := range pivot.Tables {
		tableVals, ok := pivot.Values[tbl.Name]
		if !ok {
			continue
		}
		for _, col := range tbl.Columns {
			val, ok := tableVals[col.Name]
			if !ok {
				continue
			}
			key := fmt.Sprintf("%s.%s", tbl.Name, col.Name)
			if val.Null {
				out[key] = nil
			} else {
				out[key] = val.Raw
			}
		}
	}
	return out
}

func pqsPivotValueFor(pivot *pqsPivotRow, tableName, columnName string) (pqsPivotValue, bool) {
	if pivot == nil {
		return pqsPivotValue{}, false
	}
	tableVals, ok := pivot.Values[tableName]
	if !ok {
		return pqsPivotValue{}, false
	}
	val, ok := tableVals[columnName]
	return val, ok
}

func pqsPivotTableNames(pivot *pqsPivotRow) []string {
	if pivot == nil {
		return nil
	}
	names := make([]string, 0, len(pivot.Tables))
	for _, tbl := range pivot.Tables {
		names = append(names, tbl.Name)
	}
	return names
}

func pqsSingleTableName(tables []string) string {
	if len(tables) == 1 {
		return tables[0]
	}
	return ""
}

func pqsTableHasColumn(tbl schema.Table, name string) bool {
	for _, col := range tbl.Columns {
		if col.Name == name {
			return true
		}
	}
	return false
}

func pqsTableColumn(tbl schema.Table, name string) (schema.Column, bool) {
	for _, col := range tbl.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return schema.Column{}, false
}

func pqsSelectColumns(tables []schema.Table) []pqsSelectColumn {
	totalCols := 0
	for _, tbl := range tables {
		totalCols += len(tbl.Columns)
	}
	cols := make([]pqsSelectColumn, 0, totalCols)
	for _, tbl := range tables {
		for _, col := range tbl.Columns {
			cols = append(cols, pqsSelectColumn{
				TableName: tbl.Name,
				Column:    col,
				SQL:       fmt.Sprintf("%s.%s", tbl.Name, col.Name),
			})
		}
	}
	return cols
}

func pqsPivotRowFromRaw(tables []schema.Table, cols []pqsSelectColumn, raw []sql.RawBytes) *pqsPivotRow {
	values := make(map[string]map[string]pqsPivotValue, len(tables))
	for _, tbl := range tables {
		values[tbl.Name] = make(map[string]pqsPivotValue, len(tbl.Columns))
	}
	for i, col := range cols {
		val := pqsPivotValue{Column: col.Column}
		if raw[i] == nil {
			val.Null = true
		} else {
			val.Raw = string(raw[i])
		}
		values[col.TableName][col.Column.Name] = val
	}
	return &pqsPivotRow{Tables: tables, Values: values}
}

func fetchPQSTableIDRange(ctx context.Context, exec *db.DB, tbl schema.Table) (minID int64, maxID int64, ok bool, err error) {
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", pqsPivotIDColumn, pqsPivotIDColumn, tbl.Name)
	row := exec.QueryRowContext(ctx, query)
	var minVal sql.NullInt64
	var maxVal sql.NullInt64
	if err = row.Scan(&minVal, &maxVal); err != nil {
		return 0, 0, false, err
	}
	if !minVal.Valid || !maxVal.Valid {
		return 0, 0, false, nil
	}
	return minVal.Int64, maxVal.Int64, true, nil
}

func pqsFromClause(tables []schema.Table) string {
	if len(tables) == 0 {
		return ""
	}
	clause := tables[0].Name
	for i := 1; i < len(tables); i++ {
		clause = fmt.Sprintf("%s JOIN %s USING (%s)", clause, tables[i].Name, pqsPivotIDColumn)
	}
	return clause
}

func pqsOrderByClause(tables []schema.Table) string {
	if len(tables) == 0 || !pqsTableHasColumn(tables[0], pqsPivotIDColumn) {
		return ""
	}
	return fmt.Sprintf("ORDER BY %s.%s", tables[0].Name, pqsPivotIDColumn)
}

func fetchPQSPivotRowByQuery(ctx context.Context, exec *db.DB, tables []schema.Table, whereSQL string, args ...any) (*pqsPivotRow, error) {
	cols := pqsSelectColumns(tables)
	if len(cols) == 0 {
		return nil, nil
	}
	selectList := make([]string, 0, len(cols))
	for _, col := range cols {
		selectList = append(selectList, col.SQL)
	}
	fromClause := pqsFromClause(tables)
	if fromClause == "" {
		return nil, nil
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectList, ", "), fromClause)
	if whereSQL != "" {
		query += " WHERE " + whereSQL
	}
	if orderBy := pqsOrderByClause(tables); orderBy != "" {
		query += " " + orderBy
	}
	query += " LIMIT 1"
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer util.CloseWithErr(rows, "pqs pivot rows")
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	raw := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(raw))
	for i := range raw {
		scanArgs[i] = &raw[i]
	}
	if err := rows.Scan(scanArgs...); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pqsPivotRowFromRaw(tables, cols, raw), nil
}

func fetchPQSJoinPivotRow(ctx context.Context, exec *db.DB, gen *generator.Generator, tables []schema.Table) (*pqsPivotRow, error) {
	if len(tables) < 2 {
		return nil, nil
	}
	for i := 0; i < pqsJoinPickTries; i++ {
		left := tables[gen.Rand.Intn(len(tables))]
		right := left
		for right.Name == left.Name {
			right = tables[gen.Rand.Intn(len(tables))]
		}
		if !pqsTableHasColumn(left, pqsPivotIDColumn) || !pqsTableHasColumn(right, pqsPivotIDColumn) {
			continue
		}
		pivot, err := fetchPQSJoinPivotRowForTables(ctx, exec, gen, left, right)
		if err != nil {
			return nil, err
		}
		if pivot != nil {
			return pivot, nil
		}
	}
	return nil, nil
}

func fetchPQSJoinPivotRowForTables(ctx context.Context, exec *db.DB, gen *generator.Generator, left, right schema.Table) (*pqsPivotRow, error) {
	minLeft, maxLeft, okLeft, err := fetchPQSTableIDRange(ctx, exec, left)
	if err != nil || !okLeft {
		return nil, err
	}
	minRight, maxRight, okRight, err := fetchPQSTableIDRange(ctx, exec, right)
	if err != nil || !okRight {
		return nil, err
	}
	start := minLeft
	if minRight > start {
		start = minRight
	}
	end := maxLeft
	if maxRight < end {
		end = maxRight
	}
	if start > end {
		return nil, nil
	}
	candidate := start
	if end > start {
		candidate = start + gen.Rand.Int63n(end-start+1)
	}
	where := fmt.Sprintf("%s.%s >= ?", left.Name, pqsPivotIDColumn)
	pivot, err := fetchPQSPivotRowByQuery(ctx, exec, []schema.Table{left, right}, where, candidate)
	if err != nil || pivot != nil {
		return pivot, err
	}
	return fetchPQSPivotRowByQuery(ctx, exec, []schema.Table{left, right}, "")
}

func pqsMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
