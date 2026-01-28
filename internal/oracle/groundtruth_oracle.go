package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/oracle/groundtruth"
	"shiro/internal/schema"
)

// GroundTruth compares join counts against a Go-evaluated truth model.
// It only runs on simple inner-join queries without additional predicates.
type GroundTruth struct{}

// Name returns the oracle identifier.
func (o GroundTruth) Name() string { return "GroundTruth" }

// Run evaluates join counts using an in-memory join and compares with the DB count.
func (o GroundTruth) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	query := gen.GenerateSelectQuery()
	if query == nil {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:empty_query"}}
	}
	if shouldSkipGroundTruth(query) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:guardrail"}}
	}
	maxRows := gen.Config.Oracles.GroundTruthMaxRows
	if maxRows <= 0 {
		maxRows = 50
	}
	if gen.Truth != nil {
		if truth, ok := gen.Truth.(*groundtruth.SchemaTruth); ok {
			return o.runWithTruth(ctx, exec, truth, query, state, gen.Config.Features.DSG, maxRows)
		}
	}
	edges := groundtruth.JoinEdgesFromQuery(query, state)
	if len(edges) != len(query.From.Joins) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:edge_mismatch"}}
	}
	if gen != nil && gen.Config.Features.DSG && !validDSGTruthJoin(query.From.BaseTable, edges) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:dsg_key_mismatch"}}
	}
	for _, edge := range edges {
		if edge.JoinType != groundtruth.JoinInner {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:join_type"}}
		}
		if edge.LeftKey == "" || edge.RightKey == "" {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:key_missing"}}
		}
	}
	columnsByTable := joinKeyColumns(state, edges, query.From.BaseTable)
	if len(columnsByTable) == 0 {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:no_columns"}}
	}
	if shouldSkipGroundTruthByRowCount(ctx, exec, query, maxRows) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{
			"groundtruth_skip": "rowcount_exceeded",
			"skip_reason":      "groundtruth:rowcount_exceeded",
		}}
	}
	leftRows, leftSQL, err := fetchRows(ctx, exec, query.From.BaseTable, columnsByTable[query.From.BaseTable], maxRows)
	if err != nil {
		if isSchemaColumnMissingErr(err) {
			return Result{OK: false, Oracle: o.Name(), SQL: []string{leftSQL}, Err: err}
		}
		return Result{OK: true, Oracle: o.Name(), Err: err}
	}
	rows := leftRows
	var ok bool
	for i, join := range query.From.Joins {
		edge := edges[i]
		rightCols := columnsByTable[join.Table]
		rightRows, rightSQL, err := fetchRows(ctx, exec, join.Table, rightCols, maxRows)
		if err != nil {
			if isSchemaColumnMissingErr(err) {
				return Result{OK: false, Oracle: o.Name(), SQL: []string{rightSQL}, Err: err}
			}
			return Result{OK: true, Oracle: o.Name(), Err: err}
		}
		rows, ok = joinRows(rows, rightRows, edge.LeftTable, edge.LeftKey, edge.RightTable, edge.RightKey, maxRows)
		if !ok {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{
				"groundtruth_skip": "join_rows_exceeded",
				"skip_reason":      "groundtruth:join_rows_exceeded",
			}}
		}
	}
	truthCount := len(rows)
	sqlText := query.SQLString()
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) q", sqlText)
	actual, err := exec.QueryCount(ctx, countSQL)
	if err != nil {
		if isSchemaColumnMissingErr(err) {
			return Result{OK: false, Oracle: o.Name(), SQL: []string{countSQL}, Err: err}
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{countSQL}, Err: err}
	}
	truth := &GroundTruthMetrics{
		Enabled:  true,
		Mismatch: int64(truthCount) != actual,
		JoinSig:  joinSignature(query),
		RowCount: truthCount,
	}
	if truth.Mismatch {
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{sqlText, countSQL},
			Expected: fmt.Sprintf("truth count=%d", truthCount),
			Actual:   fmt.Sprintf("db count=%d", actual),
			Truth:    truth,
			Details: map[string]any{
				"replay_kind":         "count",
				"replay_expected_sql": countSQL,
				"replay_actual_sql":   countSQL,
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{sqlText, countSQL}, Truth: truth}
}

func (o GroundTruth) runWithTruth(ctx context.Context, exec *db.DB, truth *groundtruth.SchemaTruth, query *generator.SelectQuery, state *schema.State, dsgEnabled bool, maxRows int) Result {
	if truth == nil {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:truth_missing"}}
	}
	edges := groundtruth.JoinEdgesFromQuery(query, state)
	if len(edges) != len(query.From.Joins) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:edge_mismatch"}}
	}
	if dsgEnabled && !validDSGTruthJoin(query.From.BaseTable, edges) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:dsg_key_mismatch"}}
	}
	for _, edge := range edges {
		if edge.LeftKey == "" || edge.RightKey == "" {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:key_missing"}}
		}
	}
	if dsgEnabled {
		tableCap, joinCap := groundTruthCaps(maxRows)
		executor := groundtruth.JoinTruthExecutor{Truth: *truth}
		truthCount, ok, reason := executor.EvalJoinChainExact(query.From.BaseTable, edges, tableCap, joinCap)
		if !ok {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": exactSkipReason(reason)}}
		}
		return o.compareTruthCount(ctx, exec, query, truthCount)
	}
	for _, edge := range edges {
		if edge.JoinType != groundtruth.JoinInner {
			return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "groundtruth:join_type"}}
		}
	}
	executor := groundtruth.JoinTruthExecutor{Truth: *truth}
	bitmap := executor.EvalJoinChain(query.From.BaseTable, edges)
	truthCount := bitmap.Count()
	return o.compareTruthCount(ctx, exec, query, truthCount)
}

func (o GroundTruth) compareTruthCount(ctx context.Context, exec *db.DB, query *generator.SelectQuery, truthCount int) Result {
	sqlText := query.SQLString()
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) q", sqlText)
	actual, err := exec.QueryCount(ctx, countSQL)
	if err != nil {
		if isSchemaColumnMissingErr(err) {
			return Result{OK: false, Oracle: o.Name(), SQL: []string{countSQL}, Err: err}
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{countSQL}, Err: err}
	}
	truthMeta := &GroundTruthMetrics{
		Enabled:  true,
		Mismatch: int64(truthCount) != actual,
		JoinSig:  joinSignature(query),
		RowCount: truthCount,
	}
	if truthMeta.Mismatch {
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{sqlText, countSQL},
			Expected: fmt.Sprintf("truth count=%d", truthCount),
			Actual:   fmt.Sprintf("db count=%d", actual),
			Truth:    truthMeta,
			Details: map[string]any{
				"replay_kind":         "count",
				"replay_expected_sql": countSQL,
				"replay_actual_sql":   countSQL,
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{sqlText, countSQL}, Truth: truthMeta}
}

func groundTruthCaps(maxRows int) (tableCap int, joinCap int) {
	if maxRows <= 0 {
		maxRows = 50
	}
	joinCap = maxRows * maxRows
	if joinCap < maxRows {
		joinCap = maxRows
	}
	if joinCap > 10_000 {
		joinCap = 10_000
	}
	return maxRows, joinCap
}

func exactSkipReason(reason string) string {
	switch reason {
	case "join_rows_exceeded":
		return "groundtruth:join_rows_exceeded"
	case "missing_rows":
		return "groundtruth:rows_missing"
	case "table_rows_exceeded":
		return "groundtruth:table_rows_exceeded"
	case "unsupported_join":
		return "groundtruth:unsupported_join"
	default:
		return "groundtruth:exact_unknown"
	}
}

func shouldSkipGroundTruth(query *generator.SelectQuery) bool {
	if query == nil {
		return true
	}
	if len(query.With) > 0 {
		return true
	}
	if query.Distinct || query.Where != nil || len(query.GroupBy) > 0 || query.Having != nil {
		return true
	}
	if len(query.OrderBy) > 0 || query.Limit != nil {
		return true
	}
	if len(query.From.Joins) == 0 {
		return true
	}
	return false
}

func validDSGTruthJoin(base string, edges []groundtruth.JoinEdge) bool {
	if base != "t0" {
		return false
	}
	for _, edge := range edges {
		if !validDSGTruthKey(base, edge.LeftTable, edge.LeftKey) {
			return false
		}
		if !validDSGTruthKey(base, edge.RightTable, edge.RightKey) {
			return false
		}
	}
	return true
}

func validDSGTruthKey(base, table, key string) bool {
	if table == base {
		return true
	}
	idx, ok := parseTableIndex(table)
	if !ok || idx <= 0 {
		return false
	}
	// For DSG dimension tables (t{idx}, idx > 0), allow the shared key k0
	// or the chain key k{idx-1}.
	return key == "k0" || key == fmt.Sprintf("k%d", idx-1)
}

func parseTableIndex(name string) (int, bool) {
	if !strings.HasPrefix(name, "t") || len(name) < 2 {
		return 0, false
	}
	val, err := strconv.Atoi(name[1:])
	if err != nil {
		return 0, false
	}
	return val, true
}

func joinKeyColumns(state *schema.State, edges []groundtruth.JoinEdge, base string) map[string][]string {
	out := make(map[string]map[string]struct{})
	add := func(table, col string) {
		if table == "" || col == "" {
			return
		}
		if out[table] == nil {
			out[table] = make(map[string]struct{})
		}
		out[table][col] = struct{}{}
	}
	if state != nil {
		if tbl, ok := state.TableByName(base); ok {
			if _, ok := tbl.ColumnByName("id"); ok {
				add(base, "id")
			}
		}
	}
	for _, edge := range edges {
		add(edge.LeftTable, edge.LeftKey)
		add(edge.RightTable, edge.RightKey)
	}
	result := make(map[string][]string)
	for table, cols := range out {
		list := make([]string, 0, len(cols))
		for col := range cols {
			list = append(list, col)
		}
		result[table] = list
	}
	return result
}

func shouldSkipGroundTruthByRowCount(ctx context.Context, exec *db.DB, query *generator.SelectQuery, maxRows int) bool {
	if query == nil || exec == nil {
		return true
	}
	if maxRows <= 0 {
		maxRows = 50
	}
	if query.From.BaseTable != "" {
		count, err := exec.QueryCount(ctx, fmt.Sprintf("SELECT COUNT(*) FROM `%s`", query.From.BaseTable))
		if err == nil && int(count) > maxRows {
			return true
		}
	}
	for _, join := range query.From.Joins {
		if join.Table == "" {
			continue
		}
		count, err := exec.QueryCount(ctx, fmt.Sprintf("SELECT COUNT(*) FROM `%s`", join.Table))
		if err == nil && int(count) > maxRows {
			return true
		}
	}
	return false
}

type cellValue struct {
	Val  string
	Null bool
}

type rowData map[string]cellValue

func fetchRows(ctx context.Context, exec *db.DB, table string, cols []string, limit int) ([]rowData, string, error) {
	if len(cols) == 0 {
		return nil, "", nil
	}
	if limit <= 0 {
		limit = 50
	}
	selectCols := ""
	for i, col := range cols {
		if i > 0 {
			selectCols += ","
		}
		selectCols += fmt.Sprintf("`%s`", col)
	}
	query := fmt.Sprintf("SELECT %s FROM `%s` LIMIT %d", selectCols, table, limit)
	rows, err := exec.QueryContext(ctx, query)
	if err != nil {
		return nil, query, err
	}
	defer func() {
		_ = rows.Close()
	}()
	colNames, err := rows.Columns()
	if err != nil {
		return nil, query, err
	}
	values := make([]sql.RawBytes, len(colNames))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	out := make([]rowData, 0, limit)
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, query, err
		}
		row := make(rowData, len(colNames))
		for i, name := range colNames {
			cell := cellValue{Val: string(values[i])}
			if values[i] == nil {
				cell.Null = true
				cell.Val = ""
			}
			row[fmt.Sprintf("%s.%s", table, name)] = cell
		}
		out = append(out, row)
	}
	return out, query, nil
}

func joinRows(left []rowData, right []rowData, leftTable, leftKey, rightTable, rightKey string, maxRows int) ([]rowData, bool) {
	if len(left) == 0 || len(right) == 0 {
		return nil, true
	}
	lk := fmt.Sprintf("%s.%s", leftTable, leftKey)
	rk := fmt.Sprintf("%s.%s", rightTable, rightKey)
	rightIndex := make(map[string][]rowData)
	for _, row := range right {
		cell := row[rk]
		if cell.Null {
			continue
		}
		rightIndex[cell.Val] = append(rightIndex[cell.Val], row)
	}
	out := make([]rowData, 0)
	for _, lrow := range left {
		cell := lrow[lk]
		if cell.Null {
			continue
		}
		matches := rightIndex[cell.Val]
		for _, rrow := range matches {
			if maxRows > 0 && len(out) >= maxRows {
				return nil, false
			}
			merged := make(rowData, len(lrow)+len(rrow))
			for k, v := range lrow {
				merged[k] = v
			}
			for k, v := range rrow {
				merged[k] = v
			}
			out = append(out, merged)
		}
	}
	return out, true
}

func joinSignature(query *generator.SelectQuery) string {
	if query == nil {
		return ""
	}
	if len(query.From.Joins) == 0 {
		return query.From.BaseTable
	}
	parts := []string{query.From.BaseTable}
	for _, join := range query.From.Joins {
		parts = append(parts, fmt.Sprintf("%s:%s", join.Type, join.Table))
	}
	return strings.Join(parts, "->")
}
