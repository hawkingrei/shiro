package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

// PQS implements a v1 Pivoted Query Synthesis oracle (single-table WHERE).
type PQS struct{}

// Name returns the oracle identifier.
func (o PQS) Name() string { return "PQS" }

const (
	pqsPivotPickTries   = 6
	pqsPredicateMaxCols = 3
)

type pqsPivotValue struct {
	Column schema.Column
	Raw    string
	Null   bool
}

type pqsPivotRow struct {
	Table  schema.Table
	Values map[string]pqsPivotValue
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
	predicate := pqsPredicateForPivot(gen, pivot)
	if predicate == nil {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "pqs:predicate_empty"}}
	}
	query.Where = predicate

	querySQL := query.SQLString()
	matchExpr := pqsMatchExpr(pivot, aliases)
	matchSQL := buildExpr(matchExpr)
	containSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) pqs WHERE %s", querySQL, matchSQL)
	present, err := exec.QueryCount(ctx, containSQL)
	if err != nil {
		reason, code := sqlErrorReason("pqs", err)
		details := map[string]any{"error_reason": reason}
		if code != 0 {
			details["error_code"] = int(code)
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{querySQL, containSQL}, Err: err, Details: details}
	}
	if present == 0 {
		replayExpected := fmt.Sprintf("SELECT IF(COUNT(*)>0,1,0) FROM (%s) pqs WHERE %s", querySQL, matchSQL)
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{querySQL, containSQL},
			Expected: "pivot_row_present",
			Actual:   "pivot_row_missing",
			Details: map[string]any{
				"pqs_table":            pivot.Table.Name,
				"pqs_predicate":        buildExpr(predicate),
				"pqs_match":            matchSQL,
				"pivot_values":         pqsPivotValueMap(pivot),
				"rectified_predicates": []string{buildExpr(predicate)},
				"containment_query":    containSQL,
				"replay_kind":          "count",
				"replay_expected_sql":  replayExpected,
				"replay_actual_sql":    "SELECT 1",
				"replay_expected_note": "pqs_contains",
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{querySQL, containSQL}}
}

func pickPQSPivotRow(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) (*pqsPivotRow, map[string]any, error) {
	tables := state.BaseTables()
	if len(tables) == 0 {
		return nil, map[string]any{"skip_reason": "pqs:no_tables"}, nil
	}
	for i := 0; i < pqsPivotPickTries; i++ {
		tbl := tables[gen.Rand.Intn(len(tables))]
		pivot, err := fetchPQSPivotRow(ctx, exec, tbl)
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

func fetchPQSPivotRow(ctx context.Context, exec *db.DB, tbl schema.Table) (*pqsPivotRow, error) {
	if len(tbl.Columns) == 0 {
		return nil, nil
	}
	colNames := make([]string, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		colNames = append(colNames, col.Name)
	}
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", strings.Join(colNames, ", "), tbl.Name)
	rows, err := exec.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
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
	values := make(map[string]pqsPivotValue, len(tbl.Columns))
	for i, col := range tbl.Columns {
		val := pqsPivotValue{Column: col}
		if raw[i] == nil {
			val.Null = true
		} else {
			val.Raw = string(raw[i])
		}
		values[col.Name] = val
	}
	return &pqsPivotRow{Table: tbl, Values: values}, nil
}

func buildPQSQuery(pivot *pqsPivotRow) (*generator.SelectQuery, []string) {
	query := &generator.SelectQuery{
		From: generator.FromClause{BaseTable: pivot.Table.Name},
	}
	aliases := make([]string, 0, len(pivot.Table.Columns))
	items := make([]generator.SelectItem, 0, len(pivot.Table.Columns))
	for i, col := range pivot.Table.Columns {
		alias := fmt.Sprintf("c%d", i)
		items = append(items, generator.SelectItem{
			Expr:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: pivot.Table.Name, Name: col.Name, Type: col.Type}},
			Alias: alias,
		})
		aliases = append(aliases, alias)
	}
	query.Items = items
	return query, aliases
}

func pqsPredicateForPivot(gen *generator.Generator, pivot *pqsPivotRow) generator.Expr {
	cols := pivot.Table.Columns
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
	maxCols := pqsMin(pqsPredicateMaxCols, len(indices))
	useCols := 1
	if maxCols > 1 {
		useCols = 1 + gen.Rand.Intn(maxCols)
	}
	var expr generator.Expr
	for _, idx := range indices[:useCols] {
		col := cols[idx]
		val := pivot.Values[col.Name]
		ref := generator.ColumnRef{Table: pivot.Table.Name, Name: col.Name, Type: col.Type}
		part := pqsPredicateExprForValue(ref, val)
		if expr == nil {
			expr = part
		} else {
			expr = generator.BinaryExpr{Left: expr, Op: "AND", Right: part}
		}
	}
	return expr
}

func pqsMatchExpr(pivot *pqsPivotRow, aliases []string) generator.Expr {
	var expr generator.Expr
	for i, col := range pivot.Table.Columns {
		val := pivot.Values[col.Name]
		ref := generator.ColumnRef{Name: aliases[i], Type: col.Type}
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
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			return v
		}
	case schema.TypeBool:
		if raw == "0" || raw == "1" {
			if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
				return v
			}
		}
		lower := strings.ToLower(raw)
		if lower == "true" || lower == "false" {
			return lower == "true"
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
	for name, val := range pivot.Values {
		if val.Null {
			out[name] = nil
		} else {
			out[name] = val.Raw
		}
	}
	return out
}

func pqsMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
