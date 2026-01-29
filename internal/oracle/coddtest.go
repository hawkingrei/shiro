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
	"shiro/internal/util"
)

// CODDTest implements the constant folding oracle.
//
// It compares a query using predicate Phi against a variant where Phi is
// replaced by a constant (or a CASE mapping), based on sampled rows.
// If constant folding/propagation is incorrect, the two signatures differ.
type CODDTest struct{}

// Name returns the oracle identifier.
func (o CODDTest) Name() string { return "CODDTest" }

// Run selects a predicate Phi, ensures it is deterministic and NULL-free,
// then builds independent or dependent variants:
// - Independent: Phi is replaced by a single literal (global mapping).
// - Dependent: Phi is replaced by a CASE mapping computed from sample rows.
// The query signatures must match.
//
// Example:
//
//	Phi:  a > 10
//	Q:    SELECT * FROM t WHERE a > 10
//	Fold: SELECT * FROM t WHERE 1
//
// If folding changes results, constant propagation is incorrect.
func (o CODDTest) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	if !state.HasTables() {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "coddtest:no_tables"}}
	}
	tbl := state.Tables[gen.Rand.Intn(len(state.Tables))]
	phi := gen.GenerateSimplePredicate([]schema.Table{tbl}, 2)
	if !phi.Deterministic() || exprHasSubquery(phi) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "coddtest:predicate_guard"}}
	}
	policy := predicatePolicyFor(gen)
	policy.allowIsNull = false
	if !predicateMatches(phi, policy) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "coddtest:predicate_guard"}}
	}
	columns := phi.Columns()
	if !onlyIntOrBoolColumns(columns) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "coddtest:type_guard"}}
	}
	if !o.noNullsInTable(ctx, exec, tbl, columns) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "coddtest:null_guard"}}
	}
	if len(columns) == 0 {
		return o.runIndependent(ctx, exec, gen, tbl, phi)
	}
	return o.runDependent(ctx, exec, gen, tbl, phi, columns)
}

func onlyIntOrBoolColumns(columns []generator.ColumnRef) bool {
	if len(columns) == 0 {
		return false
	}
	for _, col := range columns {
		switch col.Type {
		case schema.TypeInt, schema.TypeBigInt, schema.TypeBool:
			continue
		default:
			return false
		}
	}
	return true
}

func (o CODDTest) noNullsInTable(ctx context.Context, exec *db.DB, tbl schema.Table, columns []generator.ColumnRef) bool {
	if len(columns) == 0 {
		return o.noNullsAnyColumn(ctx, exec, tbl)
	}
	seen := map[string]struct{}{}
	for _, col := range columns {
		if col.Table != tbl.Name {
			continue
		}
		if _, ok := seen[col.Name]; ok {
			continue
		}
		seen[col.Name] = struct{}{}
		countSQL := fmt.Sprintf("SELECT SUM(%s IS NULL) FROM %s", col.Name, tbl.Name)
		nulls, err := exec.QueryCount(ctx, countSQL)
		if err != nil || nulls > 0 {
			return false
		}
	}
	return true
}

func (o CODDTest) noNullsAnyColumn(ctx context.Context, exec *db.DB, tbl schema.Table) bool {
	for _, col := range tbl.Columns {
		countSQL := fmt.Sprintf("SELECT SUM(%s IS NULL) FROM %s", col.Name, tbl.Name)
		nulls, err := exec.QueryCount(ctx, countSQL)
		if err != nil || nulls > 0 {
			return false
		}
	}
	return true
}

func (o CODDTest) runIndependent(ctx context.Context, exec *db.DB, gen *generator.Generator, tbl schema.Table, phi generator.Expr) Result {
	auxSQL := fmt.Sprintf("SELECT %s", buildExpr(phi))
	row := exec.QueryRowContext(ctx, auxSQL)
	var auxVal sql.RawBytes
	if err := row.Scan(&auxVal); err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL}, Err: err}
	}
	mapped := buildLiteralFromBytes(auxVal, schema.TypeBool)
	query := &generator.SelectQuery{
		Items: gen.GenerateSelectList([]schema.Table{tbl}),
		From:  generator.FromClause{BaseTable: tbl.Name},
		Where: phi,
	}

	folded := query.Clone()
	folded.Where = mapped

	origSig, err := exec.QuerySignature(ctx, query.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{query.SQLString()}, Err: err}
	}
	foldSig, err := exec.QuerySignature(ctx, folded.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{folded.SQLString()}, Err: err}
	}
	if origSig != foldSig {
		expectedExplain, expectedExplainErr := explainSQL(ctx, exec, query.SignatureSQL())
		actualExplain, actualExplainErr := explainSQL(ctx, exec, folded.SignatureSQL())
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{query.SQLString(), folded.SQLString(), auxSQL},
			Expected: fmt.Sprintf("cnt=%d checksum=%d", origSig.Count, origSig.Checksum),
			Actual:   fmt.Sprintf("cnt=%d checksum=%d", foldSig.Count, foldSig.Checksum),
			Details: map[string]any{
				"replay_kind":          "signature",
				"replay_expected_sql":  query.SignatureSQL(),
				"replay_actual_sql":    folded.SignatureSQL(),
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{query.SQLString(), folded.SQLString(), auxSQL}}
}

func (o CODDTest) runDependent(ctx context.Context, exec *db.DB, gen *generator.Generator, tbl schema.Table, phi generator.Expr, cols []generator.ColumnRef) Result {
	colNames := make([]string, 0, len(cols))
	for _, col := range cols {
		colNames = append(colNames, fmt.Sprintf("%s.%s", tbl.Name, col.Name))
	}

	auxSQL := fmt.Sprintf("SELECT %s, %s AS v FROM %s LIMIT 50", strings.Join(colNames, ", "), buildExpr(phi), tbl.Name)
	rows, err := exec.QueryContext(ctx, auxSQL)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL}, Err: err}
	}
	defer util.CloseWithErr(rows, "coddtest rows")

	caseExpr := generator.CaseExpr{}
	for rows.Next() {
		values := make([]sql.RawBytes, len(colNames)+1)
		scanArgs := make([]any, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL}, Err: err}
		}

		var cond generator.Expr
		for i, col := range cols {
			val := buildLiteralFromBytes(values[i], col.Type)
			comp := generator.BinaryExpr{Left: generator.ColumnExpr{Ref: col}, Op: "<=>", Right: val}
			if cond == nil {
				cond = comp
			} else {
				cond = generator.BinaryExpr{Left: cond, Op: "AND", Right: comp}
			}
		}
		resultVal := buildLiteralFromBytes(values[len(values)-1], schema.TypeBool)
		caseExpr.Whens = append(caseExpr.Whens, generator.CaseWhen{When: cond, Then: resultVal})
	}

	if len(caseExpr.Whens) == 0 {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL}}
	}
	caseExpr.Else = generator.LiteralExpr{Value: nil}

	total, err := exec.QueryCount(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl.Name))
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL}, Err: err}
	}
	if total > int64(len(caseExpr.Whens)) {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL}}
	}

	query := &generator.SelectQuery{
		Items: gen.GenerateSelectList([]schema.Table{tbl}),
		From:  generator.FromClause{BaseTable: tbl.Name},
		Where: phi,
	}
	folded := query.Clone()
	folded.Where = caseExpr

	origSig, err := exec.QuerySignature(ctx, query.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{query.SQLString()}, Err: err}
	}
	foldSig, err := exec.QuerySignature(ctx, folded.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{folded.SQLString()}, Err: err}
	}
	if origSig != foldSig {
		expectedExplain, expectedExplainErr := explainSQL(ctx, exec, query.SignatureSQL())
		actualExplain, actualExplainErr := explainSQL(ctx, exec, folded.SignatureSQL())
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{query.SQLString(), folded.SQLString(), auxSQL},
			Expected: fmt.Sprintf("cnt=%d checksum=%d", origSig.Count, origSig.Checksum),
			Actual:   fmt.Sprintf("cnt=%d checksum=%d", foldSig.Count, foldSig.Checksum),
			Details: map[string]any{
				"replay_kind":          "signature",
				"replay_expected_sql":  query.SignatureSQL(),
				"replay_actual_sql":    folded.SignatureSQL(),
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{query.SQLString(), folded.SQLString(), auxSQL}}
}

func buildLiteralFromBytes(b sql.RawBytes, colType schema.ColumnType) generator.LiteralExpr {
	if b == nil {
		return generator.LiteralExpr{Value: nil}
	}
	text := string(b)
	switch colType {
	case schema.TypeInt, schema.TypeBigInt:
		if v, err := strconv.ParseInt(text, 10, 64); err == nil {
			return generator.LiteralExpr{Value: v}
		}
		return generator.LiteralExpr{Value: text}
	case schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
		// Preserve exact formatting to avoid float rounding mismatches in CASE mapping.
		return generator.LiteralExpr{Value: text}
	case schema.TypeBool:
		if text == "1" || strings.EqualFold(text, "true") {
			return generator.LiteralExpr{Value: 1}
		}
		if text == "0" || strings.EqualFold(text, "false") {
			return generator.LiteralExpr{Value: 0}
		}
		return generator.LiteralExpr{Value: text}
	default:
		return generator.LiteralExpr{Value: text}
	}
}
