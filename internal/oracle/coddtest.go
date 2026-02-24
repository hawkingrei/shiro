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
	baseTables := state.BaseTables()
	if len(baseTables) == 0 {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "coddtest:no_base_tables"}}
	}
	baseNames := make(map[string]struct{}, len(baseTables))
	for _, tbl := range baseTables {
		baseNames[tbl.Name] = struct{}{}
	}
	policy := predicatePolicyFor(gen)
	policy.allowIsNull = false
	queryGuard := func(query *generator.SelectQuery) (bool, string) {
		if query == nil {
			return false, "constraint:query_guard"
		}
		if len(query.With) > 0 {
			return false, "constraint:query_guard"
		}
		if !queryUsesOnlyBaseTables(query, baseNames) {
			return false, "constraint:query_guard"
		}
		if reason := coddtestPredicatePrecheckReason(state, query); reason != "" {
			return false, reason
		}
		return true, ""
	}
	predicateGuard := func(expr generator.Expr) bool {
		return predicateMatches(expr, policy)
	}
	constraints := generator.SelectQueryConstraints{
		RequireWhere:         true,
		PredicateMode:        generator.PredicateModeSimple,
		RequireDeterministic: true,
		DisallowAggregate:    true,
		MaxJoinCount:         2,
		MaxJoinCountSet:      true,
		QueryGuardReason:     queryGuard,
		PredicateGuard:       predicateGuard,
	}
	if profile := ProfileByName("CODDTest"); profile != nil {
		applyProfileToSpec(&constraints, profile)
		if profile.PredicateMode != nil {
			constraints.PredicateMode = *profile.PredicateMode
		}
	}
	builder := generator.NewSelectQueryBuilder(gen).WithConstraints(constraints)
	query, reason, attempts := builder.BuildWithReason()
	if query == nil || query.Where == nil {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": builderSkipReason("coddtest", reason), "builder_reason": reason, "builder_attempts": attempts}}
	}
	phi := query.Where
	if !phi.Deterministic() || exprHasSubquery(phi) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "coddtest:predicate_guard"}}
	}
	if !predicateMatches(phi, policy) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "coddtest:predicate_guard"}}
	}
	columns := phi.Columns()
	if len(columns) == 0 {
		return o.runIndependent(ctx, exec, gen, query, phi)
	}
	if !onlySupportedCODDColumns(columns) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "coddtest:type_guard"}}
	}
	if coddtestColumnsGuaranteedNonNull(state, columns) {
		return o.runDependent(ctx, exec, gen, query, phi, columns)
	}
	if !o.noNullsInQuery(ctx, exec, state, columns) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "coddtest:null_guard"}}
	}
	return o.runDependent(ctx, exec, gen, query, phi, columns)
}

func onlySupportedCODDColumns(columns []generator.ColumnRef) bool {
	if len(columns) == 0 {
		return false
	}
	for _, col := range columns {
		switch col.Type {
		case schema.TypeInt,
			schema.TypeBigInt,
			schema.TypeFloat,
			schema.TypeDouble,
			schema.TypeDecimal,
			schema.TypeVarchar,
			schema.TypeDate,
			schema.TypeDatetime,
			schema.TypeTimestamp,
			schema.TypeBool:
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

func (o CODDTest) noNullsInQuery(ctx context.Context, exec *db.DB, state *schema.State, columns []generator.ColumnRef) bool {
	if state == nil {
		return false
	}
	if len(columns) == 0 {
		return true
	}
	byTable := make(map[string][]generator.ColumnRef)
	for _, col := range columns {
		if col.Table == "" {
			return false
		}
		byTable[col.Table] = append(byTable[col.Table], col)
	}
	for table, cols := range byTable {
		tbl, ok := state.TableByName(table)
		if !ok {
			return false
		}
		if !o.noNullsInTable(ctx, exec, tbl, cols) {
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

func (o CODDTest) runIndependent(ctx context.Context, exec *db.DB, _ *generator.Generator, query *generator.SelectQuery, phi generator.Expr) Result {
	auxSQL := fmt.Sprintf("SELECT %s", buildExpr(phi))
	row := exec.QueryRowContext(ctx, auxSQL)
	var auxVal sql.RawBytes
	if err := row.Scan(&auxVal); err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL}, Err: err}
	}
	mapped := buildLiteralFromBytes(auxVal, schema.TypeBool)

	base := query.Clone()
	base.Where = phi
	folded := base.Clone()
	folded.Where = mapped

	origSig, err := exec.QuerySignature(ctx, base.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{base.SQLString()}, Err: err}
	}
	foldSig, err := exec.QuerySignature(ctx, folded.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{folded.SQLString()}, Err: err}
	}
	if origSig != foldSig {
		expectedExplain, expectedExplainErr := explainSQL(ctx, exec, base.SignatureSQL())
		actualExplain, actualExplainErr := explainSQL(ctx, exec, folded.SignatureSQL())
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{base.SQLString(), folded.SQLString(), auxSQL},
			Expected: fmt.Sprintf("cnt=%d checksum=%d", origSig.Count, origSig.Checksum),
			Actual:   fmt.Sprintf("cnt=%d checksum=%d", foldSig.Count, foldSig.Checksum),
			Details: map[string]any{
				"replay_kind":          "signature",
				"replay_expected_sql":  base.SignatureSQL(),
				"replay_actual_sql":    folded.SignatureSQL(),
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{base.SQLString(), folded.SQLString(), auxSQL}}
}

func (o CODDTest) runDependent(ctx context.Context, exec *db.DB, gen *generator.Generator, query *generator.SelectQuery, phi generator.Expr, cols []generator.ColumnRef) Result {
	caseWhenMax := coddtestCaseWhenMax(gen)
	colNames := make([]string, 0, len(cols))
	for _, col := range cols {
		colNames = append(colNames, fmt.Sprintf("%s.%s", col.Table, col.Name))
	}

	auxSQL := fmt.Sprintf("SELECT %s, %s AS v FROM %s LIMIT 50", strings.Join(colNames, ", "), buildExpr(phi), buildFrom(query))
	rows, err := exec.QueryContext(ctx, auxSQL)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL}, Err: err}
	}
	defer util.CloseWithErr(rows, "coddtest rows")

	seen := make(map[string]struct{})
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

		key := coddtestCaseKey(cols, values)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if len(caseExpr.Whens) >= caseWhenMax {
			break
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
	if err := rows.Err(); err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL}, Err: err}
	}

	if len(caseExpr.Whens) == 0 {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL}}
	}
	caseExpr.Else = generator.LiteralExpr{Value: nil}

	totalSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", buildFrom(query))
	total, err := exec.QueryCount(ctx, totalSQL)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL, totalSQL}, Err: err}
	}
	if total > int64(len(caseExpr.Whens)) {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{auxSQL, totalSQL}}
	}

	base := query.Clone()
	base.Where = phi
	folded := base.Clone()
	folded.Where = caseExpr

	origSig, err := exec.QuerySignature(ctx, base.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{base.SQLString()}, Err: err}
	}
	foldSig, err := exec.QuerySignature(ctx, folded.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{folded.SQLString()}, Err: err}
	}
	if origSig != foldSig {
		expectedExplain, expectedExplainErr := explainSQL(ctx, exec, base.SignatureSQL())
		actualExplain, actualExplainErr := explainSQL(ctx, exec, folded.SignatureSQL())
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{base.SQLString(), folded.SQLString(), auxSQL},
			Expected: fmt.Sprintf("cnt=%d checksum=%d", origSig.Count, origSig.Checksum),
			Actual:   fmt.Sprintf("cnt=%d checksum=%d", foldSig.Count, foldSig.Checksum),
			Details: map[string]any{
				"replay_kind":          "signature",
				"replay_expected_sql":  base.SignatureSQL(),
				"replay_actual_sql":    folded.SignatureSQL(),
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{base.SQLString(), folded.SQLString(), auxSQL}}
}

func coddtestCaseWhenMax(gen *generator.Generator) int {
	if gen == nil || gen.Config.Oracles.CODDCaseWhenMax <= 0 {
		return 2
	}
	return gen.Config.Oracles.CODDCaseWhenMax
}

func coddtestCaseKey(cols []generator.ColumnRef, values []sql.RawBytes) string {
	if len(cols) == 0 || len(values) == 0 {
		return ""
	}
	var b strings.Builder
	for i, col := range cols {
		b.WriteString(col.Table)
		b.WriteByte('.')
		b.WriteString(col.Name)
		b.WriteByte('=')
		if i >= len(values) || values[i] == nil {
			b.WriteString("NULL")
			b.WriteByte(';')
			continue
		}
		b.WriteString(strconv.Itoa(len(values[i])))
		b.WriteByte(':')
		b.Write(values[i])
		b.WriteByte(';')
	}
	return b.String()
}

func queryUsesOnlyBaseTables(query *generator.SelectQuery, baseNames map[string]struct{}) bool {
	if query == nil {
		return false
	}
	if query.From.BaseTable == "" {
		return false
	}
	if _, ok := baseNames[query.From.BaseTable]; !ok {
		return false
	}
	for _, join := range query.From.Joins {
		if join.Table == "" {
			return false
		}
		if _, ok := baseNames[join.Table]; !ok {
			return false
		}
	}
	return true
}

func coddtestPredicatePrecheckReason(state *schema.State, query *generator.SelectQuery) string {
	if query == nil || query.Where == nil {
		return "constraint:no_where"
	}
	columns := query.Where.Columns()
	if len(columns) == 0 {
		return ""
	}
	if !onlySupportedCODDColumns(columns) {
		return "constraint:type_guard"
	}
	if !coddtestColumnsGuaranteedNonNull(state, columns) {
		return "constraint:null_guard"
	}
	return ""
}

func coddtestColumnsGuaranteedNonNull(state *schema.State, columns []generator.ColumnRef) bool {
	if state == nil || len(columns) == 0 {
		return false
	}
	for _, col := range columns {
		if col.Table == "" || col.Name == "" {
			return false
		}
		tbl, ok := state.TableByName(col.Table)
		if !ok {
			return false
		}
		info, ok := tbl.ColumnByName(col.Name)
		if !ok {
			return false
		}
		if info.Nullable {
			return false
		}
	}
	return true
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
