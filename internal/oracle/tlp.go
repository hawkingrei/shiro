package oracle

import (
	"context"
	"fmt"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

// TLP implements the TLP oracle.
//
// Ternary Logic Partitioning (TLP) splits a predicate P into:
//
//	P, NOT P, and P IS NULL
//
// and checks that UNION ALL of these partitions preserves the original result.
// Any mismatch indicates a potential optimizer or execution bug.
type TLP struct{}

// Name returns the oracle identifier.
func (o TLP) Name() string { return "TLP" }

const tlpBuildMaxTries = 10

// Run builds a query, computes its signature, then compares against the TLP union.
// It only uses deterministic, simple predicates to reduce false positives.
//
// Example:
//
//	Q:     SELECT * FROM t WHERE a > 10
//	Q_tlp: SELECT * FROM t WHERE a > 10           -- P
//	       UNION ALL
//	       SELECT * FROM t WHERE NOT (a > 10)     -- NOT P
//	       UNION ALL
//	       SELECT * FROM t WHERE (a > 10) IS NULL -- P IS NULL
//
// The signatures of Q and Q_tlp (the UNION ALL of all three partitions) must match.
func (o TLP) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	policy := predicatePolicyFor(gen)
	policy.allowNot = true
	policy.allowIsNull = true
	spec := QuerySpec{
		Oracle:          "tlp",
		PredicatePolicy: policy,
		PredicateGuard:  true,
		MaxTries:        tlpBuildMaxTries,
		Constraints: generator.SelectQueryConstraints{
			RequireWhere:         true,
			PredicateMode:        generator.PredicateModeSimple,
			RequireDeterministic: true,
			DisallowLimit:        true,
			DisallowSetOps:       true,
		},
		SkipReasonOverrides: map[string]string{
			"constraint:nondeterministic": "tlp:nondeterministic",
			"constraint:predicate_guard":  "tlp:predicate_guard",
			"constraint:limit":            "tlp:limit",
			"constraint:set_ops":          "tlp:set_ops",
		},
	}
	query, details := buildQueryWithSpec(gen, spec)
	if query == nil || query.Where == nil {
		return Result{OK: true, Oracle: o.Name(), Details: details}
	}
	tlpNormalizeUsingRefs(gen, query)
	if reason := tlpSkipReason(query); reason != "" {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": reason}}
	}
	if skipReason, reason := signaturePrecheck(query, state, "tlp"); skipReason != "" {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{
			"skip_reason":     skipReason,
			"precheck_reason": reason,
		}}
	}

	base := query.Clone()
	base.Where = nil
	ensureTLPOrderBy(base)
	origSig, err := exec.QuerySignature(ctx, base.SignatureSQL())
	if err != nil {
		if code, ok := isWhitelistedSQLError(err); ok {
			return Result{OK: true, Oracle: o.Name(), SQL: []string{base.SQLString()}, Details: map[string]any{"skip_reason": fmt.Sprintf("tlp:sql_error_%d", code)}}
		}
		details := map[string]any{"error_reason": "tlp:base_signature_error"}
		if code, ok := mysqlErrCode(err); ok {
			details["error_code"] = int(code)
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{base.SQLString()}, Err: err, Details: details}
	}

	q1 := base.Clone()
	q2 := base.Clone()
	q3 := base.Clone()

	// The partition queries inherit ORDER BY from base.Clone(), but ORDER BY is not
	// allowed in UNION ALL subqueries (e.g., MySQL error 1221). Keep ORDER BY only
	// on the combined query via ensureTLPOrderBy(base).
	q1.OrderBy = nil
	q2.OrderBy = nil
	q3.OrderBy = nil
	q1.Where = query.Where
	q2.Where = generator.UnaryExpr{Op: "NOT", Expr: query.Where}
	q3.Where = generator.BinaryExpr{Left: query.Where, Op: "IS", Right: generator.LiteralExpr{Value: nil}}

	unionSQL := fmt.Sprintf("%sSELECT %s FROM (%s UNION ALL %s UNION ALL %s) u", buildWith(query), signatureColumns(query), q1.SQLString(), q2.SQLString(), q3.SQLString())
	unionSig, err := exec.QuerySignature(ctx, unionSQL)
	if err != nil {
		if code, ok := isWhitelistedSQLError(err); ok {
			return Result{OK: true, Oracle: o.Name(), SQL: []string{unionSQL}, Details: map[string]any{"skip_reason": fmt.Sprintf("tlp:sql_error_%d", code)}}
		}
		details := map[string]any{"error_reason": "tlp:union_signature_error"}
		if code, ok := mysqlErrCode(err); ok {
			details["error_code"] = int(code)
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{unionSQL}, Err: err, Details: details}
	}

	if origSig != unionSig {
		expectedExplain, expectedExplainErr := explainSQL(ctx, exec, base.SignatureSQL())
		actualExplain, actualExplainErr := explainSQL(ctx, exec, unionSQL)
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{base.SQLString(), unionSQL},
			Expected: fmt.Sprintf("cnt=%d checksum=%d", origSig.Count, origSig.Checksum),
			Actual:   fmt.Sprintf("cnt=%d checksum=%d", unionSig.Count, unionSig.Checksum),
			Details: map[string]any{
				"replay_kind":          "signature",
				"replay_expected_sql":  base.SignatureSQL(),
				"replay_actual_sql":    unionSQL,
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{base.SQLString(), unionSQL}}
}

func signatureColumns(query *generator.SelectQuery) string {
	aliases := query.ColumnAliases()
	cols := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		cols = append(cols, fmt.Sprintf("u.%s", alias))
	}
	if len(cols) == 0 {
		return "COUNT(*) AS cnt, 0 AS checksum"
	}
	return fmt.Sprintf("COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum", strings.Join(cols, ", "))
}

func ensureTLPOrderBy(query *generator.SelectQuery) {
	if query == nil || len(query.OrderBy) > 0 {
		return
	}
	cols := tlpOrderColumns(query.Items)
	if len(cols) == 0 {
		return
	}
	orderBy := make([]generator.OrderBy, 0, len(cols))
	for _, col := range cols {
		orderBy = append(orderBy, generator.OrderBy{Expr: generator.ColumnExpr{Ref: col}})
	}
	query.OrderBy = orderBy
}

func tlpSkipReason(query *generator.SelectQuery) string {
	if query == nil {
		return ""
	}
	if query.Limit != nil {
		return "tlp:limit"
	}
	if len(query.SetOps) > 0 {
		return "tlp:set_ops"
	}
	return ""
}

func tlpNormalizeUsingRefs(gen *generator.Generator, query *generator.SelectQuery) {
	if query == nil || gen == nil {
		return
	}
	columnsByTable := tlpColumnsByTable(gen, query)
	preferred := tlpRewriteUsingJoins(query, columnsByTable)
	if len(preferred) == 0 {
		return
	}
	query.Where = tlpRewriteExpr(query.Where, preferred, gen)
	for i := range query.Items {
		query.Items[i].Expr = tlpRewriteExpr(query.Items[i].Expr, preferred, gen)
	}
	for i := range query.GroupBy {
		query.GroupBy[i] = tlpRewriteExpr(query.GroupBy[i], preferred, gen)
	}
	if query.Having != nil {
		query.Having = tlpRewriteExpr(query.Having, preferred, gen)
	}
	for i := range query.OrderBy {
		query.OrderBy[i].Expr = tlpRewriteExpr(query.OrderBy[i].Expr, preferred, gen)
	}
	for i := range query.From.Joins {
		if query.From.Joins[i].On != nil {
			query.From.Joins[i].On = tlpRewriteExpr(query.From.Joins[i].On, preferred, gen)
		}
	}
}

func tlpRewriteExpr(expr generator.Expr, preferred map[string]string, gen *generator.Generator) generator.Expr {
	switch e := expr.(type) {
	case nil:
		return nil
	case generator.ColumnExpr:
		if e.Ref.Table == "" {
			if table, ok := preferred[e.Ref.Name]; ok && table != "" {
				e.Ref.Table = table
			}
		}
		return e
	case generator.LiteralExpr, generator.ParamExpr:
		return e
	case generator.UnaryExpr:
		e.Expr = tlpRewriteExpr(e.Expr, preferred, gen)
		return e
	case generator.BinaryExpr:
		e.Left = tlpRewriteExpr(e.Left, preferred, gen)
		e.Right = tlpRewriteExpr(e.Right, preferred, gen)
		return e
	case generator.FuncExpr:
		for i := range e.Args {
			e.Args[i] = tlpRewriteExpr(e.Args[i], preferred, gen)
		}
		return e
	case generator.CaseExpr:
		for i := range e.Whens {
			e.Whens[i].When = tlpRewriteExpr(e.Whens[i].When, preferred, gen)
			e.Whens[i].Then = tlpRewriteExpr(e.Whens[i].Then, preferred, gen)
		}
		if e.Else != nil {
			e.Else = tlpRewriteExpr(e.Else, preferred, gen)
		}
		return e
	case generator.SubqueryExpr:
		tlpNormalizeUsingRefs(gen, e.Query)
		return e
	case generator.ExistsExpr:
		tlpNormalizeUsingRefs(gen, e.Query)
		return e
	case generator.InExpr:
		e.Left = tlpRewriteExpr(e.Left, preferred, gen)
		for i := range e.List {
			e.List[i] = tlpRewriteExpr(e.List[i], preferred, gen)
		}
		return e
	case generator.WindowExpr:
		for i := range e.Args {
			e.Args[i] = tlpRewriteExpr(e.Args[i], preferred, gen)
		}
		for i := range e.PartitionBy {
			e.PartitionBy[i] = tlpRewriteExpr(e.PartitionBy[i], preferred, gen)
		}
		for i := range e.OrderBy {
			e.OrderBy[i].Expr = tlpRewriteExpr(e.OrderBy[i].Expr, preferred, gen)
		}
		return e
	case generator.GroupByOrdinalExpr:
		if e.Expr != nil {
			e.Expr = tlpRewriteExpr(e.Expr, preferred, gen)
		}
		return e
	default:
		return e
	}
}

func tlpColumnsByTable(gen *generator.Generator, query *generator.SelectQuery) map[string]map[string]struct{} {
	out := make(map[string]map[string]struct{})
	if gen == nil || query == nil {
		return out
	}
	for _, tbl := range gen.TablesForQueryScope(query) {
		if tbl.Name == "" || len(tbl.Columns) == 0 {
			continue
		}
		cols := make(map[string]struct{}, len(tbl.Columns))
		for _, col := range tbl.Columns {
			cols[col.Name] = struct{}{}
		}
		out[tbl.Name] = cols
	}
	return out
}

func tlpRewriteUsingJoins(query *generator.SelectQuery, columnsByTable map[string]map[string]struct{}) map[string]string {
	preferred := make(map[string]string)
	if query == nil {
		return preferred
	}
	visible := make([]string, 0, 1+len(query.From.Joins))
	if query.From.BaseTable != "" {
		visible = append(visible, query.From.BaseTable)
	}
	for i := range query.From.Joins {
		join := &query.From.Joins[i]
		if len(join.Using) > 0 {
			for _, col := range join.Using {
				leftTable := tlpPickLeftTable(col, visible, columnsByTable)
				if leftTable == "" && len(visible) > 0 {
					leftTable = visible[0]
				}
				if _, ok := preferred[col]; !ok && leftTable != "" {
					preferred[col] = leftTable
				}
				if leftTable != "" && join.Table != "" {
					cond := generator.BinaryExpr{
						Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: leftTable, Name: col}},
						Op:    "=",
						Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: join.Table, Name: col}},
					}
					if join.On == nil {
						join.On = cond
					} else {
						join.On = generator.BinaryExpr{Left: join.On, Op: "AND", Right: cond}
					}
				}
			}
			join.Using = nil
		}
		if join.Table != "" {
			visible = append(visible, join.Table)
		}
	}
	return preferred
}

func tlpPickLeftTable(col string, visible []string, columnsByTable map[string]map[string]struct{}) string {
	for _, tbl := range visible {
		if cols, ok := columnsByTable[tbl]; ok {
			if _, ok := cols[col]; ok {
				return tbl
			}
		}
	}
	return ""
}

func tlpOrderColumns(items []generator.SelectItem) []generator.ColumnRef {
	cols := make([]generator.ColumnRef, 0, TLPMaxOrderByCols)
	seen := make(map[string]struct{}, TLPMaxOrderByCols)
	for _, item := range items {
		for _, col := range item.Expr.Columns() {
			key := col.Table + "." + col.Name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			cols = append(cols, col)
			if len(cols) >= TLPMaxOrderByCols {
				return cols
			}
		}
	}
	return cols
}
