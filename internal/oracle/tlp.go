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
func (o TLP) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, _ *schema.State) Result {
	query := gen.GenerateSelectQueryWithConstraints(generator.SelectQueryConstraints{
		RequireWhere:  true,
		PredicateMode: generator.PredicateModeSimple,
	})
	if query == nil || query.Where == nil {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "tlp:no_where"}}
	}
	if !queryDeterministic(query) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "tlp:nondeterministic"}}
	}
	policy := predicatePolicyFor(gen)
	policy.allowIsNull = false
	if !predicateMatches(query.Where, policy) {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": "tlp:predicate_guard"}}
	}

	base := query.Clone()
	base.Where = nil
	ensureTLPOrderBy(base)
	origSig, err := exec.QuerySignature(ctx, base.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{base.SQLString()}, Err: err}
	}

	q1 := base.Clone()
	q2 := base.Clone()
	q3 := base.Clone()

	q1.Where = query.Where
	q2.Where = generator.UnaryExpr{Op: "NOT", Expr: query.Where}
	q3.Where = generator.BinaryExpr{Left: query.Where, Op: "IS", Right: generator.LiteralExpr{Value: nil}}

	unionSQL := fmt.Sprintf("%sSELECT %s FROM (%s UNION ALL %s UNION ALL %s) u", buildWith(query), signatureColumns(query), q1.SQLString(), q2.SQLString(), q3.SQLString())
	unionSig, err := exec.QuerySignature(ctx, unionSQL)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{unionSQL}, Err: err}
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
