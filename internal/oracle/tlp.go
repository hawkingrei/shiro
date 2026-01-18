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
//	Q:  SELECT * FROM t WHERE a > 10
//	TLP: SELECT * FROM t WHERE a > 10
//	     UNION ALL SELECT * FROM t WHERE NOT (a > 10)
//	     UNION ALL SELECT * FROM t WHERE (a > 10) IS NULL
//
// The signatures must match.
//
//	Q:      SELECT * FROM t WHERE a > 10
//	Q_tlp:  SELECT * FROM t WHERE a > 10           -- P
//	        UNION ALL
//	        SELECT * FROM t WHERE NOT (a > 10)     -- NOT P
//	        UNION ALL
//	        SELECT * FROM t WHERE (a > 10) IS NULL -- P IS NULL
//
// The signatures of Q and Q_tlp (the UNION ALL of all three partitions) must match.
func (o TLP) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	query := gen.GenerateSelectQuery()
	if query == nil || query.Where == nil {
		return Result{OK: true, Oracle: o.Name()}
	}
	if query.Distinct || len(query.GroupBy) > 0 || query.Having != nil || query.Limit != nil || queryHasAggregate(query) || queryHasSubquery(query) {
		return Result{OK: true, Oracle: o.Name()}
	}
	if !query.Where.Deterministic() {
		return Result{OK: true, Oracle: o.Name()}
	}
	if gen.Config.Oracles.StrictPredicates && !isSimplePredicate(query.Where) {
		return Result{OK: true, Oracle: o.Name()}
	}

	origSig, err := exec.QuerySignature(ctx, query.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{query.SQLString()}, Err: err}
	}

	q1 := query.Clone()
	q2 := query.Clone()
	q3 := query.Clone()

	q1.With = nil
	q2.With = nil
	q3.With = nil

	q1.Where = query.Where
	q2.Where = generator.UnaryExpr{Op: "NOT", Expr: query.Where}
	q3.Where = generator.BinaryExpr{Left: query.Where, Op: "IS", Right: generator.LiteralExpr{Value: nil}}

	unionSQL := fmt.Sprintf("%sSELECT %s FROM (%s UNION ALL %s UNION ALL %s) u", buildWith(query), signatureColumns(query), q1.SQLString(), q2.SQLString(), q3.SQLString())
	unionSig, err := exec.QuerySignature(ctx, unionSQL)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{unionSQL}, Err: err}
	}

	if origSig != unionSig {
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{query.SQLString(), unionSQL},
			Expected: fmt.Sprintf("cnt=%d checksum=%d", origSig.Count, origSig.Checksum),
			Actual:   fmt.Sprintf("cnt=%d checksum=%d", unionSig.Count, unionSig.Checksum),
			Details: map[string]any{
				"replay_kind":         "signature",
				"replay_expected_sql": query.SignatureSQL(),
				"replay_actual_sql":   unionSQL,
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{query.SQLString(), unionSQL}}
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
