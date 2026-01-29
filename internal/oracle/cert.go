package oracle

import (
	"context"
	"fmt"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
	"shiro/internal/util"
)

// CERT implements cardinality estimation restriction testing.
//
// It checks whether adding a restrictive predicate decreases (or at least does not
// drastically increase) the estimated row count in EXPLAIN. A large increase after
// adding a filter is suspicious and may indicate optimizer cardinality bugs.
type CERT struct {
	Tolerance   float64
	MinBaseRows float64
}

// Name returns the oracle identifier.
func (o CERT) Name() string { return "CERT" }

// Run compares EXPLAIN estRows for a base query and a restricted query.
// If restricted estRows exceeds base estRows by the configured tolerance,
// the case is flagged.
//
// Example:
//
//	Base:       EXPLAIN SELECT * FROM t WHERE a > 10
//	Restricted: EXPLAIN SELECT * FROM t WHERE a > 10 AND b = 5
//
// If restricted estRows is much larger, cardinality estimation is suspicious.
func (o CERT) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	if o.Tolerance == 0 {
		o.Tolerance = 0.1
	}
	builder := generator.NewSelectQueryBuilder(gen).
		RequireWhere().
		PredicateMode(generator.PredicateModeSimple).
		RequireDeterministic()
	var query *generator.SelectQuery
	var restrictPred generator.Expr
	lastReason := ""
	lastAttempts := 0
	var base *generator.SelectQuery
	var baseRows float64
	for i := 0; i < 5; i++ {
		query, lastReason, lastAttempts = builder.BuildWithReason()
		if query == nil || query.Where == nil {
			continue
		}
		scopeTables := gen.TablesForQueryScope(query)
		restrictPred = nil
		for j := 0; j < 8; j++ {
			pred := gen.GenerateSimpleColumnLiteralPredicate(scopeTables)
			if pred == nil {
				continue
			}
			if !gen.ValidateExprInQueryScope(pred, query) {
				continue
			}
			restrictPred = pred
			break
		}
		if restrictPred == nil || !isSimplePredicate(restrictPred) {
			restrictPred = nil
			lastReason = "constraint:restrict_predicate"
			continue
		}
		tables := tablesForQuery(query, state)
		if len(tables) == 0 && state != nil {
			tables = state.Tables
		}
		baseTables := certBaseTables(tables, state)
		if len(baseTables) == 0 {
			baseTables = tables
		}

		base = query.Clone()
		base.OrderBy = nil
		base.Limit = nil
		base.Having = nil
		base.GroupBy = nil
		base.Distinct = false
		base.Items = gen.GenerateSelectList(baseTables)
		var cteTable *schema.Table
		if len(base.With) > 0 {
			for _, tbl := range gen.TablesForQueryScope(base) {
				if state == nil {
					continue
				}
				if _, ok := state.TableByName(tbl.Name); ok {
					continue
				}
				cteTable = &tbl
				break
			}
		}
		if cteTable != nil {
			if util.Chance(gen.Rand, 50) {
				base.From = buildCTEBaseFromClause(*cteTable, baseTables)
				if !fromHasTable(base.From, cteTable.Name) {
					base.From = buildBaseFromClause(baseTables, cteTable)
				}
			} else {
				base.From = buildBaseFromClause(baseTables, cteTable)
				if !fromHasTable(base.From, cteTable.Name) {
					base.From = buildCTEBaseFromClause(*cteTable, baseTables)
				}
			}
		} else {
			base.From = buildBaseFromClause(baseTables, nil)
		}
		ensureFromHasPredicateTables(base, state)

		baseExplain := "EXPLAIN " + base.SQLString()
		rows, err := exec.QueryPlanRows(ctx, baseExplain)
		if err != nil {
			return Result{
				OK:     true,
				Oracle: o.Name(),
				SQL:    []string{baseExplain},
				Err:    err,
				Details: map[string]any{
					"error_reason": "cert:base_explain_error",
				},
			}
		}
		baseRows = rows
		if o.MinBaseRows > 0 && baseRows < o.MinBaseRows {
			lastReason = "constraint:base_rows_low"
			continue
		}
		break
	}
	if query == nil || query.Where == nil {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": builderSkipReason("cert", lastReason), "builder_reason": lastReason, "builder_attempts": lastAttempts}}
	}
	if restrictPred == nil {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": builderSkipReason("cert", lastReason), "builder_reason": lastReason, "builder_attempts": lastAttempts}}
	}
	if o.MinBaseRows > 0 && baseRows < o.MinBaseRows {
		return Result{OK: true, Oracle: o.Name(), Details: map[string]any{"skip_reason": builderSkipReason("cert", lastReason), "builder_reason": lastReason, "builder_attempts": lastAttempts}}
	}

	restricted := base.Clone()
	restricted.Where = generator.BinaryExpr{Left: base.Where, Op: "AND", Right: restrictPred}
	if o.MinBaseRows > 0 && baseRows < o.MinBaseRows {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{base.SQLString(), restricted.SQLString()}, Details: map[string]any{"skip_reason": "cert:base_rows_low"}}
	}
	restrictedExplain := "EXPLAIN " + restricted.SQLString()
	restrictedRows, err := exec.QueryPlanRows(ctx, restrictedExplain)
	if err != nil {
		return Result{
			OK:     true,
			Oracle: o.Name(),
			SQL:    []string{restrictedExplain},
			Err:    err,
			Details: map[string]any{
				"error_reason": "cert:restricted_explain_error",
			},
		}
	}

	if restrictedRows > baseRows*(1.0+o.Tolerance) {
		expectedExplain, expectedExplainErr := explainSQL(ctx, exec, base.SQLString())
		actualExplain, actualExplainErr := explainSQL(ctx, exec, restricted.SQLString())
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{base.SQLString(), restricted.SQLString()},
			Expected: fmt.Sprintf("restricted estRows <= %.2f", baseRows),
			Actual:   fmt.Sprintf("restricted estRows %.2f", restrictedRows),
			Details: map[string]any{
				"base_est_rows":        baseRows,
				"restricted_est_rows":  restrictedRows,
				"replay_kind":          "plan_rows",
				"replay_expected_sql":  base.SQLString(),
				"replay_actual_sql":    restricted.SQLString(),
				"replay_tolerance":     o.Tolerance,
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
			},
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{base.SQLString(), restricted.SQLString()}}
}

func certBaseTables(tables []schema.Table, state *schema.State) []schema.Table {
	if state == nil {
		return tables
	}
	out := make([]schema.Table, 0, len(tables))
	for _, tbl := range tables {
		if _, ok := state.TableByName(tbl.Name); ok {
			out = append(out, tbl)
		}
	}
	return out
}

func buildBaseFromClause(tables []schema.Table, cte *schema.Table) generator.FromClause {
	if len(tables) == 0 {
		return generator.FromClause{}
	}
	from := generator.FromClause{BaseTable: tables[0].Name}
	if len(tables) == 1 {
		if cte == nil {
			return from
		}
	}
	for i := 1; i < len(tables); i++ {
		left := tables[i-1]
		right := tables[i]
		leftCol, rightCol, ok := firstJoinPair(left, right)
		if !ok {
			continue
		}
		from.Joins = append(from.Joins, generator.Join{
			Type:  generator.JoinInner,
			Table: right.Name,
			On: generator.BinaryExpr{
				Left:  generator.ColumnExpr{Ref: leftCol},
				Op:    "=",
				Right: generator.ColumnExpr{Ref: rightCol},
			},
		})
	}
	if cte != nil && cte.Name != "" {
		left := tables[len(tables)-1]
		leftCol, rightCol, ok := firstJoinPair(left, *cte)
		if !ok {
			return from
		}
		from.Joins = append(from.Joins, generator.Join{
			Type:  generator.JoinInner,
			Table: cte.Name,
			On: generator.BinaryExpr{
				Left:  generator.ColumnExpr{Ref: leftCol},
				Op:    "=",
				Right: generator.ColumnExpr{Ref: rightCol},
			},
		})
	}
	return from
}

func firstJoinPair(left schema.Table, right schema.Table) (leftRef generator.ColumnRef, rightRef generator.ColumnRef, ok bool) {
	for _, lcol := range left.Columns {
		if !strings.HasPrefix(lcol.Name, "k") {
			continue
		}
		if rcol, ok := right.ColumnByName(lcol.Name); ok {
			if !compatibleJoinColumnType(lcol.Type, rcol.Type) {
				continue
			}
			return generator.ColumnRef{Table: left.Name, Name: lcol.Name, Type: lcol.Type},
				generator.ColumnRef{Table: right.Name, Name: rcol.Name, Type: rcol.Type},
				true
		}
	}
	for _, lcol := range left.Columns {
		for _, rcol := range right.Columns {
			if !compatibleJoinColumnType(lcol.Type, rcol.Type) {
				continue
			}
			return generator.ColumnRef{Table: left.Name, Name: lcol.Name, Type: lcol.Type},
				generator.ColumnRef{Table: right.Name, Name: rcol.Name, Type: rcol.Type},
				true
		}
	}
	return generator.ColumnRef{}, generator.ColumnRef{}, false
}

func fromHasTable(from generator.FromClause, name string) bool {
	if name == "" {
		return false
	}
	if from.BaseTable == name {
		return true
	}
	for _, join := range from.Joins {
		if join.Table == name {
			return true
		}
	}
	return false
}

func compatibleJoinColumnType(left schema.ColumnType, right schema.ColumnType) bool {
	if left == right {
		return true
	}
	leftCat := joinTypeCategory(left)
	rightCat := joinTypeCategory(right)
	return leftCat != 0 && leftCat == rightCat
}

func joinTypeCategory(typ schema.ColumnType) int {
	switch typ {
	case schema.TypeInt, schema.TypeBigInt, schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal, schema.TypeBool:
		return 1
	case schema.TypeVarchar:
		return 2
	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return 3
	default:
		return 0
	}
}

func ensureFromHasPredicateTables(query *generator.SelectQuery, state *schema.State) bool {
	if query == nil || query.Where == nil {
		return false
	}
	present := map[string]struct{}{}
	if query.From.BaseTable != "" {
		present[query.From.BaseTable] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if join.Table != "" {
			present[join.Table] = struct{}{}
		}
	}
	valid := map[string]struct{}{}
	for _, cte := range query.With {
		if cte.Name != "" {
			valid[cte.Name] = struct{}{}
		}
	}
	if state != nil {
		for _, tbl := range state.Tables {
			valid[tbl.Name] = struct{}{}
		}
	}
	changed := false
	for _, ref := range query.Where.Columns() {
		if ref.Table == "" {
			continue
		}
		if _, ok := present[ref.Table]; ok {
			continue
		}
		if _, ok := valid[ref.Table]; !ok {
			continue
		}
		query.From.Joins = append(query.From.Joins, generator.Join{
			Type:  generator.JoinCross,
			Table: ref.Table,
		})
		present[ref.Table] = struct{}{}
		changed = true
	}
	return changed
}

func buildCTEBaseFromClause(cte schema.Table, tables []schema.Table) generator.FromClause {
	from := generator.FromClause{BaseTable: cte.Name}
	for _, tbl := range tables {
		leftCol, rightCol, ok := firstJoinPair(cte, tbl)
		if !ok {
			continue
		}
		from.Joins = append(from.Joins, generator.Join{
			Type:  generator.JoinInner,
			Table: tbl.Name,
			On: generator.BinaryExpr{
				Left:  generator.ColumnExpr{Ref: leftCol},
				Op:    "=",
				Right: generator.ColumnExpr{Ref: rightCol},
			},
		})
	}
	return from
}
