package oracle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/oracle/impo"
	"shiro/internal/schema"

	"github.com/go-sql-driver/mysql"
)

// Impo implements the Pinolo-style implication oracle.
type Impo struct{}

// Name returns the oracle identifier.
func (o Impo) Name() string { return "Impo" }

const impoSeedBuildRetries = 10

// Run generates a seed query, applies approximate mutations, and checks
// implication relations between the base and mutated results.
func (o Impo) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	metrics := map[string]int64{"impo_total": 1}
	if !state.HasTables() {
		return impoSkip(o.Name(), metrics, "no_tables")
	}
	origAggregates := gen.Config.Features.Aggregates
	gen.Config.Features.Aggregates = false
	defer func() {
		gen.Config.Features.Aggregates = origAggregates
	}()
	seedQuery, seedSkipReason := pickImpoSeedQuery(gen, state, metrics)
	if seedQuery == nil {
		return impoSkip(o.Name(), metrics, seedSkipReason)
	}
	seedSQL := seedQuery.SQLString()
	initSQL, err := impo.InitWithOptions(seedSQL, impo.InitOptions{
		DisableStage1: gen.Config.Oracles.ImpoDisableStage1,
		KeepLRJoin:    gen.Config.Oracles.ImpoKeepLRJoin,
	})
	if err != nil {
		if errors.Is(err, impo.ErrWithClause) {
			return impoSkip(o.Name(), metrics, "with_clause")
		}
		return impoSkip(o.Name(), metrics, "init_failed")
	}

	maxRows := gen.Config.Oracles.ImpoMaxRows
	if maxRows <= 0 {
		maxRows = gen.Config.MaxDataDumpRows
	}
	if shouldPrecheckRows(initSQL) {
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS impo_base", initSQL)
		count, err := exec.QueryCount(ctx, countSQL)
		if err == nil && int(count) > maxRows {
			metrics["impo_trunc"]++
			return impoSkip(o.Name(), metrics, "base_row_precheck")
		}
	}
	baseRows, baseTruncated, err := queryRowSet(ctx, exec, initSQL, maxRows)
	if err != nil {
		if IsSchemaColumnMissingErr(err) {
			details := map[string]any{
				"impo_seed_sql":      seedSQL,
				"impo_init_sql":      initSQL,
				"impo_base_exec_err": err.Error(),
				"replay_sql":         initSQL,
			}
			var mysqlErr *mysql.MySQLError
			if errors.As(err, &mysqlErr) {
				details["impo_base_exec_err_code"] = int(mysqlErr.Number)
			}
			return Result{
				OK:       false,
				Oracle:   o.Name(),
				SQL:      []string{initSQL},
				Expected: "base_exec_success",
				Actual:   fmt.Sprintf("base_exec_error: %s", err.Error()),
				Details:  details,
				Metrics:  metrics,
				Err:      err,
			}
		}
		if isTidbRowidErr(err) {
			details := map[string]any{
				"impo_seed_sql":      seedSQL,
				"impo_init_sql":      initSQL,
				"impo_base_exec_err": err.Error(),
				"replay_sql":         initSQL,
			}
			var mysqlErr *mysql.MySQLError
			if errors.As(err, &mysqlErr) {
				details["impo_base_exec_err_code"] = int(mysqlErr.Number)
			}
			return Result{
				OK:       false,
				Oracle:   o.Name(),
				SQL:      []string{initSQL},
				Expected: "base_exec_success",
				Actual:   fmt.Sprintf("base_exec_error: %s", err.Error()),
				Details:  details,
				Metrics:  metrics,
				Err:      err,
			}
		}
		return impoSkipErr(o.Name(), metrics, "base_exec_failed", initSQL, err)
	}
	if baseTruncated {
		metrics["impo_trunc"]++
		return impoSkip(o.Name(), metrics, "base_truncated")
	}

	mutations := impo.MutateAll(initSQL, gen.Rand.Int63())
	if mutations.Err != nil {
		return impoSkip(o.Name(), metrics, "mutate_failed")
	}
	if len(mutations.MutateUnits) == 0 {
		return impoSkip(o.Name(), metrics, "no_mutations")
	}
	impoMutationCounts := make(map[string]int64)
	impoMutationExecCounts := make(map[string]int64)
	for _, unit := range mutations.MutateUnits {
		if unit == nil || unit.Name == "" {
			continue
		}
		impoMutationCounts[unit.Name]++
	}
	units := mutations.MutateUnits
	maxMutations := gen.Config.Oracles.ImpoMaxMutations
	if maxMutations > 0 && len(units) > maxMutations {
		gen.Rand.Shuffle(len(units), func(i, j int) {
			units[i], units[j] = units[j], units[i]
		})
		units = units[:maxMutations]
	}
	timeout := time.Duration(gen.Config.Oracles.ImpoTimeoutMs) * time.Millisecond
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for _, unit := range units {
		if !deadline.IsZero() && time.Now().After(deadline) {
			return impoSkip(o.Name(), metrics, "mutation_timeout")
		}
		if unit.Err != nil || unit.SQL == "" {
			continue
		}
		impoMutationExecCounts[unit.Name]++
		if shouldPrecheckRows(unit.SQL) {
			countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS impo_mut", unit.SQL)
			count, err := exec.QueryCount(ctx, countSQL)
			if err == nil && int(count) > maxRows {
				metrics["impo_trunc"]++
				continue
			}
		}
		mutRows, mutTruncated, err := queryRowSet(ctx, exec, unit.SQL, maxRows)
		if err != nil {
			if IsSchemaColumnMissingErr(err) {
				return Result{
					OK:       false,
					Oracle:   o.Name(),
					SQL:      []string{initSQL, unit.SQL},
					Expected: "mut_exec_success",
					Actual:   fmt.Sprintf("mut_exec_error: %s", err.Error()),
					Details: map[string]any{
						"impo_seed_sql":    seedSQL,
						"impo_init_sql":    initSQL,
						"impo_mutated_sql": unit.SQL,
						"replay_sql":       unit.SQL,
					},
					Metrics: metrics,
					Err:     err,
				}
			}
			continue
		}
		if mutTruncated {
			metrics["impo_trunc"]++
			continue
		}
		cmp, err := compareRowSets(baseRows, mutRows)
		if err != nil {
			continue
		}
		if implicationOK(unit.IsUpper, cmp) {
			continue
		}
		expectedExplain, expectedExplainErr := explainSQL(ctx, exec, initSQL)
		actualExplain, actualExplainErr := explainSQL(ctx, exec, unit.SQL)
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{initSQL, unit.SQL},
			Expected: fmt.Sprintf("implication=%s", implicationExpected(unit.IsUpper)),
			Actual:   fmt.Sprintf("cmp=%s", cmpString(cmp)),
			Details: map[string]any{
				"impo_seed_sql":        seedSQL,
				"impo_init_sql":        initSQL,
				"impo_mutated_sql":     unit.SQL,
				"impo_mutation":        unit.Name,
				"impo_is_upper":        unit.IsUpper,
				"replay_kind":          "impo_contains",
				"replay_expected_sql":  initSQL,
				"replay_actual_sql":    unit.SQL,
				"replay_impo_is_upper": unit.IsUpper,
				"replay_max_rows":      maxRows,
				"expected_explain":     expectedExplain,
				"actual_explain":       actualExplain,
				"expected_explain_err": errString(expectedExplainErr),
				"actual_explain_err":   errString(actualExplainErr),
			},
			Metrics: metrics,
		}
	}
	return Result{OK: true, Oracle: o.Name(), Metrics: metrics, Details: map[string]any{
		"impo_mutation_counts":      impoMutationCounts,
		"impo_mutation_exec_counts": impoMutationExecCounts,
	}}
}

func implicationOK(isUpper bool, cmp int) bool {
	if cmp == 0 {
		return true
	}
	if isUpper {
		return cmp == -1
	}
	return cmp == 1
}

func implicationExpected(isUpper bool) string {
	if isUpper {
		return "base_subset_of_mutated"
	}
	return "mutated_subset_of_base"
}

func pickImpoSeedQuery(gen *generator.Generator, state *schema.State, metrics map[string]int64) (*generator.SelectQuery, string) {
	sawEmpty := false
	lastGuardrailReason := ""
	for attempt := 0; attempt < impoSeedBuildRetries; attempt++ {
		candidate := gen.GenerateSelectQuery()
		if candidate == nil {
			sawEmpty = true
			continue
		}
		if !queryDeterministic(candidate) {
			lastGuardrailReason = "nondeterministic"
			continue
		}
		if hasAggregate(candidate) {
			lastGuardrailReason = "aggregate_query"
			continue
		}
		candidate = candidate.Clone()
		rewriteUsingToOn(candidate, state)
		if containsScalarSubquery(candidate) {
			lastGuardrailReason = "scalar_subquery"
			continue
		}
		if hasPlanCacheHintsOrVars(candidate.SQLString()) {
			lastGuardrailReason = "plan_cache_hint_or_var"
			continue
		}
		sanitized := sanitizeQueryColumns(candidate, state)
		if ok, _ := queryColumnsValid(candidate, state, nil); !ok {
			lastGuardrailReason = "invalid_columns"
			continue
		}
		if sanitized {
			metrics["impo_sanitize"] = 1
		}
		return candidate, ""
	}
	return nil, impoSeedSkipReason(sawEmpty, lastGuardrailReason)
}

func impoSeedSkipReason(sawEmpty bool, lastGuardrailReason string) string {
	reason := strings.TrimSpace(lastGuardrailReason)
	if reason != "" {
		return "seed_guardrail:" + reason
	}
	if sawEmpty {
		return "empty_query"
	}
	return "seed_guardrail"
}

func cmpString(cmp int) string {
	switch cmp {
	case -1:
		return "mutated_contains_base"
	case 0:
		return "equal"
	case 1:
		return "base_contains_mutated"
	default:
		return "incomparable"
	}
}

func impoSkip(name string, metrics map[string]int64, reason string) Result {
	metrics["impo_skip"]++
	return Result{
		OK:     true,
		Oracle: name,
		Details: map[string]any{
			"impo_skip_reason": reason,
			"skip_reason":      "impo:" + reason,
		},
		Metrics: metrics,
	}
}

func impoSkipErr(name string, metrics map[string]int64, reason string, sqlText string, err error) Result {
	metrics["impo_skip"]++
	details := map[string]any{
		"impo_skip_reason": reason,
		"skip_reason":      "impo:" + reason,
	}
	if strings.TrimSpace(sqlText) != "" {
		details["impo_init_sql"] = sqlText
	}
	if err != nil {
		details["impo_base_exec_err"] = err.Error()
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		details["impo_base_exec_err_code"] = int(mysqlErr.Number)
	} else {
		details["impo_base_exec_err_code"] = 0
	}
	return Result{
		OK:      true,
		Oracle:  name,
		Details: details,
		Metrics: metrics,
	}
}

func shouldPrecheckRows(query string) bool {
	trimmed := strings.TrimSpace(query)
	return !strings.HasPrefix(strings.ToUpper(trimmed), "WITH ")
}

func rewriteUsingToOn(query *generator.SelectQuery, state *schema.State) {
	if query == nil || state == nil {
		return
	}
	leftTables := make([]schema.Table, 0, len(query.From.Joins)+1)
	if base, ok := state.TableByName(query.From.BaseTable); ok {
		leftTables = append(leftTables, base)
	}
	for i, join := range query.From.Joins {
		if len(join.Using) == 0 {
			if tbl, ok := state.TableByName(join.Table); ok {
				leftTables = append(leftTables, tbl)
			}
			continue
		}
		right, ok := state.TableByName(join.Table)
		if !ok {
			continue
		}
		var on generator.Expr
		for _, name := range join.Using {
			leftRef := generator.ColumnRef{}
			for li := len(leftTables) - 1; li >= 0; li-- {
				if _, ok := leftTables[li].ColumnByName(name); ok {
					leftRef = generator.ColumnRef{Table: leftTables[li].Name, Name: name}
					break
				}
			}
			if leftRef.Table == "" {
				continue
			}
			rightRef := generator.ColumnRef{Table: right.Name, Name: name}
			eq := generator.BinaryExpr{
				Left:  generator.ColumnExpr{Ref: leftRef},
				Op:    "=",
				Right: generator.ColumnExpr{Ref: rightRef},
			}
			if on == nil {
				on = eq
			} else {
				on = generator.BinaryExpr{Left: on, Op: "AND", Right: eq}
			}
		}
		if on != nil {
			join.On = on
			join.Using = nil
			query.From.Joins[i] = join
		}
		leftTables = append(leftTables, right)
	}
}

func containsScalarSubquery(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	for _, item := range query.Items {
		if exprHasScalarSubquery(item.Expr) {
			return true
		}
	}
	if exprHasScalarSubquery(query.Where) {
		return true
	}
	for _, expr := range query.GroupBy {
		if exprHasScalarSubquery(expr) {
			return true
		}
	}
	if exprHasScalarSubquery(query.Having) {
		return true
	}
	for _, ob := range query.OrderBy {
		if exprHasScalarSubquery(ob.Expr) {
			return true
		}
	}
	return false
}

func isTidbRowidErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "_tidb_rowid")
}

func hasPlanCacheHintsOrVars(sql string) bool {
	upper := strings.ToUpper(sql)
	if strings.Contains(upper, "/*+") {
		return true
	}
	if strings.Contains(upper, "SET_VAR(") {
		return true
	}
	if strings.Contains(upper, "SET @@") {
		return true
	}
	return false
}

func hasAggregate(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	if len(query.GroupBy) > 0 || query.Having != nil {
		return true
	}
	for _, item := range query.Items {
		if generator.ExprHasAggregate(item.Expr) {
			return true
		}
	}
	return false
}

func exprHasScalarSubquery(expr generator.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case generator.SubqueryExpr:
		return true
	case generator.UnaryExpr:
		return exprHasScalarSubquery(e.Expr)
	case generator.BinaryExpr:
		return exprHasScalarSubquery(e.Left) || exprHasScalarSubquery(e.Right)
	case generator.FuncExpr:
		for _, arg := range e.Args {
			if exprHasScalarSubquery(arg) {
				return true
			}
		}
		return false
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if exprHasScalarSubquery(w.When) || exprHasScalarSubquery(w.Then) {
				return true
			}
		}
		return exprHasScalarSubquery(e.Else)
	case generator.InExpr:
		if exprHasScalarSubquery(e.Left) {
			return true
		}
		for _, item := range e.List {
			if exprHasScalarSubquery(item) {
				return true
			}
		}
		return false
	case generator.WindowExpr:
		for _, arg := range e.Args {
			if exprHasScalarSubquery(arg) {
				return true
			}
		}
		for _, expr := range e.PartitionBy {
			if exprHasScalarSubquery(expr) {
				return true
			}
		}
		for _, ob := range e.OrderBy {
			if exprHasScalarSubquery(ob.Expr) {
				return true
			}
		}
		return false
	default:
		return false
	}
}
