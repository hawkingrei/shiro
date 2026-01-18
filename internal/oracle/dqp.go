package oracle

import (
	"context"
	"fmt"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

// DQP implements differential query plan testing.
//
// It runs the same query under different plan choices (hints or session variables)
// and compares result signatures (COUNT + checksum). Any mismatch suggests a plan-
// dependent correctness bug in the optimizer or execution engine.
type DQP struct {
	HintSets  []string
	Variables []string
}

func (o DQP) Name() string { return "DQP" }

// Run generates a join query, executes the base signature, then tries variants:
// - join hints (HASH_JOIN/MERGE_JOIN/INL_*)
// - join order hint
// - session variables toggling optimizer paths
// Differences in signature are reported with the hint/variable that triggered it.
//
// Example:
//   Base:  SELECT * FROM t1 JOIN t2 ON t1.id = t2.id
//   Hint:  SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t1 JOIN t2 ON t1.id = t2.id
// If the signatures differ, the plan choice affected correctness.
func (o DQP) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	query := gen.GenerateSelectQuery()
	if query == nil {
		return Result{OK: true, Oracle: o.Name()}
	}
	if len(query.With) > 0 {
		return Result{OK: true, Oracle: o.Name()}
	}
	if query.Limit != nil && len(query.OrderBy) == 0 {
		return Result{OK: true, Oracle: o.Name()}
	}
	if queryHasSubquery(query) {
		return Result{OK: true, Oracle: o.Name()}
	}
	if len(query.From.Joins) == 0 {
		return Result{OK: true, Oracle: o.Name()}
	}

	baseSQL := query.SQLString()
	baseSig, err := exec.QuerySignature(ctx, query.SignatureSQL())
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}, Err: err}
	}

	variants := buildDQPVariants(query, state, o.HintSets, o.Variables)
	for _, variant := range variants {
		if variant.setVar != "" {
			_, _ = exec.ExecContext(ctx, "SET SESSION "+variant.setVar)
		}
		variantSig, err := exec.QuerySignature(ctx, variant.signatureSQL)
		if variant.setVar != "" {
			resetVar(exec, ctx, variant.setVar)
		}
		if err != nil {
			continue
		}
		if variantSig != baseSig {
			details := map[string]any{
				"hint":                variant.hint,
				"replay_kind":         "signature",
				"replay_expected_sql": query.SignatureSQL(),
				"replay_actual_sql":   variant.signatureSQL,
			}
			if variant.setVar != "" {
				details["replay_set_var"] = variant.setVar
			}
			return Result{
				OK:       false,
				Oracle:   o.Name(),
				SQL:      []string{baseSQL, variant.sql},
				Expected: fmt.Sprintf("cnt=%d checksum=%d", baseSig.Count, baseSig.Checksum),
				Actual:   fmt.Sprintf("cnt=%d checksum=%d", variantSig.Count, variantSig.Checksum),
				Details:  details,
			}
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{baseSQL}}
}

type dqpVariant struct {
	sql          string
	signatureSQL string
	hint         string
	setVar       string
}

func buildDQPVariants(query *generator.SelectQuery, state *schema.State, hintSets []string, variables []string) []dqpVariant {
	tables := []string{query.From.BaseTable}
	for _, join := range query.From.Joins {
		tables = append(tables, join.Table)
	}

	hints := hintSets
	if len(hints) == 0 {
		hints = []string{
			"HASH_JOIN",
			"MERGE_JOIN",
			"INL_JOIN",
			"INL_HASH_JOIN",
			"INL_MERGE_JOIN",
		}
	}
	variants := make([]dqpVariant, 0, len(hints)+1)

	for _, hint := range hints {
		hintSQL := fmt.Sprintf("%s(%s)", hint, strings.Join(tables, ", "))
		variantSQL := injectHint(query, hintSQL)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: hintSQL})
	}
	if len(tables) > 1 {
		orderHint := fmt.Sprintf("JOIN_ORDER(%s)", strings.Join(tables, ", "))
		variantSQL := injectHint(query, orderHint)
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: orderHint})
	}

	if state != nil {
		for _, tbl := range tables {
			table, ok := state.TableByName(tbl)
			if !ok {
				continue
			}
			for _, col := range table.Columns {
				if !col.HasIndex {
					continue
				}
				hint := fmt.Sprintf("USE_INDEX(%s, idx_%s)", table.Name, col.Name)
				variantSQL := injectHint(query, hint)
				variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
				variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: hint})
			}
		}
	}

	for _, variable := range variables {
		setVar := strings.TrimSpace(variable)
		if setVar == "" {
			continue
		}
		variantSQL := query.SQLString()
		variantSig := fmt.Sprintf("SELECT COUNT(*) AS cnt, IFNULL(BIT_XOR(CRC32(CONCAT_WS('#', %s))),0) AS checksum FROM (%s) q", signatureSelectList(query), variantSQL)
		variants = append(variants, dqpVariant{sql: variantSQL, signatureSQL: variantSig, hint: "SET " + setVar, setVar: setVar})
	}
	return variants
}

func injectHint(query *generator.SelectQuery, hint string) string {
	base := query.SQLString()
	idx := strings.Index(strings.ToUpper(base), "SELECT")
	if idx == -1 {
		return base
	}
	return base[:idx+6] + " /*+ " + hint + " */" + base[idx+6:]
}

func signatureSelectList(query *generator.SelectQuery) string {
	aliases := query.ColumnAliases()
	cols := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		cols = append(cols, fmt.Sprintf("q.%s", alias))
	}
	if len(cols) == 0 {
		return "0"
	}
	return strings.Join(cols, ", ")
}

func resetVar(exec *db.DB, ctx context.Context, assignment string) {
	name := strings.SplitN(assignment, "=", 2)[0]
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	_, _ = exec.ExecContext(ctx, "SET SESSION "+name+"=DEFAULT")
}
