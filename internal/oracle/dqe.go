package oracle

import (
	"context"
	"fmt"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

type DQE struct{}

func (o DQE) Name() string { return "DQE" }

func (o DQE) Run(ctx context.Context, exec *db.DB, gen *generator.Generator, state *schema.State) Result {
	if !state.HasTables() {
		return Result{OK: true, Oracle: o.Name()}
	}
	tbl := state.Tables[gen.Rand.Intn(len(state.Tables))]
	choice := gen.Rand.Intn(2)

	if choice == 0 {
		updateSQL, predicate, setExpr, colRef := gen.UpdateSQL(tbl)
		if updateSQL == "" || predicate == nil || setExpr == nil || colRef.Table == "" {
			return Result{OK: true, Oracle: o.Name()}
		}
		if !predicate.Deterministic() {
			return Result{OK: true, Oracle: o.Name()}
		}
		colSQL := fmt.Sprintf("%s.%s", colRef.Table, colRef.Name)
		setExprSQL := buildExpr(setExpr)
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s AND NOT (%s <=> %s)", tbl.Name, buildExpr(predicate), colSQL, setExprSQL)
		count, err := exec.QueryCount(ctx, countSQL)
		if err != nil {
			return Result{OK: true, Oracle: o.Name(), SQL: []string{countSQL}, Err: err}
		}
		res, err := exec.ExecContext(ctx, updateSQL)
		if err != nil {
			return Result{OK: true, Oracle: o.Name(), SQL: []string{updateSQL}, Err: err}
		}
		affected, _ := res.RowsAffected()
		if affected != count {
			return Result{
				OK:       false,
				Oracle:   o.Name(),
				SQL:      []string{updateSQL},
				Expected: fmt.Sprintf("rows affected=%d", count),
				Actual:   fmt.Sprintf("rows affected=%d", affected),
			}
		}
		return Result{OK: true, Oracle: o.Name(), SQL: []string{updateSQL, countSQL}}
	}

	deleteSQL, predicate := gen.DeleteSQL(tbl)
	if deleteSQL == "" || predicate == nil {
		return Result{OK: true, Oracle: o.Name()}
	}
	if !predicate.Deterministic() {
		return Result{OK: true, Oracle: o.Name()}
	}
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tbl.Name, buildExpr(predicate))
	count, err := exec.QueryCount(ctx, countSQL)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{countSQL}, Err: err}
	}
	res, err := exec.ExecContext(ctx, deleteSQL)
	if err != nil {
		return Result{OK: true, Oracle: o.Name(), SQL: []string{deleteSQL}, Err: err}
	}
	affected, _ := res.RowsAffected()
	if affected != count {
		return Result{
			OK:       false,
			Oracle:   o.Name(),
			SQL:      []string{deleteSQL},
			Expected: fmt.Sprintf("rows affected=%d", count),
			Actual:   fmt.Sprintf("rows affected=%d", affected),
		}
	}
	return Result{OK: true, Oracle: o.Name(), SQL: []string{deleteSQL, countSQL}}
}
