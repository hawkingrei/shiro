# Impo ANY/ALL Empty-Subquery Guard

## What changed

- Inspected the fresh `reports/case_0001_019d42fc-46ec-73e4-914d-46d07aac525c` artifact and confirmed the captured `Impo` mismatch came from `FixMAnyAllU` mutating `<= ALL (subquery)` into `<= ANY (subquery)`.
- Tightened `internal/oracle/impo/fixmanyallu.go` so `FixMAnyAllU` / `FixMAnyAllL` are only generated when the quantified subquery is syntactically guaranteed to return at least one row.
- Treated only two shapes as guaranteed non-empty for this mutation: subqueries without `FROM`, or aggregate subqueries without `GROUP BY` / `HAVING`, while also rejecting `LIMIT 0` and non-zero `OFFSET`.
- Added `internal/oracle/impo/fixmanyallu_test.go` coverage for potentially empty subqueries, guaranteed non-empty aggregate subqueries, and `LIMIT 0`.
- Tightened `internal/runner/minimize_core.go` so replay-spec minimization preserves replay shape for `FixMAnyAllU` / `FixMAnyAllL`; if the reduced SQL drops the quantified `ALL` / `ANY` form, minimization now reports `replay_shape_not_preserved` instead of `success`.
- Added `internal/runner/minimize_core_refactor_test.go` coverage for the `FixMAnyAll` replay-shape validator.

## Why

- The fresh report was a Shiro-side false positive, not a TiDB wrong-result. For empty subqueries, `x <= ALL(empty)` evaluates to `TRUE`, while `x <= ANY(empty)` evaluates to `FALSE`, so `ALL -> ANY` is not a sound upper mutation unless the subquery is known to be non-empty.
- The concrete witness in the captured case was a correlated subquery whose filtered inner relation could be empty for some outer rows, which explains the observed `base_contains_mutated` result.
- The previous minimizer behavior could keep only the replay outcome while discarding the original mutation shape, leading to degenerate minimized SQL such as `SELECT 1` being recorded as a successful minimization even though it no longer explained the original `ANY/ALL` mismatch.

## Validation

- Ran `gofmt -w internal/oracle/impo/fixmanyallu.go internal/oracle/impo/fixmanyallu_test.go internal/runner/minimize_core.go internal/runner/minimize_core_refactor_test.go`.
- Ran `go test ./internal/oracle/impo ./internal/runner -run 'Test(FixMAnyAllGuard|ReplayShapePreservedFixMAnyAll)'`.
- Ran `go test ./internal/oracle/impo ./internal/runner`.

## Follow-up

- Generalize replay-shape preservation beyond `FixMAnyAll*` so other replay-based minimizers can reject degenerate reductions without adding one-off validators mutation by mutation.
- Consider surfacing fresh false-positive classifications directly in the report summary so “Shiro-side unsound mutation” and “engine-facing reproducible mismatch” separate cleanly during batch triage.
