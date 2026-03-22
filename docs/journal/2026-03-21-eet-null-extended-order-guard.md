# EET Null-Extended Order Guard

## What changed

- Added a new `EET` guard in `internal/oracle/eet.go` to skip `LIMIT` queries whose final `ORDER BY` resolves only through expressions that depend on the null-extended side of an always-false outer join.
- Reused the existing `dqpExprAlwaysFalse` predicate analysis to detect `LEFT/RIGHT JOIN ... ON FALSE` without introducing a second constant-false checker.
- Added targeted regression coverage in `internal/oracle/eet_test.go` for:
  - false `LEFT JOIN` with ordinal-based final `ORDER BY`
  - preserved-side tie-breaker that should remain allowed
  - false `RIGHT JOIN` where the left side becomes null-extended

## Why

- The fresh `2026-03-21` wrong-result-shaped case `reports/case_0006_019d0c7a-60f2-7d3a-b428-959ad36243fd` was not a strong TiDB wrong-result candidate after closer inspection.
- Its shape was:
  - `EET`
  - outer join with `ON (1 = 0)`
  - window expression over the null-extended side
  - final `ORDER BY 1, 2 LIMIT ...`
- In that shape, the final order keys can collapse to `NULL`, so the top-N row set is not stable even when the rewrite is semantics-preserving. That is Shiro-side determinism noise, not a bug signal worth escalating.

## Validation

- Ran `gofmt -w internal/oracle/eet.go internal/oracle/eet_test.go`.
- Ran:
  - `go test ./internal/oracle -run 'TestEETHasNullExtendedLimitOrderWithFalseLeftJoin|TestEETHasNullExtendedLimitOrderIgnoresPreservedTieBreaker|TestEETHasNullExtendedLimitOrderWithFalseRightJoin|TestEETHasUnstableWindowAggregateWithTieBreaker'`

## Follow-up

- The current guard is intentionally narrow and targets final `ORDER BY` expressions resolved through ordinals/aliases.
- A good next refinement is to extract the false-outer-join null-extension analysis into a shared helper so other oracles can reuse it and so future `EET` heuristics can cover more derived-table / alias-heavy cases without duplicating logic.
