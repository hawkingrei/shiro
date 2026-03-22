# Wrong-Result Discovery: EET Null-Extended Order Triage

## What changed

- Re-triaged the fresh `2026-03-21` artifacts after the replay-noise reduction work, focusing only on wrong-result-shaped opportunities instead of replay setup failures.
- Compared the fresh report directories and `logs/shiro.log` captures to separate execution-failure bugs from result-mismatch signals.
- Narrowed the fresh wrong-result-shaped line to one primary `EET` class and recorded the next heuristic candidate instead of filing a TiDB issue prematurely.

## Fresh triage

- Fresh `2026-03-21` execution-failure cases still exist and remain high-signal, but they are not the same as wrong-result discovery:
  - `reports/case_0001_019d0c60-d548-7436-9a26-2f3cb2b30ff6`: `eet:signature_missing_column`
  - `reports/case_0005_019d0c6d-569c-788f-9163-1e95bb879005`: minimized `Impo` `INTERSECT` execution failure with `impo:missing_column`
- The main fresh wrong-result-shaped report is:
  - `reports/case_0006_019d0c7a-60f2-7d3a-b428-959ad36243fd`
- That case is `EET`, `flaky=true`, and the mismatch shape is:
  - identical row count (`cnt=16`)
  - different checksum
  - output rows differ only in the third projected column, while the first two final sort keys are all `NULL`

## Key finding

- The fresh wrong-result-shaped line is dominated by `EET`, not `Impo` or `GroundTruth`.
- The most suspicious fresh `EET` mismatch is likely Shiro-side determinism noise rather than a TiDB semantic bug:
  - query shape: `LEFT JOIN ... ON (1 = 0)` plus window aggregate over the null-extended side
  - final `ORDER BY 1 DESC, 2 LIMIT 16`
  - both `ORDER BY` keys collapse to `NULL`, so `LIMIT` can select different tied rows even when the rewrite is semantics-preserving
- This explains why the mismatch manifests as different `ABS(t0.id)` rows while the window-derived columns stay `NULL`.

## Candidate oracle/query classes

- Primary wrong-result-shaped class to fix on the Shiro side:
  - `EET`
  - outer-join null-extension
  - window aggregate or rank expressions
  - `LIMIT` with final `ORDER BY` that depends on expressions from the null-extended side
- Secondary engine-facing class to keep watching after the guardrail lands:
  - `EET` planner/runtime failures with `missing_column` / invalid column-id signatures
  - these remain better TiDB bug candidates than the fresh null-order mismatch

## Next patch / heuristic candidate

- Add an `EET` determinism guard for queries that combine:
  - `LIMIT`
  - `LEFT/RIGHT JOIN` whose `ON` predicate is statically false (for example `1=0`)
  - final `ORDER BY` or window `PARTITION BY` / `ORDER BY` expressions that reference only the null-extended side
- A practical first cut can reuse the existing constant-false join analysis already present in `internal/oracle/dqp.go` and add an `EET` skip when null-extended order keys make the final top-N non-deterministic.
- Best code touch points:
  - `internal/oracle/eet.go`
  - `internal/oracle/eet_test.go`
  - optionally a shared helper for `ON FALSE` outer-join detection if reuse across oracles is worthwhile

## Why this is the right next step

- Fresh wrong-result discovery is currently bottlenecked more by determinism noise than by a lack of candidate engine bugs.
- The engine-facing fresh cases are mostly execution failures, not wrong-result mismatches.
- Tightening this `EET` guard should improve the precision of wrong-result triage without suppressing the fresh planner/execution failures that still deserve TiDB attention.

## Evidence

- `reports/case_0006_019d0c7a-60f2-7d3a-b428-959ad36243fd/summary.json`
- `logs/shiro.log`
  - `2026/03/21 02:20:40 ... case captured oracle=EET ... expected=cnt=16 checksum=3081411714 actual=cnt=16 checksum=4034434164 flaky=true`
- `internal/oracle/eet.go`
  - existing determinism guards cover syntactic `ORDER BY` weakness and unstable window tie-breakers, but not null-extended outer-join order collapse
- `internal/oracle/dqp.go`
  - already contains static detection for always-false joins that can inform the new `EET` heuristic

## Follow-up

- Implement the `EET` null-extended order guard and add targeted tests around `LEFT/RIGHT JOIN ... ON FALSE` plus `LIMIT`.
- After that lands, re-check whether fresh wrong-result-shaped reports still cluster in `EET` or whether the next precision issue moves elsewhere.
