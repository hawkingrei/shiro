# False-Positive Reduction Via FK-Safe Minimize Replay

## What changed

- Re-triaged the fresh `2026-03-21` report directories to rank false-positive sources instead of continuing on the earlier search-range task.
- Confirmed the dominant fresh report noise is `base_replay_not_reproducible`, not a new oracle-side semantic skip cluster.
- Narrowed the clearest avoidable source to schema replay failures at `apply_schema` with MySQL error `1824` (`Failed to open the referenced table ...`) on FK-heavy minimized cases.
- Updated `internal/runner/minimize_replay.go` so schema replay runs under `FOREIGN_KEY_CHECKS=0/1`, matching the existing insert replay behavior.
- Added targeted regression coverage in `internal/runner/minimize_replay_test.go` for the new schema wrapper path.

## Key finding

- Among the fresh `2026-03-21` report directories currently on disk, four cases carry `minimize_reason=base_replay_not_reproducible`.
- Two of those fresh cases fail in `apply_schema` with `eet:sql_error_1824` or `impo:sql_error_1824`, and their captured `schema.sql` files already show why:
  - `reports/case_0002_019d0067-1bbc-7693-940b-86e5b5cbdd81/schema.sql`
  - `reports/case_0004_019d0086-7632-74ce-b3f9-f75c83ffc231/schema.sql`
  - `reports/case_0006_019d0c7a-60f2-7d3a-b428-959ad36243fd/schema.sql`
- These schemas disable `FOREIGN_KEY_CHECKS` before replay, but the minimizer's internal replay path previously did not, so base replay could fail on DDL ordering even when the captured case artifacts were otherwise valid.

## Why

- This is replay-only noise: it does not improve bug filtering, and it can incorrectly downgrade high-signal cases into flaky/non-repro buckets.
- The failure mode is mechanical and local to Shiro's replay harness, so the best fix is in the harness rather than in oracle generation.
- Disabling FK checks during schema setup keeps minimization focused on whether the target query discrepancy reproduces, not on whether `SHOW CREATE TABLE` happened to emit tables in a parent-before-child order.

## Validation

- Inspected fresh `summary.json` files for the `2026-03-21` batch and compared the replay diagnostics:
  - `minimize_base_replay_failure_stage=apply_schema`
  - `minimize_base_replay_actual_error_reason in {eet:sql_error_1824, impo:sql_error_1824}`
- Inspected the corresponding captured `schema.sql` files and confirmed they already require `SET FOREIGN_KEY_CHECKS=0`.
- Ran `gofmt -w internal/runner/minimize_replay.go internal/runner/minimize_replay_test.go`.
- Ran `go test ./internal/runner -run 'TestWrapReplay(Inserts|Schema)WithForeignKeyChecks|TestBuildReproSQLErrorSQLKind|TestMinimizeBaseReplayGateDetailedKeepsFailureDiagnostics'`.

## Signal-quality metric

- Primary:
  - reduce fresh report counts where `minimize_reason=base_replay_not_reproducible` and `minimize_base_replay_failure_stage=apply_schema`
- Secondary:
  - reduce fresh `sql_error_1824` replay diagnostics in summaries/logs without suppressing genuine `exec_case_sql` mismatches

## Wrong-result notes

- After removing the replay-harness FK noise, the more interesting remaining wrong-result-shaped fresh cases are still in `EET`, especially the window-order-sensitive signatures that currently land in flaky buckets for unrelated replay-setup reasons.
- The minimized `Impo` `INTERSECT` case `reports/case_0005_019d0c6d-569c-788f-9163-1e95bb879005` remains the strongest fresh execution-failure bug candidate and is not part of this false-positive patch.

## Follow-up

- Add interval summaries for replay failure stage plus normalized setup-error reason so new replay-only flaky clusters are visible without opening each report directory.
