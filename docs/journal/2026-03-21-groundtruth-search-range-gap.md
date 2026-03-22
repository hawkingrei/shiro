# GroundTruth Search-Range Gap

## What changed

- Re-ranked the Shiro work after the priority shift away from `missing_column` and toward search-range expansion, false-positive reduction, and wrong-result discovery.
- Checked the new canonical Team tasks and aligned worker-2 ownership to `Shiro search-range expansion planning`.
- Analyzed the fresh `2026-03-21` `logs/shiro.log` intervals to find the dominant search-range loss instead of continuing on the stale PQS line.

## Key finding

- The largest fresh coverage gap is `GroundTruth`, not `PQS`.
- Multiple fresh intervals show `GroundTruth` with `run > 0` and `eff = 0`, with skips dominated by `groundtruth:dsg_key_mismatch_right_key`.
- Secondary `GroundTruth` loss comes from `config:subqueries_off`, but the immediate dominant source is DSG join-key mismatch.
- `PQS` still has high scalar-subquery disallow volume, but it continues to execute effectively and did not produce fresh report directories in the new batch.

## Why this matters

- Search-range expansion should target the biggest effective-run sink first.
- `GroundTruth` is currently spending many attempts on queries that never reach truth comparison, so its configured oracle weight is not translating into bug-search coverage.
- The fresh wrong-result-shaped captures are still mainly in `EET`, which makes it more valuable to recover `GroundTruth` effective coverage before widening already-noisy paths.

## Patch candidate

- Favor a generation-time fix over more runtime fallback.
- Best next patch candidate:
  - add a GroundTruth-specific DSG-compatible join builder or query-builder constraint so `pickGroundTruthQuery` stops retrying generic `GenerateSelectQuery()` candidates that end in `dsg_key_mismatch_right_key`
  - likely touch points:
    - `internal/oracle/groundtruth_oracle.go`
    - `internal/generator/generator_join.go`
    - optionally a small builder/profile constraint helper if reuse is needed

## Evidence

- `logs/shiro.log`
  - repeated `GroundTruth=run/... eff/0 skip/...`
  - repeated `groundtruth:dsg_key_mismatch_right_key`
- `internal/oracle/groundtruth_oracle.go`
  - `pickGroundTruthQuery` currently retries generic `GenerateSelectQuery()` and only filters after generation
- `internal/generator/generator_join.go`
  - DSG join generation already prefers `k*` columns, so the missing piece is stronger GroundTruth-specific query shaping rather than more post-hoc filtering

## Validation metric

- Primary:
  - reduce `groundtruth:dsg_key_mismatch_right_key` skip counts in interval logs
- Secondary:
  - increase `GroundTruth effective/run ratio`
  - watch whether recovered GroundTruth coverage contributes new mismatch/report counts without increasing flaky noise
