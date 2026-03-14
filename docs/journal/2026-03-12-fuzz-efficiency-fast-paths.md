# Fuzz Efficiency Fast Paths

## What changed

- Added precomputed SQL subquery feature plumbing from oracle/generator code into `db.DB` validation observability so runner stats can reuse known `IN` / `EXISTS` features instead of reparsing SQL.
- Updated major query oracles (`NoREC`, `TLP`, `DQP`, `EET`, `PQS`, `GroundTruth`, `CERT`, `CODDTest`) to register wrapper SQL features for execution-time observation and attach result-level SQL feature maps for variant stats.
- Added generation-time GroundTruth predictive skip guardrails:
  - exact prechecks when `SchemaTruth` is available;
  - schema upper-bound / fanout heuristics otherwise.
- Moved full-join emulation probability gating ahead of attempt accounting so probability misses no longer inflate `full_join_emulation_attempt` noise.

## Why

- `logs/shiro.log` showed `parser_calls last interval` spending thousands of parses on SQL feature detection even when the generator/oracle already had equivalent structural knowledge.
- GroundTruth had repeated `groundtruth:rowcount_exceeded` intervals that were predictable from table size and join shape, so the runtime skip needed to move earlier into query selection.
- `full_join_emulation_attempt` was dominated by `probability_gate`, which obscured real rejection causes and overstated wasted work.

## Validation

- `go test ./internal/oracle ./internal/runner ./internal/generator -count=1`

## Follow-ups

- Extend precomputed SQL feature registration to remaining string-built paths such as plan-cache wrappers and Impo helper SQL.
- Add fast-path hit / parser-savings counters so the new observability path can be measured directly in interval logs.
- Add richer `throughput_guard` activation context so low-throughput windows are attributable without manual log stitching.
