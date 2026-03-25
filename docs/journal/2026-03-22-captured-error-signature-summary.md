# Captured Error-Signature Interval Summary

## Context

Fresh `2026-03-22` Shiro triage showed that the fastest way to spot the new `_tidb_rowid` / missing-column cluster was to read repeated `error_signature` values directly from `logs/shiro.log`. The runner already emitted interval summaries for capture counts, minimize status, and minimize reasons, but it did not expose repeated captured `error_signature` values. That forced workers to open individual `reports/` directories just to confirm obvious planner/runtime clusters.

## Change

Added runner-side aggregation for normalized captured `error_signature` values:

- record a normalized `error_signature` for each captured case alongside the existing minimize summary
- carry the aggregated counters through the stats logger snapshot/delta path
- emit `captured_error_signatures last interval top=<N>` whenever the current interval captured cases with repeated signatures

The normalization is intentionally lightweight:

- lowercase
- trim surrounding whitespace
- collapse separator-adjacent spaces around `|`, `:`, and `+`
- replace any remaining internal spaces with `_`

Files changed:

- `internal/runner/runner.go`
- `internal/runner/runner_report.go`
- `internal/runner/runner_stats.go`
- `internal/runner/runner_stats_test.go`

## Validation

Ran:

- `go test ./internal/runner -run 'TestNormalizeErrorSignature|TestObserveReproducibilitySummary' -count=1`
- `go test ./internal/runner -count=1`

## Follow-up

The new summary surfaces repeated signatures, but it still treats all captured signatures as one flat bucket. A follow-up should classify them into planner/runtime/infra groups and add recency-aware pre-crash vs post-crash hints so timeout-driven crash tails can be downweighted automatically during fresh-batch triage.
