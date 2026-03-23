# Replay Failure-Stage Interval Summary

## Context

Fresh Shiro reruns were already logging capture counts, minimize status, minimize reasons, and normalized captured `error_signature` values. That still left one blind spot during `base_replay_not_reproducible` triage:

- workers could see that replay drift was happening,
- but they still had to open individual `summary.json` files to learn whether the drift was failing in `apply_schema`, `load_data`, `set_session_var`, or `exec_case_sql`,
- and setup-specific failures such as `sql_error_1824` were not visible at interval scope.

This made replay drift look flatter than it really was, especially when setup failures and case-error mismatches were mixed in the same batch.

## Change

Extended the runner-side reproducibility aggregation with two new interval summaries:

- `minimize_base_replay_stage last interval top=<N>` for normalized `minimize_base_replay_failure_stage`
- `minimize_base_replay_setup_errors last interval top=<N>` for normalized replay setup-error signatures

Implementation details:

- store per-case replay failure stages alongside existing captured minimize status / reason / `error_signature` counters
- detect setup failures from `minimize_base_replay_outcome=setup_error` or setup-stage names such as `apply_schema`, `load_data`, and `set_session_var`
- normalize setup failures from `minimize_base_replay_actual_error_signature` when present, with fallback to the replay actual-error reason

Files changed:

- `internal/runner/runner.go`
- `internal/runner/runner_report.go`
- `internal/runner/runner_stats.go`
- `internal/runner/runner_stats_test.go`

## Validation

Ran:

- `go test ./internal/runner -run 'TestNormalizeReplayFailureStage|TestObserveReproducibilitySummary' -count=1`
- `go test ./internal/runner -count=1`

## Follow-up

The new summaries still aggregate all replay stages into one flat bucket. A follow-up should split them by `replay_kind` and `outcome` so setup drift (`apply_schema`, `load_data`) and true mismatch failures (`exec_case_sql`) can be separated directly from the interval logs.
