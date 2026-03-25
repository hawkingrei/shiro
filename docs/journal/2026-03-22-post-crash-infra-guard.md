# Post-Crash Infra Guard

## Context

Fresh `2026-03-22` Shiro intake produced a late `PQS` timeout case at `2026-03-22T22:08:16+08:00` on trivial `SELECT MIN(id), MAX(id) FROM t5`, followed immediately by repeated `low_sql_throughput` intervals and `throughput_guard` activation.

The logs showed a classification gap:

- `oracle_timeout_reasons last interval top=10: pqs:timeout=1`
- next intervals collapsed to `qps=0.03` / `qps=0.40`
- `stalled_reason=low_sql_throughput`
- `infra_unhealthy=false infra_ttl=0`

That meant Shiro was reacting only with the generic throughput guard instead of entering the stronger infra-degraded mode that already reduces oracle weights and caps statement timeouts.

## Change

Added a small runner-side heuristic:

- record a short-lived `recentOracleTimeoutTTL` whenever any oracle timeout is observed
- if the next stats interval is low-throughput while that recent-timeout marker is still active, promote the runner into `infra_unhealthy`

This keeps the existing behavior that a raw timeout alone does not immediately imply infra failure, but it now upgrades the state once the timeout is followed by the characteristic post-crash throughput collapse.

Files changed:

- `internal/runner/runner.go`
- `internal/runner/runner_throughput_guard.go`
- `internal/runner/runner_throughput_guard_test.go`

## Validation

Ran:

- `go test ./internal/runner -run 'TestUpdateThroughputControlsActivatesGuard|TestObserveOracleTimeoutControlSetsDQPCooldown|TestUpdateThroughputControlsMarksInfraUnhealthyAfterRecentTimeoutAndLowSample|TestObserveInfraErrorControlSetsInfraUnhealthy|TestWithTimeoutForOracleCapsInfraUnhealthy' -count=1`
- `go test ./internal/runner -count=1`

Both passed.

## Follow-up

The first timeout case that triggers the post-crash pattern can still be captured before the runner upgrades into `infra_unhealthy`. A later follow-up should let reporting/triage downgrade that triggering case when it is immediately followed by sustained low-throughput collapse.
