# 2026-03-11 Log Observability And Replay Diagnostics

## Completed

- Added stable error observability helpers so captured cases now derive an effective `error_reason` from either `error_reason` or `skip_error_reason`, and emit a normalized `error_signature` for report/log clustering.
- Extended report summaries with a top-level `error_signature` field and updated case-capture logging to print both `error_reason` and `error_signature`.
- Added structured base-replay diagnostics for minimize failures, including replay kind, failure stage, outcome, last replay operation/SQL, and expected vs actual replay error metadata.
- Switched runner-side oracle error aggregation to use the effective error reason so skip-for-minimize captures still contribute to interval error-reason summaries.
- Added focused unit tests for effective error-reason fallback, signature generation, base-replay diagnostic propagation, and minimize gate detail retention.

## Validation

- Ran `gofmt -w` on the touched Go files.
- Ran `go test ./internal/report -run TestDoesNotExist -count=1`.
- Ran `go test ./internal/runner -run 'TestEffectiveResultErrorReasonFallsBackToSkipErrorReason|TestAnnotateEffectiveErrorMetadataSetsStableSignatureForSkippedMissingColumn|TestFormatBaseReplayLogSuffix|TestMinimizeBaseReplayGateDetailedKeepsFailureDiagnostics|TestApplyMinimizeOutcomeMergesBaseReplayDetails' -count=1`.

## Follow-ups

- Push `groundtruth:rowcount_exceeded` prediction earlier into generator/oracle constraints so obviously explosive cases are skipped before execution instead of after the oracle runs.
- Consider adding interval summaries keyed by `error_signature` in addition to `error_reason` so repeated internal planner/runtime failures are visible without opening report artifacts.
