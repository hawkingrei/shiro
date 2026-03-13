# PR 170 Review Follow-ups

## What changed

- Aligned `groundTruthExactPredictableSkipReason` with `groundTruthCaps(maxRows)` so exact predictive skips use the same table/join caps as the runtime GroundTruth path.
- Added variadic observation helpers to record precomputed SQL feature metadata for multiple SQL strings consistently.
- Updated NoREC and CERT to attach SQL feature metadata for returned count / EXPLAIN SQL strings, not only for the underlying builder SQL.
- Added regression tests covering the cap alignment and returned-SQL feature map coverage.

## Why

PR review identified one behavior bug and several fast-path coverage gaps:

- the GroundTruth exact precheck could over-predict `join_rows` skips by using a stricter cap than the runtime execution path
- NoREC and CERT error paths returned SQL strings that were not present in `Result.SQLFeatures`, forcing runner stats to reparse them

## Validation

- `go test ./internal/oracle -run 'TestNoRECErrorReturnsSQLFeaturesForCountQueries|TestCERTExplainErrorReturnsSQLFeaturesForReturnedSQL|TestGroundTruthExactPredictableSkipReasonUsesGroundTruthCaps|TestRecordObservedResultSQLsStoresAllTrimmedKeys' -count=1`
- `go test ./internal/oracle -count=1`

## Follow-up

- Add expression-level SQL feature observation for helper SQL such as CODDTest `SELECT <phi>` so aux SQL can use the fast path without borrowing broader query-level features.
