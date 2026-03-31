# Missing Column Downgrade Config Gate

## What changed

- Added a new `oracles.downgrade_missing_column_to_skip` config flag and left it disabled by default.
- Wired `internal/runner/runner.go` so the `EET` / `Impo` / `PQS` missing-column false-positive downgrade only runs when that flag is explicitly enabled.
- Updated `internal/runner/runner_errors_test.go` to cover both the enabled and disabled behavior.
- Updated `internal/config/config_test.go` and `config.example.yaml` so the new default and explicit override are documented and verified.

## Why

- The previous behavior downgraded many ordinary `missing_column` failures to skip unconditionally for `EET`, `Impo`, and `PQS`.
- That made it too easy to hide potentially interesting runtime regressions unless the operator already knew the historical false-positive context.
- Making the downgrade opt-in keeps the safety valve available for noisy campaigns while restoring the default posture to "report unless explicitly configured otherwise".

## Validation

- Ran `gofmt -w internal/config/config.go internal/config/config_test.go internal/runner/runner.go internal/runner/runner_errors.go internal/runner/runner_errors_test.go`.
- Ran `go test ./internal/config ./internal/runner -run 'Test(LoadDefaults|LoadMissingColumnDowngradeOverride|DowngradeMissingColumnFalsePositive|ClassifyResultError|AnnotateEffectiveErrorMetadata|EffectiveResultErrorReason)'`.

## Follow-up

- Consider replacing the single global downgrade flag with per-oracle or per-signature policy so `EET`, `Impo`, and `PQS` can adopt different missing-column handling without another code change.
