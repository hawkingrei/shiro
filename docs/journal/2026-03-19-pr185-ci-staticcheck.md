# PR 185 CI Staticcheck Fix

## What changed

- Updated `internal/generator/select_query_builder_test.go` to add explicit `return` statements after three `query == nil` guards before later field access.
- Updated `internal/generator/sql2023_features_test.go`, `internal/oracle/pqs_test.go`, and `internal/oracle/query_spec_test.go` with the same explicit post-guard returns after the next CI rerun surfaced additional `SA5011` reports.
- Left the test behavior unchanged while making the control flow explicit for `staticcheck`.

## Why

- PR `#185` failed the GitHub Actions `test` job during `golangci-lint`.
- The failed annotations were all `SA5011` reports in `TestSelectQueryBuilderDisallowLimit`, `TestSelectQueryBuilderMinJoinTables`, and `TestSelectQueryBuilderDisallowSetOps`.
- After pushing the first fix, CI run `23262396382` cleared those three sites and exposed the next `SA5011` sites in `TestGenerateRecursiveCTEQuery`, `TestPQSJoinContainmentSQL`, and `TestBuildQueryWithSpecAppliesProfileConstraints`.

## Validation

- Reviewed failed CI logs from `gh run view 23258249560 --log-failed`.
- Reviewed failed CI logs from `gh run view 23262396382`.
- Ran `go test ./internal/generator`.
- Ran `go test ./internal/oracle`.

## Follow-up

- Add a repo-pinned lint entrypoint so local reproduction uses the same `golangci-lint` version and arguments as CI.
