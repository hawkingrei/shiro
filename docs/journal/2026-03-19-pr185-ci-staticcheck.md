# PR 185 CI Staticcheck Fix

## What changed

- Updated `internal/generator/select_query_builder_test.go` to add explicit `return` statements after three `query == nil` guards before later field access.
- Left the test behavior unchanged while making the control flow explicit for `staticcheck`.

## Why

- PR `#185` failed the GitHub Actions `test` job during `golangci-lint`.
- The failed annotations were all `SA5011` reports in `TestSelectQueryBuilderDisallowLimit`, `TestSelectQueryBuilderMinJoinTables`, and `TestSelectQueryBuilderDisallowSetOps`.

## Validation

- Reviewed failed CI logs from `gh run view 23258249560 --log-failed`.
- Ran `go test ./internal/generator`.

## Follow-up

- Add a repo-pinned lint entrypoint so local reproduction uses the same `golangci-lint` version and arguments as CI.
