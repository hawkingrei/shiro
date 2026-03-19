# PR 185 PQS USING(id) Fix and CI Follow-ups

## What changed

- Updated `internal/oracle/pqs.go` to normalize predicate-side merged `id` references for `JOIN ... USING (id)` paths while keeping subquery inner SQL qualified.
- Added targeted regression coverage in `internal/oracle/pqs_test.go` for merged-column predicate normalization and subquery handling on `USING(id)` joins.
- Added follow-up test-only fixes in `internal/generator/select_query_builder_test.go`, `internal/generator/sql2023_features_test.go`, `internal/oracle/query_spec_test.go`, `internal/runinfo/basic_info_test.go`, and `internal/runner/runner_qpg_test.go` to make nil-guard control flow explicit for `staticcheck` without changing test behavior.
- Recorded a repo TODO to add a repo-pinned lint entrypoint so local lint reproduction matches CI.

## Why

- The primary goal of PR `#185` is to fix PQS handling of qualified merged-column references on `JOIN ... USING (id)` paths, which could otherwise lead to `pqs:missing_column` failures.
- After the functional PQS fix landed, GitHub Actions exposed unrelated but blocking `SA5011` findings in several tests, so the PR also needed a narrow sequence of nil-guard follow-ups to get CI green again.
- The journal captures both the functional PQS change and the later CI-only cleanup so the PR history matches the actual scope reviewers saw.

## Validation

- Added/updated targeted regression coverage in `internal/oracle/pqs_test.go`.
- Reviewed failed CI logs from `gh run view 23258249560 --log-failed`.
- Reviewed failed CI logs from `gh run view 23262396382`.
- Reviewed failed CI logs from `gh run view 23262976689 --log-failed`.
- Ran `go test ./internal/generator`.
- Ran `go test ./internal/oracle`.
- Ran `go test ./internal/runinfo`.
- Verified refreshed PR checks passed on head `5026810ee86f192d44f5b89d5b119ded24e1cf9e`.

## Follow-up

- Add a repo-pinned lint entrypoint so local reproduction uses the same `golangci-lint` version and arguments as CI.
