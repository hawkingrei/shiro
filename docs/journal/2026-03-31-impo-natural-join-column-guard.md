# Impo Natural Join Column Guard

## What changed

- Inspected `logs/shiro.log` and focused on the freshest repeated `Impo` failures because no local `reports/` directory was present in this workspace snapshot.
- Identified a Shiro-side seed problem behind the repeated `Error 1054 (42S22): Unknown column ...` `Impo` base execution failures: valid unqualified merged columns from `NATURAL JOIN` were being treated as invalid by the local column guard/sanitizer.
- Updated `internal/oracle/sql.go` so `queryColumnsValid` and `sanitizeQueryColumnsWithOuter` accept unqualified column references when the referenced name is visible from the current table set instead of requiring every column to be qualified.
- Added a defense-in-depth scope gate in `internal/oracle/impo.go` so sanitized `Impo` seeds must still pass generator scope validation before entering stage1/init execution.
- Added targeted regression coverage in `internal/oracle/sql_column_guard_test.go` for `NATURAL JOIN` merged-column validation and sanitization retention.

## Why

- The fresh runtime signal in `logs/shiro.log` was not a TiDB wrong-result case. It was a repeated Shiro-side invalid-seed pattern clustered under `impo:base_exec_failed`, with examples such as `Unknown column 't3.k0' in 'where clause'` and `Unknown column 't4.k0' in 'on clause'`.
- `Impo` relies on `sanitizeQueryColumnsWithOuter` as a repair/guard step before building the init SQL. Requiring qualifiers there is wrong for merged columns produced by `NATURAL JOIN` or `USING`-style semantics, because those columns must remain unqualified in later scopes.
- Preserving valid unqualified merged-column references is better than silently rewriting them to arbitrary qualified columns, because the latter both changes query meaning and can create invalid SQL that looks like an engine issue in logs.

## Validation

- Ran `gofmt -w internal/oracle/sql.go internal/oracle/impo.go internal/oracle/sql_column_guard_test.go`.
- Ran `go test ./internal/oracle -run 'Test(QueryColumnsValid|SanitizeQueryColumns|ImpoSeedSkipReason)'`.
- Ran `go test ./internal/generator -run 'TestValidateQueryScope(Natural|Using)'`.
- Ran `go test ./internal/oracle/...`.

## Follow-up

- Extend the same merged-column visibility modeling into derived-table alias/projection repair so `Impo` can classify alias-scope mistakes earlier instead of depending on a final scope rejection.
- Consider splitting `impo:base_exec_failed` interval summaries by parse/scope/runtime buckets so Shiro-side invalid-seed regressions can be separated from TiDB execution regressions directly from logs.
