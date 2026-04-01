# Impo Column Guard for Derived Scope and Merged Columns

## What changed

- Tightened `internal/oracle/sql.go` column validation so `queryColumnsValid` now models visible scope instead of only raw base-table columns.
- Added derived-table projection awareness: references now validate against projected output aliases rather than the underlying source table schema.
- Added alias-aware table resolution so qualified references must use the visible alias name, not the hidden base-table name.
- Added merged-column visibility handling for `USING` / `NATURAL JOIN`, including rejection of later qualified references to merged columns after they become unqualified-only.
- Added regression coverage in `internal/oracle/sql_column_guard_test.go` for hidden derived-table columns, valid projected aliases, and merged-column references that become invalid in later joins.

## Why

- The latest `2026-04-01` run showed `Impo` spending budget on `base_exec_failed` seeds with `1052 ambiguous column` and `1054 unknown column`.
- Those failures were not TiDB bug candidates; they were Shiro-side scope leaks where the seed guard accepted queries that became invalid once derived-table projection boundaries or merged-column visibility rules mattered.
- Filtering these seeds before execution raises effective `Impo` yield by shifting budget away from invalid seeds and toward executable mutation candidates.

## Validation

- Ran `gofmt -w internal/oracle/sql.go internal/oracle/sql_column_guard_test.go`.
- Ran `go test ./internal/oracle -run 'Test(QueryColumnsValid|SanitizeQueryColumns)'`.
- Ran `go test ./internal/oracle`.

## Follow-up

- Add a post-`InitWithOptions` scope validation pass for `Impo` so stage1 rewrites that change visibility can be rejected before `base_exec_failed`.
- Extend the same scope model into `sanitizeQueryColumns` so more invalid derived-table seeds are repaired instead of only rejected.
