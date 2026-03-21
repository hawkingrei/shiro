# Impo Fresh Intake And Set-Op Guardrail

## What changed

- Re-triaged the newly dropped `2026-03-21` artifacts under `reports/` and `logs/shiro.log` instead of continuing to optimize against the older `2026-03-17` batch.
- Confirmed the fresh batch shifted away from the historical `PQS missing_column` cluster: the new reports are `7` `EET` cases and `3` `Impo` cases, with no fresh `PQS` report directories.
- Identified `reports/case_0005_019d0c6d-569c-788f-9163-1e95bb879005` as the strongest new TiDB issue candidate: a minimized `Impo` `INTERSECT` query fails to execute with `Can't find column ... in schema` even though the captured `schema.sql` shows the referenced columns exist.
- Tightened `internal/oracle/sql.go` so `queryColumnsValid` and `sanitizeQueryColumnsWithOuter` recurse into `SetOps` instead of only checking the left query body.
- Added `internal/oracle/sql_column_guard_test.go` with targeted regression coverage for set-operation operand validation and sanitization.

## Why

- The previous `PQS missing_column` direction was driven by stale `2026-03-17` artifacts. The new batch shows `PQS` still running at full effective ratio in `logs/shiro.log` but not producing fresh `missing_column` reports.
- `Impo` now has the freshest repeated planner/schema failure signature, while at least one minimized case (`case_0005`) looks like a valid TiDB planner bug rather than obvious Shiro-generated invalid SQL.
- The old column guardrail path in `internal/oracle/sql.go` validated subqueries in expressions but skipped `UNION`/`INTERSECT`/`EXCEPT` operands entirely, leaving an avoidable blind spot in Impo seed screening.

## Validation

- Inspected fresh `summary.json` files from the `2026-03-21` batch and compared them against the older `2026-03-17` reports.
- Reviewed `logs/shiro.log` around the new captures to confirm fresh `EET` and `Impo` signatures and the absence of fresh `PQS` error reports.
- Ran `gofmt -w internal/oracle/sql.go internal/oracle/sql_column_guard_test.go`.
- Ran `go test ./internal/oracle -run 'TestQueryColumnsValidChecksSetOperationOperands|TestSanitizeQueryColumnsRepairsSetOperationOperands|TestImpoSeedSkipReason'`.

## Follow-up

- Extend the same Impo column guardrail logic to derived-table aliases/projections, not only set-operation operands.
- Promote the fresh minimized `missing_column` cases into direct TiDB issue filing, starting with the minimized `Impo` `INTERSECT` case and the fresh `EET` planner signature case.
