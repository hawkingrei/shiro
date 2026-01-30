# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.
Last review: 2026-01-30. No items completed; added join scope tests and CERT validation, reviewed latest logs, added TLP error breakdowns and captured examples, reviewed latest reports, fixed TLP UNION/ORDER BY errors, fixed TLP USING column qualification errors, normalized USING merged references, and rewrote USING to ON. Checked `logs/shiro.log`; TLP errors were 0 after 12:32. Added a TLP ORDER BY/UNION comment, removed the temporary TLP config, removed completed roadmap items, removed CERT weight config, and fixed CERT sampling at a constant rate outside bandit control. Added CERT sampling ratio logging and documented fixed sampling behavior. Ran `go test ./...`. Fixed PR description line breaks via `gh pr edit --body-file`. Refined CERT sampling counters to avoid counting fallback picks and reduce lock contention. Minimized lock contention by keeping bandit picks under a single statsMu scope and refactoring non-CERT selection helpers. Initialized non-CERT oracle index slice defensively and reran tests. Added a safe default oracle index fallback for empty non-CERT sets and reran tests. Fixed EXISTS/NOT EXISTS metrics to use regex matching for whitespace/newlines and reran tests.

## Generator / Oracles

1. CERT: add stronger guardrails for DISTINCT/ORDER BY/ONLY_FULL_GROUP_BY.
2. DQP/TLP: reduce predicate_guard frequency without weakening semantic assumptions.
3. CODDTest: extend to multi-table dependent expressions while preserving NULL semantics.
4. GroundTruth: reduce edge_mismatch by aligning join edge extraction with generator (USING/AND handling).
4. Consider making `CTECountMax` configurable for resource-sensitive runs.
5. Consider increasing `groundtruth_max_rows` to reduce `groundtruth:table_rows_exceeded` skips.
6. Consider lowering DSG per-table row counts to stay under the GroundTruth table cap.

## Reporting / Aggregation

1. Add frontend aggregation views (commit/bug type) and export.
2. Add S3/report incremental merging and multi-source aggregation.

## Coverage / Guidance

1. Centralize tuning knobs for template sampling weights and QPG template overrides (enable prob/weights/TTLs/thresholds).
