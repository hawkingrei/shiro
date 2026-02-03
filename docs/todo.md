# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.
Last review: 2026-02-03. Simplified join predicate unwrap to satisfy revive lint.

## Generator / Oracles

1. CERT: add stronger guardrails for DISTINCT/ORDER BY/ONLY_FULL_GROUP_BY.
2. DQP/TLP: reduce predicate_guard frequency without weakening semantic assumptions.
3. CODDTest: extend to multi-table dependent expressions while preserving NULL semantics.
4. Consider making `CTECountMax` configurable for resource-sensitive runs.
5. Consider increasing `groundtruth_max_rows` to reduce `groundtruth:table_rows_exceeded` skips.
6. Consider lowering DSG per-table row counts to stay under the GroundTruth table cap.
7. Improve EXISTS/NOT EXISTS coverage; validate whether DMLSubqueryProb=30 and PredicateExistsProb=60 move the counters, otherwise add DQE-specific EXISTS forcing.
8. EET: add broader expression-level rewrites with schema-aware type inference and safety checks.
9. EET: add per-rewrite skip reason counters and coverage logging to validate weighting.

## Reporting / Aggregation

1. Add frontend aggregation views (commit/bug type) and export.
2. Add S3/report incremental merging and multi-source aggregation.
3. Consider column-aware EXPLAIN diff once table parsing stabilizes.

## Coverage / Guidance

1. Centralize tuning knobs for template sampling weights and QPG template overrides (enable prob/weights/TTLs/thresholds).
