# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.

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
