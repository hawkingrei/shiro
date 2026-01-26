# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.

## Generator / Oracles

1. CERT: add stronger guardrails for DISTINCT/ORDER BY/ONLY_FULL_GROUP_BY.
2. DQP/TLP: reduce predicate_guard frequency without weakening semantic assumptions.
3. CODDTest: extend to multi-table dependent expressions while preserving NULL semantics.

## Reporting / Aggregation

1. Add frontend aggregation views (commit/bug type) and export.
2. Add S3/report incremental merging and multi-source aggregation.

## Coverage / Guidance

1. Centralize tuning knobs for template sampling weights and QPG template overrides (enable prob/weights/TTLs/thresholds).
