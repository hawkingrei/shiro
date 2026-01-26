# Follow-ups

- Tune generator type compatibility to reduce benign type errors.
- Expand plan-hint coverage and add TiDB optimizer variables to DQP if needed.
- Extend CODDTest to multi-table dependent expressions with join-aware mappings.
- Add CERT-specific guardrails for DISTINCT/ORDER BY to avoid ONLY_FULL_GROUP_BY errors.
- Add report aggregation views (commit/bug type) and export in the frontend.
- Add S3/report incremental merging and multi-source aggregation.
- Centralize tuning knobs for template sampling/weights and QPG template overrides (enable prob, weights, TTLs, thresholds).
