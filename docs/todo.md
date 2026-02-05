# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.
Last review: 2026-02-05. No TODO changes from EET details/date identity fix.

## Generator / Oracles

1. CERT: add stronger guardrails for DISTINCT/ORDER BY/ONLY_FULL_GROUP_BY.
2. DQP/TLP: reduce predicate_guard frequency without weakening semantic assumptions.
3. DQP: expand plan-hint coverage and add optimizer variables if needed.
4. Consider increasing `groundtruth_max_rows` to reduce `groundtruth:table_rows_exceeded` skips.
5. Consider lowering DSG per-table row counts to stay under the GroundTruth cap.
6. Split join-only vs join+filter predicates into explicit strategies with separate weights and observability.
7. Wire GroundTruth join key extraction into oracle execution for JoinEdge building.
8. Refactor per-oracle generator overrides into data-driven capability profiles to reduce duplicated toggles.

## Reporting / Aggregation

1. Build a report index for on-demand loading (replace monolithic `report.json` for large runs). Define `report_index.json` (or sharded `report_index_000.json` + `report_index.meta.json`) with `version`, `generated_at`, and `cases[]` entries containing `case_id`, `oracle`, `reason`, `error_reason`, `flaky`, `timestamp`, `path`, and `has_details`.
2. Add index writer in `internal/report`: scan case dirs, read `summary.json`, emit index files with a configurable shard size; include optional `index.gz` support for CDN/S3.
3. Update report UI to load the index first, then fetch individual `summary.json` files on demand; add paging and client-side caching; keep a compatibility mode to read legacy `report.json` when index is missing.
4. Add `report_base_url` (or reuse existing config) to allow loading reports from a public S3/HTTP endpoint; ensure CORS guidance is documented.
5. Consider column-aware EXPLAIN diff once table parsing stabilizes.

## Coverage / Guidance

1. Centralize tuning knobs for template sampling weights and QPG template overrides (enable prob/weights/TTLs/thresholds).

## Architecture / Refactor

1. Add an adaptive feature capability model (SQLancer++ style) to learn DBMS support and auto-tune generator/oracle gating.
2. Centralize query feature analysis + EXPLAIN parsing to avoid duplicated AST walks and plan parsing (shared by QPG/DQP/CERT/report).
3. Add KQE-lite join-graph coverage guidance to bias join generation toward under-covered structures.
4. Unify expression rewrite/mutation registries for EET/CODDTest/Impo with shared type inference and NULL-safety policies.
5. Refine type compatibility and implicit cast rules using SQL standard guidance to reduce benign type errors.
