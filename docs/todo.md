# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.
Last review: 2026-02-07. Added broader SQL2023 regression coverage (recursive CTE guards, FULL JOIN emulation edge cases, window determinism/named-window overrides, GROUPING ordinal unwrap), oracle SQL helper fast-path/build tests, TiDB-compat regressions to keep `INTERSECT ALL`/`EXCEPT ALL` disabled, nil-operand hardening for expression determinism/build paths, runner-side `error_reason/bug_hint` classification for PlanCache and GroundTruth mismatch reporting, P1 batch-1 skip-reduction overrides for GroundTruth/CODDTest, systemic NoREC constraint tightening via builder-level set-op disallow + query guard reasons, scope-manager enforcement for `USING` qualified-column visibility with regression tests, set-op ORDER/LIMIT normalization, inline-subquery `WITH` guard unification, canonical `plan_reference_missing` bug-hint alignment, GroundTruth DSG mismatch reason taxonomy with retry-on-mismatch picking, CODDTest build-time null/type prechecks, EET/TLP signature prechecks for invalid ORDER BY ordinals and known-table column visibility, top-level interval aggregation for `groundtruth_dsg_mismatch_reason`, and summary/index top-level propagation of `groundtruth_dsg_mismatch_reason`.
Latest sync: cleaned lint-only `ineffassign` findings in GroundTruth query picking and runner DSG mismatch label extraction (2026-02-07).
Latest sync: completed PR-77 follow-ups for alias rendering, nested-query scope enforcement (with strict empty-column-set checks), and FULL JOIN emulation USING anti-filter scope compatibility; added generator/oracle regression tests (2026-02-07).
Latest sync: addressed PR #78 security/reliability findings for Worker auth defaults/body limits/download URL handling and `shiro-report` artifact URL generation (2026-02-07).

## Generator / Oracles

1. CERT: add stronger guardrails for DISTINCT/ORDER BY/ONLY_FULL_GROUP_BY.
2. DQP/TLP: reduce predicate_guard frequency without weakening semantic assumptions.
3. DQP: expand plan-hint coverage and add optimizer variables if needed.
4. Consider increasing `groundtruth_max_rows` to reduce `groundtruth:table_rows_exceeded` skips.
5. Consider lowering DSG per-table row counts to stay under the GroundTruth cap.
6. Split join-only vs join+filter predicates into explicit strategies with separate weights and observability.
7. Wire GroundTruth join key extraction into oracle execution for JoinEdge building.
8. Refactor per-oracle generator overrides into data-driven capability profiles to reduce duplicated toggles.
9. Roll out `set_operations` / `derived_tables` / `quantified_subqueries` with profile-based oracle gating and observability before default enablement.
10. Extend grouping support from `WITH ROLLUP` to `GROUPING SETS` / `CUBE` with profile-based fallback for unsupported dialects.
11. Add per-feature observability counters for `natural_join`, `full_join_emulation`, `recursive_cte`, `window_frame`, and `interval_arith`.
12. GroundTruth now emits detailed `dsg_key_mismatch_*` skip reasons and retries candidate generation before skipping. (done)
13. CODDTest now enforces null/type guardrails during query build to reduce runtime `coddtest:null_guard` skips. (done)
14. EET/TLP now run signature prechecks for invalid ORDER BY ordinals and known-table column visibility before executing signature SQL. (done)
15. Runner interval summary now reports aggregated `groundtruth_dsg_mismatch_reason` from GroundTruth skip deltas. (done)
16. Summary/report index metadata now propagates `groundtruth_dsg_mismatch_reason` as a top-level field. (done)

## PQS (Rigger OSDI20)

1. Add a `PQS` oracle skeleton (`internal/oracle/pqs.go`) with an isolated capability profile and metrics namespace.
2. Implement pivot-row sampling for generated queries (table/alias-aware), including deterministic row identity serialization for containment checks.
3. Add a lightweight expression evaluator + rectifier for three-valued logic (`TRUE/FALSE/NULL`) that can force predicate truth for the sampled pivot row.
4. Build PQS query synthesis paths for `WHERE` first, then `JOIN ON`, and add skip reasons when rectification is unsafe or unsupported.
5. Add containment assertion SQL templates and reducer-friendly report fields (`pivot_values`, `rectified_predicates`, `containment_query`).
6. Add staged tests: evaluator correctness, rectification invariants, pivot containment (single-table), then join-path containment.

## Reporting / Aggregation

1. Build a report index for on-demand loading (replace monolithic `report.json` for large runs). Define `report_index.json` (or sharded `report_index_000.json` + `report_index.meta.json`) with `version`, `generated_at`, and `cases[]` entries containing `case_id`, `oracle`, `reason`, `error_reason`, `flaky`, `timestamp`, `path`, and `has_details`.
2. Add index writer in `internal/report`: scan case dirs, read `summary.json`, emit index files with a configurable shard size; include optional `index.gz` support for CDN/S3.
3. Update report UI to load the index first, then fetch individual `summary.json` files on demand; add paging and client-side caching; keep a compatibility mode to read legacy `report.json` when index is missing.
4. Add `report_base_url` (or reuse existing config) to allow loading reports from a public S3/HTTP endpoint; ensure CORS guidance is documented.
5. Consider column-aware EXPLAIN diff once table parsing stabilizes.
6. Report summaries now expose `error_reason`, `bug_hint`, `error_sql`, and `replay_sql` for indexing. (done)
7. Review follow-up: `sqlErrorReason(nil)` now returns empty reason and EET ORDER BY drop path is documented. (done)
8. Cloudflare metadata plane follow-up: add explicit audit trail (who/when/what) for metadata PATCH and sync operations.
9. Frontend UX: waterfall/list switch, direct archive/report links, and Worker download API integration are done; next step is adding native UI controls for label/edit patch APIs.
10. AI search: Worker now supports per-case similar lookup with optional AI summary; next step is adding vector-style embedding retrieval/rerank once case text fields are normalized.
11. Frontend CI now runs compile/lint/test in a dedicated workflow job; consider adding end-to-end smoke checks against a fixture `reports.json` payload.

## Coverage / Guidance

1. Centralize tuning knobs for template sampling weights and QPG template overrides (enable prob/weights/TTLs/thresholds).

## Architecture / Refactor

1. Add an adaptive feature capability model (SQLancer++ style) to learn DBMS support and auto-tune generator/oracle gating.
2. Centralize query feature analysis + EXPLAIN parsing to avoid duplicated AST walks and plan parsing (shared by QPG/DQP/CERT/report).
3. Add KQE-lite join-graph coverage guidance to bias join generation toward under-covered structures.
4. Unify expression rewrite/mutation registries for EET/CODDTest/Impo with shared type inference and NULL-safety policies.
5. Refine type compatibility and implicit cast rules using SQL standard guidance to reduce benign type errors.

## Fuzz Efficiency Refactor Plan

1. Introduce a `QueryAnalysis` struct in `internal/generator` that captures deterministic/aggregate/window/subquery/limit/group-by/order-by flags, join counts/signatures, predicate stats, and subquery allowance plus disallow reasons. Compute it once in `GenerateSelectQuery` and attach it to `Generator.LastFeatures`. (done for core flags; predicate stats/disallow reasons remain on QueryFeatures)
2. Replace duplicated query walkers in `internal/oracle/sql.go` (e.g., `queryDeterministic`, `queryHasSubquery`, `queryHasAggregate`, `queryHasWindow`) with the `QueryAnalysis` fields when the query is generator-produced. Keep lightweight helpers only for SQL-only paths. (done for core helpers)
3. Define `OracleSpec` or `QuerySpec` (generator constraints + predicate policy) and extend `SelectQueryBuilder` to build queries that satisfy these constraints up front. Move oracle guardrails (limit/window/nondeterministic/predicate guard/min join/require predicate match) from oracle `Run` methods into specs. (done for core oracles, including NoREC guardrails via `QueryGuardReason` and builder-level `DisallowSetOps`)
4. Replace `internal/runner/runner_oracle_overrides.go` hard-coded toggles with data-driven capability profiles (e.g., `OracleProfile` map). Profiles should include feature toggles, predicate mode, join policy, min join tables, and subquery allowances, then apply a single profile per oracle.
5. Reduce TiDB parser overhead in `observeSQL` by adding a keyword fast-path and allowing precomputed analysis from generator/oracle paths (fast-path done). Added a tiny LRU cache for repeated SQL parse results.
6. Validation: update tests for builder/spec equivalence and ensure oracle semantics remain unchanged; run targeted oracle/generator tests to confirm skip reduction and coverage stability.
