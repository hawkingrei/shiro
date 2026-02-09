# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.
Last review: 2026-02-07. Added broader SQL2023 regression coverage (recursive CTE guards, FULL JOIN emulation edge cases, window determinism/named-window overrides, GROUPING ordinal unwrap), oracle SQL helper fast-path/build tests, TiDB-compat regressions to keep `INTERSECT ALL`/`EXCEPT ALL` disabled, nil-operand hardening for expression determinism/build paths, runner-side `error_reason/bug_hint` classification for PlanCache and GroundTruth mismatch reporting, P1 batch-1 skip-reduction overrides for GroundTruth/CODDTest, systemic NoREC constraint tightening via builder-level set-op disallow + query guard reasons, scope-manager enforcement for `USING` qualified-column visibility with regression tests, NATURAL JOIN column-visibility enforcement, set-op ORDER/LIMIT normalization, inline-subquery `WITH` guard unification, canonical `plan_reference_missing` bug-hint alignment, GroundTruth DSG mismatch reason taxonomy with retry-on-mismatch picking, GroundTruth USING-to-ON rewrite for ambiguity reduction, CODDTest build-time null/type prechecks, EET/TLP signature prechecks for invalid ORDER BY ordinals and known-table column visibility, EET distinct-order/scope pre-guards, top-level interval aggregation for `groundtruth_dsg_mismatch_reason`, summary/index top-level propagation of `groundtruth_dsg_mismatch_reason`, builder retry tuning for constrained oracles, GroundTruth/Impo retry-on-invalid-seed behavior, and summary-level `minimize_status` with interrupted fallback.
Latest sync: hardened compute worker webpack replacement plugin resolution so CI tests do not fail when `NormalModuleReplacementPlugin` is missing from the bundled webpack export (2026-02-09).
Latest sync: replaced the `JSX.Element` type alias in the report page with `ReactNode` to avoid missing JSX namespace errors during Next.js builds (2026-02-09).
Latest sync: switched the report diff viewer `compareMethod` to use `DiffMethod.WORDS_WITH_SPACE` for type-safe builds (2026-02-09).
Latest sync: updated the remaining report diff viewer `compareMethod` usage to `DiffMethod.WORDS_WITH_SPACE` for type-safe builds (2026-02-09).
Latest sync: configured Next.js alias/extension alias to map `computeWorker.ts` to the shipped `computeWorker.js` and updated the worker verification script for the web report build (2026-02-09).
Latest sync: guarded `next build` behind a release flag and added a `build:release` script so local defaults use `next dev` (2026-02-09).
Latest sync: switched the default `next dev` script to `--turbo` and added `dev:webpack` for a webpack fallback (2026-02-09).
Latest sync: vendored `react-diff-viewer-continued` compute worker implementation to break the worker/compute-lines cycle while keeping Worker execution, and aliased Next.js to the local compute-lines module (2026-02-09).
Latest sync: CI now verifies the web worker alias config and runs a release web build via `SHIRO_RELEASE=1` to exercise the optimized build path (2026-02-09).
Latest sync: next config now loads webpack via Next's bundled webpack fallback so the worker config test runs in CI without a direct webpack dependency (2026-02-09).
Latest sync: turbopack aliases now include relative compute-lines/computeWorker specifiers to avoid resolution failures with relative worker imports (2026-02-09).
Latest sync: documented web worker override upgrade guidance in `docs/notes/feature.md` (2026-02-09).
Latest sync: cleaned lint-only `ineffassign` findings in GroundTruth query picking and runner DSG mismatch label extraction (2026-02-07).
Latest sync: completed PR-77 follow-ups for alias rendering, nested-query scope enforcement (with strict empty-column-set checks), and FULL JOIN emulation USING anti-filter scope compatibility; added generator/oracle regression tests (2026-02-07).
Latest sync: Impo seed guardrail now preserves the last concrete skip reason, and minimize now requires base replay reproducibility before reduction (non-reproducible cases are tagged flaky with explicit reason fields); added runner/oracle regression tests (2026-02-07).
Latest sync: SelectQueryBuilder now reuses query analysis for constraint checks to avoid redundant AST walks (2026-02-08).
Latest sync: runner oracle overrides now use data-driven profiles for consistent capability gating (2026-02-08).
Latest sync: QuerySpec now accepts oracle profiles to derive generator constraints from the same capability gating (2026-02-08).
Latest sync: renamed oracle profile types/helpers to avoid revive stutter warnings (2026-02-08).
Latest sync: NoREC profile disables set operations and added regression coverage for profile constraint mapping and analysis refresh (2026-02-08).
Latest sync: profile constraints no longer relax subquery bans, CERT/CODDTest builder setup is deduped, and runner override tests cover AllowSubquery/JoinUsingProbMin (2026-02-08).
Latest sync: minimizer now uses strategy-based multi-pass reduction (error-case vs replay-spec) with validated insert merge and weighted candidate selection to improve minimization depth and stability; added runner tests (2026-02-08).
Latest sync: SelectQueryBuilder now skips full feature analysis for join-only constraints and reuses cached determinism metadata when present (2026-02-08).
Latest sync: SelectQueryBuilder now invalidates cached analysis after predicate attachment to keep determinism checks accurate (2026-02-08).
Latest sync: minimizer lint cleanup for revive (named returns + unused param removal) (2026-02-08).
Latest sync: centralized minimizer default rounds into a shared constant to avoid divergent reducer defaults (2026-02-08).

## Generator / Oracles

1. CERT: add stronger guardrails for DISTINCT/ORDER BY/ONLY_FULL_GROUP_BY.
2. DQP/TLP: reduce predicate_guard frequency without weakening semantic assumptions.
3. DQP: expand plan-hint coverage and add optimizer variables if needed.
4. Consider increasing `groundtruth_max_rows` to reduce `groundtruth:table_rows_exceeded` skips.
5. Consider lowering DSG per-table row counts to stay under the GroundTruth cap.
6. Split join-only vs join+filter predicates into explicit strategies with separate weights and observability.
7. Wire GroundTruth join key extraction into oracle execution for JoinEdge building.
8. Refactor per-oracle generator overrides into data-driven capability profiles to reduce duplicated toggles. (done)
9. Roll out `set_operations` / `derived_tables` / `quantified_subqueries` with profile-based oracle gating and observability before default enablement.
10. Extend grouping support from `WITH ROLLUP` to `GROUPING SETS` / `CUBE` with profile-based fallback for unsupported dialects.
11. Add per-feature observability counters for `natural_join`, `full_join_emulation`, `recursive_cte`, `window_frame`, and `interval_arith`.
12. GroundTruth now emits detailed `dsg_key_mismatch_*` skip reasons and retries candidate generation before skipping. (done)
13. CODDTest now enforces null/type guardrails during query build to reduce runtime `coddtest:null_guard` skips. (done)
14. EET/TLP now run signature prechecks for invalid ORDER BY ordinals and known-table column visibility before executing signature SQL. (done)
15. Runner interval summary now reports aggregated `groundtruth_dsg_mismatch_reason` from GroundTruth skip deltas. (done)
16. Summary/report index metadata now propagates `groundtruth_dsg_mismatch_reason` as a top-level field. (done)
17. Constrained oracle builders now use higher build retries (`QuerySpec.MaxTries`) and TLP disallows set-ops at build time to reduce false-positive/empty-query skips. (done)
18. GroundTruth and Impo now retry candidate picking before returning skip reasons on empty/guardrail seeds. (done)
19. GroundTruth now rewrites USING joins to explicit ON predicates during query picking to avoid ambiguous USING resolution in multi-table left factors. (done)
20. EET now runs additional scope/distinct-order pre-guards before signature execution to reduce SQL-invalid base errors. (done)

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
8. Report summary now includes `minimize_status` and emits early case-allocation logs to improve logs/reports correlation. (done)
9. Minimize status flow now has explicit `interrupted` fallback when execution exits while minimize is in progress. (done)
10. Minimize now prechecks base replay reproducibility and marks non-reproducible cases as `flaky` with explicit `minimize_reason` / `flaky_reason` metadata. (done)
11. Minimize now runs strategy-based multi-pass reduction with validated insert-merge adoption and weighted candidate acceptance to avoid non-improving rewrites. (done)

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
4. Replace `internal/runner/runner_oracle_overrides.go` hard-coded toggles with data-driven capability profiles (e.g., `Profile` map). Profiles should include feature toggles, predicate mode, join policy, min join tables, and subquery allowances, then apply a single profile per oracle. (done)
5. Reduce TiDB parser overhead in `observeSQL` by adding a keyword fast-path and allowing precomputed analysis from generator/oracle paths (fast-path done). Added a tiny LRU cache for repeated SQL parse results.
6. Validation: update tests for builder/spec equivalence and ensure oracle semantics remain unchanged; run targeted oracle/generator tests to confirm skip reduction and coverage stability.
