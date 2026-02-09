# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.
Latest sync: addressed PQS review feedback (bool literal consistency, row error checks, preallocations, doc status) (2026-02-09).
Latest sync: fixed PQS lints for errcheck (rows.Close) and revive confusing-results (2026-02-09).
Latest sync: aligned TODO with follow-ups after PQS float-guard work (2026-02-09).
Latest sync: added NATURAL JOIN guard to avoid ambiguous columns in generated SQL (2026-02-09).
Latest sync: documented TiDB issue formatting guidance (collapse schema/load data details, format run query SQL) (2026-02-09).
Last review: 2026-02-07. Added broader SQL2023 regression coverage (recursive CTE guards, FULL JOIN emulation edge cases, window determinism/named-window overrides, GROUPING ordinal unwrap), oracle SQL helper fast-path/build tests, TiDB-compat regressions to keep `INTERSECT ALL`/`EXCEPT ALL` disabled, nil-operand hardening for expression determinism/build paths, runner-side `error_reason/bug_hint` classification for PlanCache and GroundTruth mismatch reporting, P1 batch-1 skip-reduction overrides for GroundTruth/CODDTest, systemic NoREC constraint tightening via builder-level set-op disallow + query guard reasons, scope-manager enforcement for `USING` qualified-column visibility with regression tests, NATURAL JOIN column-visibility enforcement, set-op ORDER/LIMIT normalization, inline-subquery `WITH` guard unification, canonical `plan_reference_missing` bug-hint alignment, GroundTruth DSG mismatch reason taxonomy with retry-on-mismatch picking, GroundTruth USING-to-ON rewrite for ambiguity reduction, CODDTest build-time null/type prechecks, EET/TLP signature prechecks for invalid ORDER BY ordinals and known-table column visibility, EET distinct-order/scope pre-guards, top-level interval aggregation for `groundtruth_dsg_mismatch_reason`, summary/index top-level propagation of `groundtruth_dsg_mismatch_reason`, builder retry tuning for constrained oracles, GroundTruth/Impo retry-on-invalid-seed behavior, and summary-level `minimize_status` with interrupted fallback.
Latest sync: skipped float/double columns when building PQS predicates to avoid exact-float false positives (2026-02-09).
Latest sync: added a PQS predicate-strategy bandit (rectify-random vs pivot-single/multi) with bandit metadata and predicate-range tests (2026-02-09).
Latest sync: added a minimal PQS 3VL evaluator/rectifier with predicate rectification metadata and fallback reasons (2026-02-08).
Latest sync: optimized PQS containment queries to match only `id` columns when available, reducing SQL size (2026-02-08).
Latest sync: reviewed a PQS-focused run: 20 PQS cases (18 join, 1 single-table, 1 error), with one TiDB runtime error (`Error 1105: index out of range`) that was non-reproducible in minimize (2026-02-09).
Latest sync: added a PQS join containment SQL integration test for basic two-table pivots (2026-02-08).
Latest sync: cleaned `docs/roadmap.md` completed items, reorganized the roadmap by stages, and synced `docs/todo.md` with `docs/notes/follow-ups.md` (2026-02-08).
Latest sync: moved completed Impo roadmap items out of TODO tracking and recorded them in feature notes (2026-02-08).
Latest sync: enhanced PQS pivot sampling with `id`-range selection (avoids `ORDER BY RAND()`), added basic two-table `JOIN ... USING (id)` pivots with alias-aware matching, switched containment checks to `LIMIT 1` existence probes, and enabled a default PQS oracle weight (2026-02-08).
Latest sync: added PQS v1 oracle (single-table pivot selection, equality/IS NULL predicates, containment check), wired weights/overrides/config, and expanded unit tests (2026-02-08).
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
Latest sync: adjusted flaky errno test assertions to use non-fatal checks (2026-02-09).
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

## PQS (Rigger OSDI20)

1. Build PQS query synthesis paths for `JOIN ON`, including join-aware pivot bindings and skip reasons when rectification is unsafe or unsupported.
2. Extend containment assertion SQL templates and reducer-friendly report fields for join-path PQS.
3. Add staged tests for join-path containment and rectification invariants. (join containment SQL test added; rectifier tests added)
4. Triage PQS join runtime error 1105 (`index out of range`) from the 2026-02-09 run and decide whether to file an upstream TiDB issue.

## Reporting / Aggregation

1. Build a report index for on-demand loading (replace monolithic `report.json` for large runs).
2. Add index writer with sharding and optional gzip support for CDN/S3.
3. Update report UI to load the index first, then fetch individual `summary.json` files on demand with paging and caching.
4. Add `report_base_url` (or reuse existing config) to allow loading reports from HTTP/S3 endpoints with CORS guidance.
5. Consider column-aware EXPLAIN diff once table parsing stabilizes.

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
