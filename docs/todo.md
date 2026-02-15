# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.
Latest sync: addressed PR #114 review follow-ups by reducing `AnalyzeQueryFeatures` redundant traversals via a primary single-pass walk, centralizing template strategy normalization in generator (reused by runner), and removing duplicate template-weight normalization from generator fallback logic; updated generator/runner tests accordingly (2026-02-15).
Latest sync: reviewed latest local `logs/shiro.log` + `reports/*/summary.json` (2026-02-15): 17 captured cases (16 `skipped`, 1 `not_applicable`), all skipped cases had `minimize_reason=base_replay_not_reproducible`; GroundTruth remained skip-dominant with DSG mismatch reasons (`base_table`/`right_key`), while DQP stayed effective and template strategy counters (`join_filter`/`join_only`) were both active.
Latest sync: GroundTruth now auto-aligns effective `maxRows` with TQS `wide_rows` in DSG mode (while still honoring baseline config floors), reducing `groundtruth:table_rows_exceeded` skips caused by cap mismatch; added regression coverage (2026-02-14).
Latest sync: added runner-level QPG tests for threshold-triggered adaptive overrides and TTL-based override retirement, plus a CI-focused QPG config snippet in README (2026-02-14).
Latest sync: centralized non-template QPG adaptive thresholds and override TTL into config (`qpg.no_*_threshold`, `qpg.override_ttl`), and updated runner QPG weight-override logic to consume config-driven values with normalization defaults (2026-02-14).
Latest sync: centralized QPG template-override tuning into config (`qpg.template_override`) with defaults/normalization for thresholds, boost weights, enabled probability, and TTL; runner now reads these config values instead of hard-coded constants (2026-02-14).
Latest sync: completed observability rollout for `set_operations` / `derived_tables` / `quantified_subqueries`: QueryFeatures now detects these flags across nested subqueries/CTEs, runner interval logs expose per-interval counts/ratios, and generator/runner regression tests were added (2026-02-14).
Latest sync: added generator observability for `window_frame` and `interval_arith` features (feature detection + runner interval counters/ratios), with regression tests for QueryFeatures and runner stats accounting (2026-02-14).
Latest sync: strengthened CERT build-time guardrails by disallowing aggregate/distinct/group-by/having/order-by/set-op/window shapes (to avoid `ONLY_FULL_GROUP_BY`/ordering-related noise), and tightened TLP/DQP predicate shaping to simple-column mode so predicate-guard skips are reduced without relaxing semantic checks; added regression tests (2026-02-14).
Latest sync: split template join-predicate shaping into explicit `join_only` vs `join_filter` strategies with configurable feature weights (`template_join_only_weight` / `template_join_filter_weight`) and interval observability; GroundTruth now uses a shared edge-building path that prefers SQL AST extraction when it provides equal/better key coverage, and both pick/run paths are wired through it with regression tests (2026-02-14).
Latest sync: reviewed fresh local logs/reports after oracle fixes (2026-02-14): DQP showed no `sql_error_1054` and stayed effective, EET `no_transform` skip dropped in the DQE interval, but GroundTruth remained skip-dominant (`dsg_key_mismatch_right_key`/`base_table`) with effective ratio 0 and captured cases still minimized as non-reproducible.
Latest sync: completed follow-ups from the 2026-02-14 logs/reports review: GroundTruth now adds DSG prechecks + right-key availability checks with higher pick retries, EET now falls back across rewrite kinds to reduce `no_transform`, and DQP now skips invalid-scope queries (`dqp:scope_invalid`) with NATURAL RIGHT JOIN scope regression coverage (2026-02-14).
Latest sync: reviewed local `logs/shiro.log` + `reports/case_*/summary.json` (2026-02-14): GroundTruth effective ratio repeatedly dropped to 0 due to `dsg_key_mismatch_right_key`/`empty_query`; EET skips were dominated by `eet:no_transform`; recent captured cases were mostly non-reproducible during minimize (`base_replay_not_reproducible`).
Latest sync: addressed PR #110 follow-up review findings by sanitizing dot-segment case path components, preferring local case summary URLs when case IDs are present, and treating empty `details` objects as not detail-loaded in frontend normalization (2026-02-13).
Latest sync: addressed PR #110 review threads by aligning search-blob empty-details handling, adding abort/race safety for on-demand case detail fetches, and preventing unresolved-summary lazy-load attempts via `detail_loaded=true` fallback in index entries (2026-02-13).
Latest sync: completed report index + on-demand detail loading (P1): shiro-report now writes `reports.index.json` and per-case `cases/<case_id>/summary.json`, publish uploads include index+case summaries, and UI loads index first with lazy `summary_url` fetch plus legacy manifest fallback (2026-02-13).
Latest sync: addressed PR #109 review follow-ups in report UI (simplified case render key, cleaned search-blob construction, fixed disabled button hover behavior, and added pagination aria-labels) (2026-02-13).
Latest sync: improved report UI query efficiency (P0) with debounced+deferred keyword search, prebuilt per-case search blobs, pagination (30 cases/page), and lazy rendering of heavy case body sections only after row expansion (2026-02-13).
Latest sync: addressed PR #108 review follow-ups for metadata bootstrap robustness (safe async error handling, complete-only missing-case loaded marking, draft-edit preservation during merge, narrowed effect dependencies via refs, and shared auth-header usage for PATCH) (2026-02-13).
Latest sync: report UI now bootstraps case metadata on load from Worker `/api/v1/cases` (with token-auth headers) so saved tags/issues remain visible after refresh; similar-case in-page fetch now includes auth headers as well (2026-02-13).
Latest sync: fixed Cloudflare Git deploy D1 binding drift by adding root `wrangler.jsonc` DB binding, aligning worker `wrangler.jsonc` binding name to `DB`, and documenting that root config must include runtime bindings to prevent post-deploy binding loss (2026-02-13).
Latest sync: added Worker 500 logging with request_id for debugging (2026-02-12).
Latest sync: improved logs/reports diagnostics (PQS error stage/sql fields, USING-id projection dedup, EET runtime DISTINCT+ORDER BY skip classification, and startup recovery of stale minimize in_progress -> interrupted) with tests (2026-02-12).
Latest sync: allow tag/issue saves even when metadata is not preloaded (2026-02-12).
Latest sync: treat metadata 404 as empty meta in the UI (2026-02-12).
Latest sync: secured worker list/search/similar behind read auth, documented metadata-only similarity/search, and enabled observability flag (2026-02-12).
Latest sync: trimmed Worker D1 schema to case_id/labels/linked_issue, removed download endpoint, and UI uses archive URLs for downloads (2026-02-12).
Latest sync: secured metadata GET with auth when API token is set, narrowed payloads, and store tokens in session storage (2026-02-12).
Latest sync: removed the waterfall view selector from the report UI (2026-02-11).
Latest sync: moved worker write-token input to a Settings panel in the report UI (2026-02-11).
Latest sync: worker metadata GET and UI tagging/issue linking via token-authenticated PATCH (2026-02-11).
Latest sync: report UI orders case detail sections as error, schema.sql, data.tsv, then case.sql (2026-02-11).
Latest sync: report UI hides Expected/Actual blocks independently when their text is empty (2026-02-11).
Latest sync: report UI removes the Open report.json action from the toolbar (2026-02-11).
Latest sync: report UI maps `gs://` artifact locations to public GCS URLs for downloads (2026-02-11).
Latest sync: report UI hides `case.tar.zst` content and uses a single download-case action per bug (2026-02-11).
Latest sync: simplified USING ambiguity checks to reuse a single left-side column scan (2026-02-11).
Latest sync: guarded USING joins against ambiguous left-side columns and added generator regression coverage (2026-02-11).
Latest sync: fixed PQS USING-id select qualification, added richer case log fields, and stabilized QPG delta logging (2026-02-11).
Latest sync: clarified recursive CTE feature semantics and refactored generator interval log format strings (2026-02-11).
Latest sync: switched tests to load `config.example.yaml` and added the example config file (2026-02-11).
Latest sync: added generator feature counters for natural joins, full join emulation, and recursive CTEs with tests (2026-02-11).
Latest sync: added GCS artifact storage support with gs:// URL mapping while keeping R2 publish flow and legacy S3 compatibility (2026-02-10).
Latest sync: completed PQS v2/v3 (join-aware JOIN ON, subquery predicates, derived tables) and refreshed PQS TODOs (2026-02-09).
Latest sync: added DQP SET_VAR hints for tidb_enable_outer_join_reorder and tidb_enable_inl_join_inner_multi_pattern (2026-02-09).
Latest sync: addressed PQS review feedback (bool literal consistency, row error checks, preallocations, doc status) (2026-02-09).
Latest sync: fixed PQS lints for errcheck (rows.Close) and revive confusing-results (2026-02-09).
Latest sync: aligned TODO with follow-ups after PQS float-guard work (2026-02-09).
Latest sync: deduplicated CODDTest CASE conditions to reduce oversized dependent predicates (2026-02-09).
Latest sync: ensured PQS pivot row fetch checks rows.Err to avoid silent skips (2026-02-09).
Latest sync: stripped database qualifiers from dumped CREATE VIEW statements in reports (2026-02-09).
Latest sync: added NATURAL JOIN guard to avoid ambiguous columns in generated SQL (2026-02-09).
Latest sync: documented TiDB issue formatting guidance (collapse schema/load data details, format run query SQL) (2026-02-09).
Latest sync: added Cloudflare Worker assets stub and wrangler config for Git-integrated deploys (2026-02-10).
Latest sync: added root `wrangler.jsonc` so `wrangler versions upload` locates the worker entrypoint/assets (2026-02-10).
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
Latest sync: fixed lints for worker sync HTTP response close path (`bodyclose`) and case-archive exported constant comments (`revive`) (2026-02-08).
Latest sync: addressed PR #78 security/reliability findings for Worker auth defaults/body limits/download URL handling and `shiro-report` artifact URL generation (2026-02-07).
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

1. DQP: expand plan-hint coverage and add optimizer variables if needed. (Added SET_VAR hints for tidb_enable_outer_join_reorder and tidb_enable_inl_join_inner_multi_pattern.)
2. Refactor per-oracle generator overrides into data-driven capability profiles to reduce duplicated toggles. (done)
3. Roll out `set_operations` / `derived_tables` / `quantified_subqueries` with profile-based oracle gating and observability before default enablement. (done: recursive QueryFeatures detection + runner interval counters/ratios + regression tests)
4. Extend grouping support from `WITH ROLLUP` to `GROUPING SETS` / `CUBE` with profile-based fallback for unsupported dialects.
5. Reduce GroundTruth DSG skip reasons (`dsg_key_mismatch_base_table` / `dsg_key_mismatch_right_key`) with additional join-shape/key-alignment constraints before oracle execution.

## PQS (Rigger OSDI20)

1. Triage PQS join runtime error 1105 (`index out of range`) from the 2026-02-09 run and decide whether to file an upstream TiDB issue.

## Reporting / Aggregation

1. Add index writer sharding and optional gzip support for CDN/S3.
2. Consider column-aware EXPLAIN diff once table parsing stabilizes.
3. Report summaries now expose `error_reason`, `bug_hint`, `error_sql`, and `replay_sql` for indexing. (done)
4. Review follow-up: `sqlErrorReason(nil)` now returns empty reason and EET ORDER BY drop path is documented. (done)
5. Report summary now includes `minimize_status` and emits early case-allocation logs to improve logs/reports correlation. (done)
6. Minimize status flow now has explicit `interrupted` fallback when execution exits while minimize is in progress. (done)
7. Minimize now prechecks base replay reproducibility and marks non-reproducible cases as `flaky` with explicit `minimize_reason` / `flaky_reason` metadata. (done)
8. Cloudflare metadata plane follow-up: add explicit audit trail (who/when/what) for metadata PATCH and sync operations.
9. Frontend UX: waterfall/list switch, direct archive/report links, Worker download API integration, and native label/issue editing controls are done. (done)
10. AI search: Worker now supports per-case similar lookup with optional AI summary; next step is adding vector-style embedding retrieval/rerank once case text fields are normalized.
11. Frontend CI now runs compile/lint/test in a dedicated workflow job; consider adding end-to-end smoke checks against a fixture `reports.json` payload.
12. Serve the report UI directly from Worker assets for single-domain deployment. (done)
13. Configure Worker observability settings in wrangler.jsonc. (done)

## Coverage / Guidance

1. Centralize tuning knobs for template sampling weights and QPG template overrides (enable prob/weights/TTLs/thresholds). (done: template join strategy weights and `qpg.template_override` thresholds/weights/probability/TTL are config-driven)

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
