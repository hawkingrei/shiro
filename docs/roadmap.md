# Shiro Roadmap: Staged Plan

This roadmap lists pending work only. Completed items are tracked in `docs/notes/summary.md` and `docs/notes/feature.md`.

## Stage 0: Correctness and Observability

- Stabilize feature gates by layer (single-table, joins, subqueries, aggregates) with per-layer configs.
- Isolate noise: categorize and cap non-bug errors, keep oracle false positives below 1%.
- Run long-horizon regressions (daily 6h/24h smokes with fixed seeds and auto-minimized repros).
- Add Impo skip reasons to `dynamic_state.json` for live observability.
- Expose Impo seed vs. init SQL in summary fields for filtering.
- Implement `rmUncertain` and an explicit unstable-function list; add per-oracle toggles for enforcement.
- Add Impo result comparison with type normalization and column-name aware, order-insensitive matching.
- Add stable hashing, row sampling, and max row-size guards for large Impo result sets.
- Add Impo invariants when `cmp==2`, plus fallback to signature comparison for oversized rowsets.
- Add per-worker oracle stats and per-oracle isolation to reduce cross-case contamination.
- Add case deduplication by `(oracle, mutation, plan signature)`.
- Centralize tuning knobs for template sampling weights and QPG template overrides.

## Stage 1: Impo and Reporting

- Add missing Impo mutations: DISTINCT in nested subqueries, redundant-predicate removal (CNF/DNF), guarded De Morgan rewrites, and boolean IS TRUE/IS FALSE toggles.
- Add NULL-handling gates: schema nullability checks, NULL-avoidance generation switches, and three-valued-logic soundness mode.
- Add set-op mutations beyond UNION, guarded by TiDB feature support (INTERSECT/EXCEPT).
- Add mutation guards for window-function queries after stage1 and for LIKE/REGEXP corner cases.
- Add CTE-aware mutation and restoration support.
- Add plan-guided mutation selection tied to QPG signals.
- Add configurable mutation selector (random subset vs. exhaustive) and mutation-weight tuning for optimizer coverage.
- Add per-mutation timeout instrumentation and logging.
- Add Impo mode to target plan-cache queries and skip plan-cache artifacts that cause interference.
- Add ability to replay Impo cases without stage1 rewrites.
- Add Impo minimizer support for reducing mutated SQL separately.
- Add report fields for Impo mutation seed, truncation status, max rows used, and compare mode.
- Add report grouping by oracle and mutation type, plus multi-run aggregation for Impo.
- Add report UI: Impo case tag, detail view, compare-explain view, and row-sample diff display.
- Add Impo diff artifacts for missing vs. extra rows (sampled).
- Add CSV export for mutation coverage stats and a dashboard panel for mutation yield vs. bug yield.
- Add Impo regression tests (known containment pair, mutation set determinism) and fuzz tests for mutation restore stability.
- Add CI smoke test for the Impo oracle path.
- Build report index for on-demand loading and update the UI to fetch per-case summaries.
- Add report index sharding, optional gzip, and `report_base_url`/CORS documentation.
- Add compatibility mode for legacy `report.json` when the index is missing.
- Consider column-aware EXPLAIN diff once table parsing stabilizes.
- Add TiDB feature compatibility checks for Impo mutations.
- Remove `impomysql` after port completion and update references.
- Add Impo docs: migration note, Pinolo mutation taxonomy, oracle limitations and NULL semantics, tuning guide, and repro recipe.
- Add Impo-only config example and CLI flag to prioritize Impo.
- Add configs to disable LIKE/REGEXP mutations and control their probability.
- Add guards for REGEXP empty patterns, LIKE all-wildcard cases, and cross-version regex engine differences.
- Add guarded mutations for collation comparisons, implicit casts, numeric boundaries, decimal rounding, date/time boundaries, and boolean normalization.
- Add IN-subquery ANY/ALL rewriting mutation and optimizer-stress hints (semi-join, join reorder).
- Add linter rule for oracle naming consistency.
- Add per-oracle seed persistence for repro.
- Add adaptive selection between Impo and DQP based on plan coverage.
- Add a tuning guide for overall oracle mix with Impo enabled.

## Stage 2: PQS and Adaptive

- Implement a 3VL expression evaluator and rectifier for PQS.
- Add join-aware pivot binding across aliases and JOIN ON rectification.
- Extend containment SQL templates for join-path PQS and add reducer-friendly artifacts.
- Add staged PQS tests for join-path containment.
- Add an adaptive capability model (SQLancer++ style) to learn feature support and auto-tune gating.
- Centralize query feature analysis and EXPLAIN parsing for QPG/DQP/CERT/report reuse.
- Add KQE-lite join-graph coverage guidance for generator biasing.
- Unify rewrite/mutation registries for EET/CODDTest/Impo with shared type inference and NULL-safety policies.
- Refine type compatibility and implicit cast rules using SQL standard guidance.
- Extend grouping support to GROUPING SETS/CUBE with profile-based fallback for unsupported dialects.
- Add per-feature observability counters for natural joins, full-join emulation, recursive CTE, window frames, and interval arithmetic.
