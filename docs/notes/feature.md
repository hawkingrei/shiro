# Feature Notes

## Report UI formatting
- Align EXPLAIN output by tab-separated columns; preserve original schema.sql text and fold it by default.
- Reordered report layout: schema/data at top, Expected/Actual blocks next, EXPLAIN blocks after, diffs at the bottom.
- EXPLAIN diffs render via `react-diff-viewer-continued` (split view, diff-only, word-level highlights, extra context lines).
- Diff blocks scroll horizontally and content width is capped for readability.

## Web linting
- Added ESLint flat config using `eslint-config-next` and wired `npm run lint` to `eslint .`.
- Added CI step to run `npm run lint` in `web`.

## Web build and worker overrides
- The diff viewer worker pipeline is vendored under `web/vendor/react-diff-viewer-continued/` to avoid the `compute-lines` ↔ worker import cycle while keeping Web Worker execution.
- When upgrading `react-diff-viewer-continued`, re-sync `compute-core.js` from `lib/esm/src/compute-lines.js` and keep `computeWorker.js` importing `compute-core.js` (not `compute-lines.js`).
- `web/next.config.js` must keep alias entries for `compute-lines.js` and relative `./computeWorker.ts` so Turbopack resolves the worker correctly.
- Validate with `npm run test:worker` and use `SHIRO_RELEASE=1 npm run build` for optimized builds.

## Generator randomness
- Randomized DATE/DATETIME/TIMESTAMP literals across year/month/day and full time range (2023-2026), with leap-year aware day bounds.
- TQS randomValue now uses the same broader date/time range with leap-year handling.
- DATE/DATETIME/TIMESTAMP equality predicates prefer sampled INSERT values to keep match rates after randomization.

## Generator observability
- Added subquery coverage logging (allowed/disabled/has/attempted/built/failed) plus disallow-reason stats per interval.
- Added per-oracle subquery coverage logging to isolate DQP/TLP overrides.
- Added IN(subquery)/NOT IN(subquery) counters using generator AST features (not SQL regex).
- Added oracle-variant IN(subquery) counters via SQL AST parsing.
- Switched EXISTS/NOT EXISTS and IN(list) counters to AST parsing for generator and plan-cache SQL paths.
- Treat NOT (IN(subquery)) as NOT IN in SQL AST parsing for variant counts.
- Split generator SQL feature counts into a separate interval log line to keep plan-cache ratios consistent.
- Ensured scalar subquery disallow flags are respected when generating predicates and template predicates.

## EET oracle
- Skip EET cases where ORDER BY is constant under LIMIT to avoid nondeterministic sampling.
- Relaxed EET/CODDTest builder subquery constraint; predicates still gate subquery forms.
- Predicate guard allows EXISTS/IN subquery forms.
- Skip EET cases with USING-qualified column references to avoid base signature errors.
- EET USING-qualified guard now checks CTEs as well.
- EET now rewrites USING to ON before applying guards, and only requires one predicate target to match the guard.

## Generator tuning
- Increased PredicateSubqueryScale and PredicateExistsProb to raise EXISTS coverage.

## Group By ordinals
- Added optional `GROUP BY 1,2` rendering by wrapping group keys as ordinals while retaining the base expressions for semantic checks.
- Group-by ordinal probability is controlled by the feature bandit (not config).
- Added a unit test for ordinal rendering and a strict panic on invalid ordinal state.

## Report UI
- Added a flaky tag when expected/actual EXPLAIN match but signatures differ.

## QPG stats
- Added a monotonic seen SQL counter to avoid negative deltas from TTL-based sweeps.

## PQS optimization
- Reduced PQS containment SQL size by selecting and matching only `id` columns when all pivot tables expose them.
- Added a minimal 3VL evaluator/rectifier for PQS predicates with rectification metadata and fallback reasons.
- Added a PQS predicate-strategy bandit (rectify-random vs pivot-single/multi) with per-run bandit metadata.
- Skipped float/double columns when building PQS predicates to reduce false positives from exact float equality.

## Impo roadmap completions
- Logged per-interval `sql_valid_ratio`, `impo_invalid_columns_ratio`, and `impo_base_exec_failed_ratio`, with threshold alerts.
- Persisted Impo artifacts (`impo_seed.sql`, `impo_init.sql`, `impo_mutated.sql`) in case outputs.
- Added Impo config knobs for max mutations, timeout, stage1 disable, and LR join retention.
- Added base row-count precheck and skip when exceeding the configured cap.
- Added seed-query guards for nondeterminism and plan cache hints/session variables.
- Implemented Impo mutations for ANY/ALL, BETWEEN, IN-list, EXISTS, HAVING, ORDER BY removal, LIMIT expansion, comparison normalization, WHERE tautology/contradiction, and UNION/UNION ALL variants.
- Rejected recursive CTEs while allowing non-recursive WITH in Impo init.
- Emitted mutation-type coverage counters in Impo details.
- Recorded Impo replay metadata and wired `impo_contains` for minimizer replay checks.
