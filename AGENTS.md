# Shiro Fuzz Tool - Agent Notes

## Summary of decisions
- Mapped the four supplied papers to concrete oracles: NoREC, DQP (plan differential), CERT (cardinality restriction), CODDTest (constant folding/propagation), plus TLP and DQE to match SQLancer’s core coverage for SELECT/UPDATE/DELETE.
- Implemented a small, modular SQL generator with explicit schema state; joins, CTEs, subqueries, aggregates, and plan cache are feature-toggled and weighted.
- Uses TiDB parser only for syntax validation; avoids other TiDB source dependencies.
- For every failing case (oracle mismatch or panic-like error), the runner triggers `PLAN REPLAYER DUMP` and attempts to download the artifact, then writes a report with SQL, schema, and bounded data samples.

## Paper-to-oracle mapping
- NoREC (2007.08292v1): compare optimized query count vs. unoptimized predicate-evaluation count.
- DQP (3654991): execute same query under different plan hints and compare results.
- CERT (2306.00355v3): EXPLAIN cardinality monotonicity under restrictive predicates.
- CODDTest (2501.11252v1): constant folding/propagation via auxiliary query + CASE mapping.
- TLP/DQE: standard SQLancer oracles for predicate partitioning and DML row-selection checks.

## Notes & limitations
- CTEs are generated and used in FROM; DQP skips CTE queries to avoid hint injection into nested SELECTs.
- Plan replayer output format varies by TiDB version; the downloader matches URLs or ZIP names and supports a template URL if needed.
- The SQL generator favors smaller datasets for report dumps; row counts are capped by config.
- Added window functions and correlated subqueries behind feature flags; subquery depth is capped to avoid runaway recursion.
- DQP can apply hint sets and session variable toggles; variables are reset to DEFAULT after each variant.
- Added S3 uploader interface; local case directories are always created, and S3 is optional when configured.
- Plan replayer uses a fixed `PLAN REPLAYER DUMP EXPLAIN` template (no config).
- Each case now includes inserts.sql along with schema/data/plan replayer for reproducibility.
- Added `tidb_last_plan_replayer_token` fallback per TiDB docs to fetch dump token when output has no URL.
- Added join depth cap, USING/CROSS joins, and extra DDL coverage (index/view/check/fk).
- Added concurrency via workers with per-database isolation; SQL validity ratio logging per interval.
- Added plan-cache-only mode with PREPARE/EXECUTE and `@@last_plan_from_cache` verification.
- Plan cache paths now capture `CONNECTION_ID()` and use `EXPLAIN FOR CONNECTION` to feed QPG plan-shape tracking (prepared statements only).
- Prepared plan cache miss handling checks `SHOW WARNINGS`; a miss with no warnings is treated as an error and reported (non-prepared misses may be normal).
- On bug, rotate to a fresh database (`<database>_rN`) and reinitialize schema/data to avoid cross-case contamination.
- Default config now lives in code (`defaultConfig`) and is printed at startup; `oracles.strict_predicates` is true by default with a config toggle to loosen it.
- CODDTest only runs when the referenced columns have no NULLs; NULL creates unknown truth values and breaks the CASE mapping.
- Generated/computed columns are avoided for now to reduce TiDB master feature skew.

## Follow-ups
- Tune generator type compatibility to reduce benign type errors.
- Expand plan-hint coverage and add TiDB optimizer variables to DQP if needed.
- Extend CODDTest to multi-table dependent expressions with join-aware mappings.

## Experience notes
- Implemented core fuzzing pipeline, oracles, plan replayer capture, and reporting.
- Added window functions, correlated subqueries, subquery depth guard, and DQP hints/variables.
- Added local case artifacts and optional S3 upload interface; inserts are recorded for reproduction.
- Plan replayer is hardcoded to `PLAN REPLAYER DUMP EXPLAIN`.
- Added predicate generation for EXISTS/IN and NOT EXISTS/NOT IN (including literal lists).
- Added config flags/weights for NOT EXISTS/NOT IN frequency.
- Added a static report renderer (`cmd/shiro-report`) for aggregating cases from local or S3 into an HTML view.
- Reworked report renderer to output `report.json` and added a Next.js frontend in `web/` for GitHub Pages/Vercel.
- Report output uses UUIDv7-based case directories to avoid collisions across concurrent Shiro runs.
- Added Query Plan Guidance (QPG): plan signature tracking via EXPLAIN with plan-diversity mutations.
- QPG now parses plan nodes (operator + depth) to track operator/shape coverage and guides join/agg/subquery weights.
- QPG adds a short-term EXPLAIN cache and shape-stall heuristic to avoid repeated plan collection.
- QPG parses JSON-format EXPLAIN and tracks join-order diversity for weight boosts.
- QPG tracks operator-sequence signatures and uses them to boost agg/subquery weights when operator coverage stalls.
- QPG JSON parsing now accepts either "id" or "operator" keys with normalization.
- QPG normalizes EXPLAIN plan text to reduce noisy plan signature variance (table/column/index tokens).
- Report summaries now record `plan_signature` (QPG EXPLAIN hash) for filtering in the frontend.
- Added `plan_signature_format` (plain/json) to report summaries and UI filters.
- QPG coexists with bandits: bandit weights apply first, QPG can temporarily override join/subquery/agg when plan coverage stalls.
- TODO: Frontend aggregation views (commit/bug type) and export.
- TODO: S3/report incremental merging and multi-source aggregation.
- TODO: Generator coverage: more join/subquery variants and stability tuning.
- TODO: 统一模板采样/权重/QPG 模板覆盖的参数调优入口（启用概率、权重、TTL、阈值等）。
- `PLAN REPLAYER DUMP` output may include URL or only a zip name; URL parsing must be tolerant of trailing punctuation.
- Using `EXPLAIN` in replayer avoids executing the buggy SQL again during dump.
- Tracking inserts in-memory provides a lightweight reproduction script without exporting full data dumps.
- TLP oracle must compare against the base query without the predicate; otherwise false positives occur.
- CODDTest mappings are sensitive to float formatting; preserving raw string literals avoids rounding mismatches.
- Suppressing whitelist logs unless verbose keeps non-verbose runs clean.
- CREATE VIEW should not strip WITH if the query references CTEs; regenerate without CTE or keep WITH.
- Avoid ORDER BY numeric literals (TiDB treats them as column positions).
- CODDTest requires careful NULL handling; current guard only enables when relevant columns have no NULLs, but some CODDTest cases still trigger.
- TLP still produces occasional mismatches even after skipping subqueries/aggregates/non-deterministic predicates.
- Plan replayer tokens already include `.zip` in TiDB master; when using a template ending in `.zip`, avoid double suffix.
- Replayer download should fall back to `@@tidb_last_plan_replayer_token` when dump output lacks URL.
- Feature-level bandit adaptation needs to set generator weights per-query and clear them after the query finishes.
- Further reducing CODDTest/TLP false positives required limiting to simple predicates (AND of comparisons) and deterministic, non-subquery expressions.
- Plan cache in TiDB master does not appear to cache CTE-based PREPARE statements; plan-cache-only mode skips CTEs.
- Plan-cache sequence must run EXECUTE immediately before `SHOW WARNINGS`; otherwise warnings belong to the last `SELECT @@last_plan_from_cache`.
- For plan cache checks: run the target statement, then `SELECT @@last_plan_from_cache`, then re-run the target statement and `SHOW WARNINGS` so warnings are bound to the target SQL (not the `SELECT`). Avoid consecutive `SHOW WARNINGS` and `SELECT @@last_plan_from_cache` without an intervening EXECUTE.
- Prepared plan cache oracle compares a literal-SQL baseline signature against a prepared execution; treat mismatches as bugs when cache hit or when miss has no warnings.
- Non-prepared plan cache: a miss without warnings can be normal; only treat result-signature mismatches as bugs.
- DQP hint generation should stay minimal: pick a small number of hints per query (0-2), avoid stacking ON/OFF pairs, and keep ordering predictable.
- Code style: keep changes simple and readable; follow the Uber Go Style Guide for formatting and naming.
- `EXPLAIN FOR CONNECTION` and `@@last_plan_from_cache` must be queried right after the target EXECUTE to avoid interleaving effects.
- Go mysql driver can emit `busy buffer` if result sets are not fully drained; ensure rows are consumed before closing statement/connection.
- Reports now emit `origin_result` from the second EXECUTE (signature + sample rows) to anchor prepared vs. original comparisons.
- Prepared SQL in reports must use `SET @pN=...` + `EXECUTE ... USING @pN` form; limit PREPARE parameters to <= 8.
- SQL error handling uses a hardcoded whitelist (e.g., 1064) as fuzz-tool faults; non-whitelisted MySQL errors are treated as bugs.
- When creating PRs via `gh`, use a heredoc or multi-line `--body` to avoid literal `\n` in the description.

## TODO
- QPG: tune seenSQL cache defaults after longer runs and consider making per-interval summary logs replace per-plan logs.
- QPG: switch to TiDB-native plan hash when it becomes available.
- Highest priority: focus on join reorder, multi-table joins, and plan cache testing; optimize all oracles to target these scenarios.
- High priority: expand coverage for CTEs, correlated subqueries, and TiDB optimizer capabilities to broaden search space.
- CERT: temporarily restrict to simple single-table queries (no JOIN/subquery/aggregate); revisit to relax later.
- Case minimization: expand AST reducer coverage (IN list trimming, expression simplification, order-by reduction) and validate impact on DQP cases.
- Case minimization: trim GROUP BY items, simplify HAVING, and drop LIMIT offsets where possible.
- Case minimization: reduce replay SQL for signature/count oracles by AST transformations before emitting case_min.
- Case minimization: strip EXPLAIN wrapper and recursively simplify subqueries; re-merge single-row inserts after reduction.
- Case minimization: simplify JOIN trees (drop join conditions, collapse to left side) and handle nested table sources.
- Case minimization: optionally coerce JOIN types/USING lists and replace scalar subqueries with constants to shrink further (may reduce repro rate).
- Documentation: add richer, end-to-end oracle examples (SQL + expected/actual signatures) for training/onboarding.
- Generator: consider extending orderedArgs to handle cross-type comparisons (int vs int64/float/string) deterministically.
- Generator: when generating plan-cache string args, use fixed prefixes if an index exists; otherwise allow uuidv7 strings.
- Low priority: CODDTest tightening to only run on integer/boolean predicates to reduce noise.
- Low priority: CERT gating by base_est_rows threshold to avoid small-sample estimator noise.
- TODO: add TiFlash hint coverage (read_from_storage) when TiFlash is enabled.
