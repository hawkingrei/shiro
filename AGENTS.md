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
- On bug, rotate to a fresh database (`<database>_rN`) and reinitialize schema/data to avoid cross-case contamination.
- Default config now lives in code (`defaultConfig`) and is printed at startup; `oracles.strict_predicates` is true by default with a config toggle to loosen it.

## Follow-ups
- Tune generator type compatibility to reduce benign type errors.
- Expand plan-hint coverage and add TiDB optimizer variables to DQP if needed.
- Extend CODDTest to multi-table dependent expressions with join-aware mappings.

## Progress
- Implemented core fuzzing pipeline, oracles, plan replayer capture, and reporting.
- Added window functions, correlated subqueries, subquery depth guard, and DQP hints/variables.
- Added local case artifacts and optional S3 upload interface; inserts are recorded for reproduction.
- Plan replayer is hardcoded to `PLAN REPLAYER DUMP EXPLAIN`.

## Experience notes
- `PLAN REPLAYER DUMP` output may include URL or only a zip name; URL parsing must be tolerant of trailing punctuation.
- Using `EXPLAIN` in replayer avoids executing the buggy SQL again during dump.
- Tracking inserts in-memory provides a lightweight reproduction script without exporting full data dumps.
- CODDTest requires careful NULL handling; current guard only enables when relevant columns have no NULLs, but some CODDTest cases still trigger.
- TLP still produces occasional mismatches even after skipping subqueries/aggregates/non-deterministic predicates.
- Plan replayer tokens already include `.zip` in TiDB master; when using a template ending in `.zip`, avoid double suffix.
- Replayer download should fall back to `@@tidb_last_plan_replayer_token` when dump output lacks URL.
- Feature-level bandit adaptation needs to set generator weights per-query and clear them after the query finishes.
- Further reducing CODDTest/TLP false positives required limiting to simple predicates (AND of comparisons) and deterministic, non-subquery expressions.

## TODO
- Re-run `go mod tidy` / `go build` after dependency config changes.
- Consider adding more join types and window function variations once dependencies are stable.
- Evaluate remaining false positives for TLP/DQP and add oracle-specific constraints if needed.
- Further restrict CODDTest to avoid three-valued logic pitfalls (e.g., only simple predicates, no NOT/OR, avoid IS NULL).
- Tighten TLP predicate shape (pure comparisons, no CASE/functions) to reduce remaining mismatches.
