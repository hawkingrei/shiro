# Notes Summary

## Summary of decisions
- Mapped the four supplied papers to concrete oracles: NoREC, DQP (plan differential), CERT (cardinality restriction), CODDTest (constant folding/propagation), plus TLP and DQE to match SQLancer’s core coverage for SELECT/UPDATE/DELETE.
- Implemented a small, modular SQL generator with explicit schema state; joins, CTEs, subqueries, aggregates, and plan cache are feature-toggled and weighted.
- Uses TiDB parser only for syntax validation; avoids other TiDB source dependencies.
- For every failing case (oracle mismatch or panic-like error), the runner triggers `PLAN REPLAYER DUMP` and attempts to download the artifact, then writes a report with SQL, schema, and bounded data samples.

## Paper-to-oracle mapping
- NoREC (docs/norec.md): compare optimized query count vs. unoptimized predicate-evaluation count.
- DQP (docs/dqp.md): execute same query under different plan hints and compare results.
- CERT (docs/cert.md): EXPLAIN cardinality monotonicity under restrictive predicates.
- CODDTest (docs/coddtest.md): constant folding/propagation via auxiliary query + CASE mapping.
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
