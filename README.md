# Shiro TiDB Optimizer Fuzzer

Shiro is a SQL fuzzing CLI for TiDB that targets optimizer bugs. It generates random schemas and SQL (DDL/DML/SELECT) and validates correctness using multiple oracles inspired by SQLancer and recent research papers.

## Features
- Random schema + data generation
- Weighted feature toggles (joins, CTEs, subqueries, aggregates, plan cache)
- Oracles: NoREC, TLP, DQP, CERT, CODDTest, DQE
- Query Plan Guidance (QPG) for plan-diversity-driven state mutations
- Panic/crash detection with automatic `PLAN REPLAYER DUMP` and artifact download
- Case reports with SQL, schema, and bounded data samples
- Optional DDL coverage for indexes, views, check constraints, and foreign keys

## Quick start
1) Start TiDB (nightly or latest stable)
2) Configure `config.yaml` as needed
3) Run:

```bash
go run ./cmd/shiro -config config.yaml
```

Shiro prints the final resolved configuration at startup.

Reports are written under `reports/`.

## Concurrency
Set `workers` in `config.yaml`. Each worker runs in its own database (`<database>_wN`) to keep session variables isolated.

## SQL validity logging
Every `report_interval_seconds`, Shiro logs the ratio of parser-valid SQL to total SQL observed in that interval.
When QPG is enabled and `logging.verbose` is true, it also prints per-interval QPG coverage deltas (plans/shapes/ops/join types).

## EXISTS/IN coverage
`features.not_exists` and `features.not_in` toggle negation forms, while `weights.features.not_exists_prob` and `weights.features.not_in_prob` control how often NOT EXISTS/NOT IN are generated.

## Oracle strictness
`oracles.strict_predicates: true` (default) limits TLP/CODDTest to simple deterministic predicates to reduce false positives.
Set it to `false` if you want broader coverage at the cost of more noisy cases.

## Adaptive weights (bandit)
Enable `adaptive.enabled` to let Shiro adjust selection of actions/oracles/DML based on bug yield.
By default, only oracle selection adapts when `adaptive.enabled` is true; set `adaptive.adapt_actions`, `adaptive.adapt_dml`, or `adaptive.adapt_features` to include them.
QPG works alongside bandits: bandit weights are applied first, then QPG can temporarily override join/subquery/aggregate weights when plan coverage stalls (TTL-based).

## Query Plan Guidance (QPG)
Enable `qpg.enabled` to collect EXPLAIN plan signatures. When a repeated plan is observed, Shiro can mutate the database state (index/analyze) to explore new plans.
Configure `qpg.explain_format` (default `brief`), `qpg.mutation_prob` (0-100), and the `qpg.seen_sql_*` cache controls.
Default QPG cache values are tuned for longer runs: `seen_sql_ttl_seconds=60`, `seen_sql_max=4096`, `seen_sql_sweep_seconds=300`.
QPG also tracks operator/shape coverage to temporarily boost join/aggregate/subquery generation when coverage stalls.
To reduce overhead, QPG caches recent SQL strings and skips EXPLAIN for repeated queries within a short window.
When `EXPLAIN FORMAT='json'` is used, QPG extracts operator IDs from the JSON to continue coverage tracking.
QPG also tracks operator-sequence signatures (plan operator lists) to nudge aggregate/subquery coverage when operator diversity stalls.
JSON parsing accepts either `id` or `operator` keys and normalizes operator names for coverage accounting.
QPG normalizes EXPLAIN text (table/column/index tokens, numeric literals) before hashing to reduce noise.

## Plan cache only
Set `plan_cache_only: true` for a focused plan-cache run that executes only prepared statements.
In normal mode, Shiro still runs prepared statements and applies the same plan-cache checks; this flag just isolates that workflow.
The plan-cache check verifies `SELECT @@last_plan_from_cache = 1` on the second execution (when no warning indicates a cache skip).
On a detected bug, the runner switches to a fresh database (`<database>_rN`) and reinitializes schema/data.
Plan-cache-only cases now record the exact `PREPARE`/`EXECUTE` SQL and parameter values in the case files.

## Case minimization
Enable `minimize.enabled` to shrink captured cases. Shiro attempts to remove redundant INSERTs and SQL statements while rechecking the failure in a fresh database.
Tune `minimize.max_rounds` to cap delta-debugging passes and `minimize.timeout_seconds` to bound minimization time (defaults are more aggressive to allow deeper shrinking).
Set `minimize.merge_inserts` to re-merge single-row inserts into multi-row batches after reduction for smaller output files.
Minimized outputs are saved as `case_min.sql`, `inserts_min.sql`, and `repro_min.sql` alongside the original files.

## Static report viewer
Generate a JSON report that a static frontend can consume:

```bash
go run ./cmd/shiro-report -input reports -output web/public
```

For S3 inputs, provide a config with `storage.s3` enabled:

```bash
go run ./cmd/shiro-report -input s3://my-bucket/shiro-reports/ -config config.yaml -output web/public
```

### Next.js frontend
```bash
cd web
npm install
npm run build
```

Deploy the `web/out/` directory (GitHub Pages/Vercel). The frontend reads `report.json` at runtime, so you only need to update the JSON to refresh the view.

The report JSON now includes `plan_signature` (QPG EXPLAIN hash) and `plan_signature_format` (plain/json); the UI can filter by both.

## Notes
- If `PLAN REPLAYER DUMP` returns only a file name, set `plan_replayer.download_url_template` in `config.yaml`.
- Shiro uses `PLAN REPLAYER DUMP EXPLAIN` to avoid executing the query.
- TiDB returns a token (zip name). If the dump output does not include a URL, configure `plan_replayer.download_url_template` using your TiDB status port, e.g. `http://127.0.0.1:10080/plan_replayer/dump/%s`.
- The parser validation uses `github.com/pingcap/tidb/pkg/parser` only.
- DQP supports custom optimizer settings via `dqp.hint_sets` and `dqp.variables` (format: `var=value`).
- Join chain length is capped by `max_join_tables`.

## Papers and techniques
| Paper | Technique | Summary (from abstract, shortened) |
| --- | --- | --- |
| Detecting Optimization Bugs in Database Engines via Non-Optimizing Reference Engine Construction (NoREC) | NoREC | Rewrites a query into an unoptimizable form and compares optimizing vs. non-optimizing evaluation to detect logic bugs in DBMSs. |
| Keep It Simple: Testing Databases via Differential Query Plans (DQP) | DQP | Forces different query plans for the same query and checks result consistency; shows plan diversity can expose join-optimization bugs. |
| CERT: Finding Performance Issues in Database Systems Through the Lens of Cardinality Estimation | CERT | Checks that more restrictive queries should have no higher estimated cardinality, using EXPLAIN to surface performance/estimation issues. |
| Constant Optimization Driven Database System Testing (CODDTest) | CODDTest | Uses constant folding/propagation to transform predicates and compares results to detect logic bugs in DBMSs. |
| Testing Database Engines via Query Plan Guidance (QPG) | QPG | Guides test-case generation toward diverse query plans by mutating database state to trigger previously unseen plans. |

## S3 upload
Configure `storage.s3` in `config.yaml`. When enabled, each case directory is uploaded as-is and the summary includes `upload_location`.
