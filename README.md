# Shiro TiDB Optimizer Fuzzer

Shiro is a SQL fuzzing CLI for TiDB that targets optimizer bugs. It generates random schemas and SQL (DDL/DML/SELECT) and validates correctness using multiple oracles inspired by SQLancer and recent research papers.

## Features
- Random schema + data generation
- Weighted feature toggles (joins, CTEs, subqueries, aggregates, plan cache)
- Oracles: NoREC, TLP, DQP, CERT, CODDTest, DQE
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

## Oracle strictness
`oracles.strict_predicates: true` (default) limits TLP/CODDTest to simple deterministic predicates to reduce false positives.
Set it to `false` if you want broader coverage at the cost of more noisy cases.

## Adaptive weights (bandit)
Enable `adaptive.enabled` to let Shiro adjust selection of actions/oracles/DML based on bug yield.
By default, only oracle selection adapts when `adaptive.enabled` is true; set `adaptive.adapt_actions`, `adaptive.adapt_dml`, or `adaptive.adapt_features` to include them.

## Plan cache only
Set `plan_cache_only: true` to run only prepared statements and verify `SELECT @@last_plan_from_cache = 1` on the second execution.
On a detected bug, the runner switches to a fresh database (`<database>_rN`) and reinitializes schema/data.
Plan-cache-only cases now record the exact `PREPARE`/`EXECUTE` SQL and parameter values in the case files.

## Notes
- If `PLAN REPLAYER DUMP` returns only a file name, set `plan_replayer.download_url_template` in `config.yaml`.
- Shiro uses `PLAN REPLAYER DUMP EXPLAIN` to avoid executing the query.
- TiDB returns a token (zip name). If the dump output does not include a URL, configure `plan_replayer.download_url_template` using your TiDB status port, e.g. `http://127.0.0.1:10080/plan_replayer/dump/%s`.
- The parser validation uses `github.com/pingcap/tidb/pkg/parser` only.
- DQP supports custom optimizer settings via `dqp.hint_sets` and `dqp.variables` (format: `var=value`).
- Join chain length is capped by `max_join_tables`.

## S3 upload
Configure `storage.s3` in `config.yaml`. When enabled, each case directory is uploaded as-is and the summary includes `upload_location`.
