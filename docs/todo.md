# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.
Last review: 2026-02-05. Added parser fast-path guard; remaining optional parser LRU.

## Generator / Oracles

1. CERT: add stronger guardrails for DISTINCT/ORDER BY/ONLY_FULL_GROUP_BY.
2. DQP/TLP: reduce predicate_guard frequency without weakening semantic assumptions.
3. DQP: expand plan-hint coverage and add optimizer variables if needed.
4. Consider increasing `groundtruth_max_rows` to reduce `groundtruth:table_rows_exceeded` skips.
5. Consider lowering DSG per-table row counts to stay under the GroundTruth cap.
6. Split join-only vs join+filter predicates into explicit strategies with separate weights and observability.
7. Wire GroundTruth join key extraction into oracle execution for JoinEdge building.
8. Refactor per-oracle generator overrides into data-driven capability profiles to reduce duplicated toggles.

## Reporting / Aggregation

1. Build a report index for on-demand loading (replace monolithic `report.json` for large runs). Define `report_index.json` (or sharded `report_index_000.json` + `report_index.meta.json`) with `version`, `generated_at`, and `cases[]` entries containing `case_id`, `oracle`, `reason`, `error_reason`, `flaky`, `timestamp`, `path`, and `has_details`.
2. Add index writer in `internal/report`: scan case dirs, read `summary.json`, emit index files with a configurable shard size; include optional `index.gz` support for CDN/S3.
3. Update report UI to load the index first, then fetch individual `summary.json` files on demand; add paging and client-side caching; keep a compatibility mode to read legacy `report.json` when index is missing.
4. Add `report_base_url` (or reuse existing config) to allow loading reports from a public S3/HTTP endpoint; ensure CORS guidance is documented.
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

1. Introduce a `QueryAnalysis` struct in `internal/generator` that captures deterministic/aggregate/window/subquery/limit/group-by/order-by flags, join counts/signatures, predicate stats, and subquery allowance plus disallow reasons. Compute it once in `GenerateSelectQuery` and attach it to `Generator.LastFeatures`.
2. Replace duplicated query walkers in `internal/oracle/sql.go` (e.g., `queryDeterministic`, `queryHasSubquery`, `queryHasAggregate`, `queryHasWindow`) with the `QueryAnalysis` fields when the query is generator-produced. Keep lightweight helpers only for SQL-only paths.
3. Define `OracleSpec` or `QuerySpec` (generator constraints + predicate policy) and extend `SelectQueryBuilder` to build queries that satisfy these constraints up front. Move oracle guardrails (limit/window/nondeterministic/predicate guard/min join/require predicate match) from oracle `Run` methods into specs.
4. Replace `internal/runner/runner_oracle_overrides.go` hard-coded toggles with data-driven capability profiles (e.g., `OracleProfile` map). Profiles should include feature toggles, predicate mode, join policy, min join tables, and subquery allowances, then apply a single profile per oracle.
5. Reduce TiDB parser overhead in `observeSQL` by adding a keyword fast-path and allowing precomputed analysis from generator/oracle paths (fast-path done). Added a tiny LRU cache for repeated SQL parse results.
6. Validation: update tests for builder/spec equivalence and ensure oracle semantics remain unchanged; run targeted oracle/generator tests to confirm skip reduction and coverage stability.
