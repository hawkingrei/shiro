# TODO

This file tracks open tasks only.
Completed work should be recorded in `docs/journal/<YYYY-MM-DD>-*.md`, not here.
Each completed task should contribute at least one new improvement item here when actionable.

## Generator / Oracles

1. DQP: continue expanding plan-hint coverage and evaluate additional optimizer session variables when signal quality justifies the runtime cost.
2. Push predictable `groundtruth:rowcount_exceeded` skips into generation-time/query-builder guardrails so high-fanout cases are filtered before oracle execution.
3. Extend Impo column guardrails from set-operation operands into derived-table alias/projection validation so fresh `missing_column` cases can be classified as Shiro-side invalid seeds vs TiDB planner bugs earlier.
4. Replace GroundTruth's retry-heavy generic query sampling with a DSG-compatible join builder or query-builder constraint so `groundtruth:dsg_key_mismatch_right_key` stops dominating fresh search-range loss.
5. Generalize false outer-join null-extension analysis into a shared helper so `EET`, `DQP`, and future oracle guards can reuse the same `ON FALSE` / null-extended-side reasoning instead of duplicating heuristics.
6. Teach the Impo column guard to model merged-column visibility through derived-table aliases and projection rewrites directly, not only through the final post-sanitize scope rejection.

## Reporting / Aggregation

1. Add index writer sharding and optional gzip support for CDN/S3.
2. Consider column-aware EXPLAIN diff once table parsing stabilizes.
3. Cloudflare metadata plane: add explicit audit trail (who/when/what) for metadata PATCH and sync operations.
4. AI search: add embedding-based retrieval/rerank on top of current similar-case lookup after case text fields are normalized.
5. Frontend CI: add end-to-end smoke checks against a fixture `reports.json` payload.
6. Cluster repeated planner error signatures (for example `Can't find column ... in schema`) across report directories and generate one aggregated TiDB issue draft with representative artifacts instead of one draft per case.
7. Add a fresh-batch triage summary that separates newly captured reports from historical artifacts so workers do not keep chasing stale PQS clusters after new logs arrive.
8. Split base-replay failure-stage summaries by `replay_kind` / `outcome` so `apply_schema` setup drift and `exec_case_sql` error mismatches stop collapsing into one flat top-N bucket.
9. Add a compact wrong-result triage summary that separates likely engine-facing mismatches from likely determinism/noise cases inside the fresh batch.
10. Expose per-oracle stable-vs-explain-same mismatch counts in interval summaries and bandit dumps so wrong-result-oriented reward tuning can be validated directly from fresh rerun logs.
11. Split captured `error_signature` interval summaries into planner/runtime/infra classes and annotate pre-crash vs post-crash recency so duplicate timeout/no-throughput clusters can be downweighted automatically.

## Architecture / Refactor

1. Add an adaptive feature capability model (SQLancer++ style) to learn DBMS support and auto-tune generator/oracle gating.
2. Centralize query feature analysis + EXPLAIN parsing to avoid duplicated AST walks and plan parsing (shared by QPG/DQP/CERT/report).
3. Add KQE-lite join-graph coverage guidance to bias join generation toward under-covered structures.
4. Unify expression rewrite/mutation registries for EET/CODDTest/Impo with shared type inference and NULL-safety policies.
5. Refine type compatibility and implicit cast rules using SQL standard guidance to reduce benign type errors.

## Fuzz Efficiency Refactor Plan

1. Extend precomputed SQL feature registration and add fast-path hit metrics for remaining string-built paths (for example plan-cache wrappers and Impo helpers) so parser savings are measurable.
2. Run validation for builder/spec equivalence and oracle semantics via targeted oracle/generator test suites and skip-rate checks.
3. Add `throughput_guard` activation context (oracle mode, parser pressure, low-QPS window summary) so low-throughput intervals can be attributed without manual log reconstruction.
4. Add expression-level SQL feature observation for helper SQL (for example CODDTest `SELECT <phi>`) so aux/result SQL can use the fast path without over-approximating query-level features.
5. Downgrade the first timeout case that immediately precedes sustained `infra_unhealthy` / low-throughput collapse so post-crash trigger cases (for example trivial `PQS` pivot range timeouts) do not stay in the bug-candidate pool by default.

## Continuous Improvement

1. Add a lightweight post-task checklist/template to make journal + TODO improvement capture consistent across agents.
2. Add a CI/doc check that fails when `docs/todo.md` contains completion markers (for example `Latest sync` or `(done)`).
3. Add a helper command/script to create `docs/journal/<YYYY-MM-DD>-*.md` entries and append candidate improvement items into TODO.
4. Add a doc lint check that ensures `AGENTS.md` does not contain a `## Recent updates` section and keeps workflow guidance centralized.
5. Add an issue filing helper/template for `gh issue create` so agents can file detailed issues directly without keeping local `docs/issues` drafts.
6. Add a repo-pinned lint entrypoint (for example `make lint` or `scripts/lint.sh`) so local lint reproduces the CI `golangci-lint` version and arguments instead of relying on host-installed binaries.
