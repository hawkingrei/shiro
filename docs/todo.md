# TODO

This file tracks open tasks only.
Completed work should be recorded in `docs/journal/<YYYY-MM-DD>-*.md`, not here.
Each completed task should contribute at least one new improvement item here when actionable.

## Generator / Oracles

1. DQP: continue expanding plan-hint coverage and evaluate additional optimizer session variables when signal quality justifies the runtime cost.

## Reporting / Aggregation

1. Add index writer sharding and optional gzip support for CDN/S3.
2. Consider column-aware EXPLAIN diff once table parsing stabilizes.
3. Cloudflare metadata plane: add explicit audit trail (who/when/what) for metadata PATCH and sync operations.
4. AI search: add embedding-based retrieval/rerank on top of current similar-case lookup after case text fields are normalized.
5. Frontend CI: add end-to-end smoke checks against a fixture `reports.json` payload.

## Architecture / Refactor

1. Add an adaptive feature capability model (SQLancer++ style) to learn DBMS support and auto-tune generator/oracle gating.
2. Centralize query feature analysis + EXPLAIN parsing to avoid duplicated AST walks and plan parsing (shared by QPG/DQP/CERT/report).
3. Add KQE-lite join-graph coverage guidance to bias join generation toward under-covered structures.
4. Unify expression rewrite/mutation registries for EET/CODDTest/Impo with shared type inference and NULL-safety policies.
5. Refine type compatibility and implicit cast rules using SQL standard guidance to reduce benign type errors.

## Fuzz Efficiency Refactor Plan

1. Finish `observeSQL` precomputed-analysis fast path so parser work is skipped whenever generator/oracle can provide reusable analysis.
2. Run validation for builder/spec equivalence and oracle semantics via targeted oracle/generator test suites and skip-rate checks.

## Continuous Improvement

1. Add a lightweight post-task checklist/template to make journal + TODO improvement capture consistent across agents.
2. Add a CI/doc check that fails when `docs/todo.md` contains completion markers (for example `Latest sync` or `(done)`).
3. Add a helper command/script to create `docs/journal/<YYYY-MM-DD>-*.md` entries and append candidate improvement items into TODO.
4. Add a doc lint check that ensures `AGENTS.md` does not contain a `## Recent updates` section and keeps workflow guidance centralized.
