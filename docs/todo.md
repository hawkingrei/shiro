# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.
Latest sync: aligned TODO with follow-ups and removed completed items (2026-02-08).
Last review: 2026-02-07. Added broader SQL2023 regression coverage (recursive CTE guards, FULL JOIN emulation edge cases, window determinism/named-window overrides, GROUPING ordinal unwrap), oracle SQL helper fast-path/build tests, TiDB-compat regressions to keep `INTERSECT ALL`/`EXCEPT ALL` disabled, nil-operand hardening for expression determinism/build paths, runner-side `error_reason/bug_hint` classification for PlanCache and GroundTruth mismatch reporting, P1 batch-1 skip-reduction overrides for GroundTruth/CODDTest, systemic NoREC constraint tightening via builder-level set-op disallow + query guard reasons, scope-manager enforcement for `USING` qualified-column visibility with regression tests, NATURAL JOIN column-visibility enforcement, set-op ORDER/LIMIT normalization, inline-subquery `WITH` guard unification, canonical `plan_reference_missing` bug-hint alignment, GroundTruth DSG mismatch reason taxonomy with retry-on-mismatch picking, GroundTruth USING-to-ON rewrite for ambiguity reduction, CODDTest build-time null/type prechecks, EET/TLP signature prechecks for invalid ORDER BY ordinals and known-table column visibility, EET distinct-order/scope pre-guards, top-level interval aggregation for `groundtruth_dsg_mismatch_reason`, summary/index top-level propagation of `groundtruth_dsg_mismatch_reason`, builder retry tuning for constrained oracles, GroundTruth/Impo retry-on-invalid-seed behavior, and summary-level `minimize_status` with interrupted fallback.

## Generator / Oracles

1. CERT: add stronger guardrails for DISTINCT/ORDER BY/ONLY_FULL_GROUP_BY.
2. DQP/TLP: reduce predicate_guard frequency without weakening semantic assumptions.
3. DQP: expand plan-hint coverage and add optimizer variables if needed.
4. Consider increasing `groundtruth_max_rows` to reduce `groundtruth:table_rows_exceeded` skips.
5. Consider lowering DSG per-table row counts to stay under the GroundTruth cap.
6. Split join-only vs join+filter predicates into explicit strategies with separate weights and observability.
7. Wire GroundTruth join key extraction into oracle execution for JoinEdge building.
8. Refactor per-oracle generator overrides into data-driven capability profiles to reduce duplicated toggles.
9. Roll out `set_operations` / `derived_tables` / `quantified_subqueries` with profile-based oracle gating and observability before default enablement.
10. Extend grouping support from `WITH ROLLUP` to `GROUPING SETS` / `CUBE` with profile-based fallback for unsupported dialects.
11. Add per-feature observability counters for `natural_join`, `full_join_emulation`, `recursive_cte`, `window_frame`, and `interval_arith`.

## PQS (Rigger OSDI20)

1. Add a lightweight expression evaluator + rectifier for three-valued logic (`TRUE/FALSE/NULL`) that can force predicate truth for the sampled pivot row.
2. Build PQS query synthesis paths for `JOIN ON`, including join-aware pivot bindings and skip reasons when rectification is unsafe or unsupported.
3. Extend containment assertion SQL templates and reducer-friendly report fields for join-path PQS.
4. Add staged tests for join-path containment and rectification invariants. (join containment SQL test added; rectifier tests pending)

## Reporting / Aggregation

1. Build a report index for on-demand loading (replace monolithic `report.json` for large runs).
2. Add index writer with sharding and optional gzip support for CDN/S3.
3. Update report UI to load the index first, then fetch individual `summary.json` files on demand with paging and caching.
4. Add `report_base_url` (or reuse existing config) to allow loading reports from HTTP/S3 endpoints with CORS guidance.
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

1. Validation: update tests for builder/spec equivalence and ensure oracle semantics remain unchanged; run targeted oracle/generator tests to confirm skip reduction and coverage stability.
