# Testing Database Engines via Pivoted Query Synthesis (PQS)

## Source
- Paper: *Testing Database Engines via Pivoted Query Synthesis* (OSDI 2020), Manuel Rigger and Zhendong Su.
- URL: https://www.usenix.org/conference/osdi20/presentation/rigger

## What PQS is
PQS is a containment oracle for SQL logic-bug testing. It selects one random row (the **pivot row**), synthesizes predicates that are guaranteed to evaluate to `TRUE` for that pivot row, and then checks whether the final query result still contains that row.

If the pivot row is missing, the DBMS likely has a logic bug in optimization or execution.

## Core algorithm
1. Build a random database state (tables, rows, optional views/indexes).
2. Pick a pivot row from the current state.
3. Randomly generate expression ASTs using valid schema references.
4. Evaluate ASTs on the pivot row and rectify each predicate to `TRUE` under SQL three-valued logic.
5. Inject rectified predicates into `WHERE` and/or `JOIN ON` clauses of a generated query.
6. Execute the query and check whether the pivot row is contained in the result.
7. Report a bug when containment is violated.

## Why it matters for Shiro
- It provides a direct row-level correctness signal, complementary to metamorphic oracles such as TLP/NoREC/EET.
- It is optimizer-sensitive and good at exposing wrong-row-elimination bugs.
- It is DBMS-agnostic in principle, with most complexity isolated in expression evaluation and SQL dialect handling.

## Practical limitations
- It validates containment of a pivot row, not full result-set equivalence.
- It is weaker for aggregate correctness, full cardinality validation, and ordering correctness.
- Expression interpreter fidelity is critical for edge semantics (`NULL`, casts, collation, special operators).
- Complex SQL features increase implementation cost (window frames, recursive CTE, dialect-specific functions).

## Suggested staged rollout in Shiro
1. **PQS v1**: single-table `SELECT ... WHERE ...` containment with basic boolean/comparison predicates.
2. **PQS v2**: multi-table joins with pivot bindings across aliases and `JOIN ON`.
3. **PQS v3**: add subquery predicates (`IN/EXISTS/ANY/ALL`) and derived tables.
4. **PQS v4**: add optional stress features (`DISTINCT`, `GROUP BY`, indexes, views) behind capability gates.
5. Add per-stage observability: `pqs_runs`, `pqs_skip_reasons`, `pqs_containment_failures`, and reducer-friendly bug artifacts.

## Shiro status
- **PQS v1 implemented (single-table)**: pick a pivot row, build equality/`IS NULL` predicates over a subset of columns, and verify containment via a pivot presence check.
- **PQS v1.5 implemented (basic joins)**: sample pivots via `id`-range selection (avoid `ORDER BY RAND()`), build alias-aware containment checks across `JOIN ... USING (id)` for two-table pivots, and use `LIMIT 1` existence probes instead of `COUNT(*)`.
- **3VL rectifier implemented**: evaluate and rectify predicates under SQL three-valued logic for PQS containment checks (excluding join-aware `JOIN ON` and subqueries).
- **PQS v2 implemented (join-aware `JOIN ON`)**: build join predicates with pivot-bound equality plus rectified predicates over visible tables, and record join-on metadata for reports.
- **PQS v3 implemented (subqueries + derived tables)**: add `EXISTS`/`IN`/`ANY`/`ALL` subquery predicates plus derived-table wrapping with pivot-safe filters.
- **Pending**: expanded stress features (e.g., `DISTINCT`, `GROUP BY`, indexes, views) behind capability gates.
