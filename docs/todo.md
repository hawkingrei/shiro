# Today Plan (TQS Integration)

以下是今天的开发计划（阶段化执行顺序）。重点在多表 JOIN 复杂度提升、Ground-Truth Oracle 原型、可观察性建设。覆盖率目标（> 0.55）放在审核阶段，优先级靠后。

---

## Phase A - Multi-Join Complexity (High Priority)

1. Join shape diversity: chain, star, snowflake.
2. Mixed join type sequences: inner/outer/semi/anti/cross.

## Phase B - GroundTruth Oracle Prototype

1. RowID mapping structure.
2. Bitmap join truth computation.
3. SELECT + JOIN truth validation.
4. Top-N truth logic (ORDER BY + LIMIT).

## Phase C - KQE-lite Coverage Guidance

1. Join graph coverage score.
2. Join type sequence coverage score.
3. Adaptive generation weights for low coverage structures.

## Phase C2 - TQS History + Walk Alignment (Current)

## Phase D - Observability & Reporting

1. Runtime stats: join depth distribution, join type combos, coverage delta.
2. GroundTruth mismatch reporting.
3. Run label support with commit/branch in report metadata.

## Phase D2 - Predicate/JoinGraph Extensions (Next)

1. Extend join-graph sampling into IN-list literals (align list literals with join-graph column types).
2. Extend join-graph sampling into scalar subquery SELECT list (non-COUNT paths).
3. Add join-graph-aware predicate generation for HAVING/ORDER BY filters.
4. Add join-graph stats for subquery predicates (separate ratio counters).
5. Add optional hint injection for TQS paths (controlled, low-probability, for optimizer stress).

## Phase E - Systematic Tests (Late Stage)

1. Unit tests for bitmap join correctness.
2. Integration coverage tests.
3. Coverage target: > 0.55 (audit stage only).
