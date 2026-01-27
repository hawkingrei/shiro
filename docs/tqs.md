# Detecting Logic Bugs of Join Optimizations in DBMS (TQS)

This note summarizes the paper "Detecting Logic Bugs of Join Optimizations in DBMS" with a focus on DSG, ground truth, and KQE, and how they map to implementable components.

---

## 1. One-sentence goal
The paper targets logic bugs in multi-join optimizations by generating complex joins from data-driven schemas and validating results with computable ground truth.

---

## 2. Why random SQL fuzzing is insufficient
Random fuzzing struggles to:

- Systematically cover multi-join structures (4+ tables, mixed join types).
- Provide ground truth, leading to "all-consistent but all-wrong" false negatives.

Key contributions:

1) DSG: data-driven schema and query generation
2) Ground Truth: computable join truth
3) KQE: coverage-driven join exploration

---

## 3. DSG: Data-guided Schema and Query Generation

### 3.1 Wide table view
Assume a dataset can be modeled as a wide table:

```
WideTable(id, a, b, c, d, e)
```

With functional dependencies (FDs), e.g.:

- a -> b
- c -> d

### 3.2 Normalization
Normalize the wide table into multiple tables:

```
T1(a, b)
T2(c, d)
T3(id, a, c, e)
```

This naturally yields joinable schemas:

```
T3 JOIN T1 ON T3.a = T1.a
T3 JOIN T2 ON T3.c = T2.c
```

### 3.3 RowID traceability
Track RowID for each wide-table row:

- RowID = row number in the wide table
- Each normalized row carries its RowID set

### 3.4 Bitmap index (ground truth core)
Maintain a RowID bitmap per normalized row:

```
Bitmap(T1.a = x) = {RowIDs where a = x}
```

Intersect/union bitmaps to compute join truth without executing the DBMS.

---

## 4. Join query generation (random walk)

### 4.1 Schema graph
Treat the schema as a graph:

- Nodes: tables
- Edges: PK/FK relationships

Example:

```
T1 -- T3 -- T2
```

### 4.2 Random walk joins
Randomly walk the graph to build a join chain:

```
T3 -> T1 -> T2 -> T4
```

Corresponding SQL:

```sql
SELECT ...
FROM T3
JOIN T1 ON T3.a = T1.a
JOIN T2 ON T3.c = T2.c
JOIN T4 ON ...
```

Supported join types:

- INNER / LEFT / RIGHT
- SEMI / ANTI
- CROSS

---

## 4.5 GroundTruth in Shiro: bitmap vs exact multiplicity

目前 Shiro 的 GroundTruth 以 RowID bitmap 为核心，适合“唯一键或近似唯一键”的 join。
在 DSG 场景下，如果 join key 不是唯一（例如维表共享 key），bitmap 会低估 join 行数，
因此需要 guard 跳过，导致 `groundtruth:dsg_key_mismatch` 比例较高。

为降低 skip 并保持低误报，我们计划新增“精确行组合计数（multiplicity）”路径：
当 DSG join key 不满足 truth guard 时，改用真实行数据进行 hash join 计数，
并设置上限防止组合爆炸，超限则安全跳过。

### 对比

| 项目 | RowID bitmap (当前) | Exact multiplicity (计划) |
| --- | --- | --- |
| 适用 join key | 唯一 / 近似唯一 | 允许 1:N / N:M |
| 结果精度 | 可能低估 | 精确计数 |
| 误报风险 | 低 | 低（带 cap） |
| 计算成本 | 低 | 中（取决于行数） |
| DSG 兼容 | 需 guard | 可覆盖非唯一 key |

### 实现要点（计划）

1) 在 groundtruth truth 结构中保存标准化表的行数据（normalized rows）。
2) 对 join chain 逐步做 hash join，累加行组合数。
3) 当组合数超过 cap 时直接 skip，避免性能与内存风险。

---

## 5. Ground-truth computation

### 5.1 Core idea
Map the join query back to the wide table and compute logical truth via RowID bitmaps.

### 5.2 Example
Query:

```sql
SELECT *
FROM T3
JOIN T1 ON T3.a = T1.a
WHERE T1.b > 10;
```

Steps:

1) Get bitmap for predicate T1.b > 10
2) Get bitmap for T3
3) Join = bitmap intersection
4) Map back to wide-table rows as ground truth

---

## 6. KQE: Knowledge-guided Query Exploration

### 6.1 Plan-iterative graph
- Nodes: tables/columns
- Edges: join types and predicate relationships
- Track coverage

### 6.2 Adaptive random walk
Bias random walks toward under-covered paths to improve join coverage.

---

## 7. Implementation takeaways

1) Use DSG-lite to increase multi-join complexity
2) Add a GroundTruth oracle for absolute correctness
3) Use KQE-lite to drive join structure coverage

---

## 8. Summary

TQS improves join testing by combining:

- Systematic multi-join generation
- Computable ground truth
- Coverage-driven exploration
