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

## 4.5 GroundTruth in Shiro: Bitmap vs Exact Multiplicity

Shiro's GroundTruth currently uses RowID bitmaps, which work best with unique or near-unique join keys.
In DSG, when join keys are non-unique (e.g., shared keys in dimension tables), bitmap truth undercounts,
so the oracle must guard and skip, leading to a high `groundtruth:dsg_key_mismatch` rate.

To reduce skips while keeping low false positives, Shiro now includes an exact multiplicity path:
when DSG join keys do not satisfy the truth guard, it switches to hash-join counting over normalized rows,
and caps intermediate results to prevent blow-ups (exceeding the cap safely skips).

### Comparison

| Item | RowID bitmap (Current) | Exact multiplicity (Current) |
| --- | --- | --- |
| Join key support | Unique / near-unique | Allows 1:N / N:M |
| Accuracy | May undercount | Exact counts |
| False-positive risk | Low | Low (with caps) |
| Cost | Low | Medium (depends on row counts) |
| DSG compatibility | Requires guard | Covers non-unique keys |

### Implementation Notes (Current)

1) Store normalized row data in the groundtruth structure.
2) Perform hash joins across the join chain and accumulate multiplicity counts.
3) Skip when intermediate rows exceed the cap to avoid performance and memory risks.

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
