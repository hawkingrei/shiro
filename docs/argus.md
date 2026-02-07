## Automated Discovery of Test Oracles for DBMSs Using LLMs (Argus)

### Source
- Paper: *Automated Discovery of Test Oracles for Database Management Systems Using LLMs* (arXiv:2510.06663v1, 2025)
- Authors: Qiuyang Mang, Runyuan He, Suyang Zhong, Xiaoxuan Liu, Huanchen Zhang, Alvin Cheung
- URL: https://arxiv.org/abs/2510.06663

### Problem
Modern DBMS testing can automatically execute oracles, but **oracle design** is still manual and slow. This becomes a bottleneck for finding new logic bugs.

### Core Idea
Argus separates the workflow into two phases:
1. **Offline oracle discovery** with LLMs.
2. **High-throughput online instantiation and execution** without frequent LLM calls.

The key abstraction is a **Constrained Abstract Query (CAQ)**:
- A SQL skeleton with placeholders (table, expression, predicate, etc.).
- Each placeholder has type/scope constraints.
- An oracle is represented as an **equivalent CAQ pair**: two CAQs that should be semantically equivalent for all valid instantiations.

### Pipeline
1. Build schema and seed CAQs.
2. Ask an LLM to generate candidate equivalent CAQs.
3. Use a SQL equivalence solver (e.g., SQLSolver) to formally verify candidate equivalence.
4. Build a reusable SQL snippet corpus (LLM + grammar generator).
5. Instantiate verified CAQ pairs into many concrete query pairs.
6. Run query pairs on target DBMS and compare outputs.

This design addresses:
- **Soundness**: filtered by formal equivalence checking.
- **Scalability**: expensive LLM work happens offline, while online testing reuses validated templates and snippet corpus.

### Reported Results (paper claims)
- Evaluated on 5 DBMSs: Dolt, DuckDB, MySQL, PostgreSQL, TiDB.
- Found 40 previously unknown bugs, including 35 logic bugs.
- 36 confirmed, 26 fixed.
- Up to 1.19x code coverage gain and 6.43x metamorphic coverage gain versus baselines in their setup.

### Why It Matters for Shiro
- It is a practical path to go beyond manually curated oracle families.
- The CAQ abstraction can unify current rewrite-style and equivalence-style checks.
- Solver-backed filtering is useful for reducing false positives from LLM-generated transformations.
- Reusable snippet corpus aligns with Shiro’s existing generator infrastructure.

### Integration Sketch for Shiro
1. Add a CAQ IR and placeholder constraint checker.
2. Start with a small seed oracle set (e.g., TLP-like forms, simple set-op identities).
3. Add a solver-validation stage to accept/reject candidate oracle pairs.
4. Reuse generator outputs as snippet corpus for placeholder instantiation.
5. Track oracle-level metrics: candidate count, solver pass rate, instantiated pair count, bug yield, duplicate rate.

### Limitations and Risks
- SQL equivalence provers support limited SQL features and dialect coverage.
- LLM-generated candidates can still be noisy; robust filtering and dedup are required.
- Feature-rich SQL (functions, JSON, dialect-specific semantics) may exceed prover capabilities.
- Engineering complexity is non-trivial (IR, solver bridge, corpus management, runtime validation).
