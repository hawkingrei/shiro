# Glossary

- Oracle: A differential/consistency check that flags mismatches (e.g., result count or plan signatures).
- Stage1: Initial generation stage that focuses on producing valid SQL and basic coverage.
- DSG: Data-driven Schema Generation; TQS-style schema with base/dim tables and key-driven joins.
- QPG: Query Plan Guidance; coverage-driven query shaping based on EXPLAIN plan signatures.
- Impo: Mutation-based oracle that compares base vs mutated queries for logical inconsistencies.
- CODDTest: Constant optimization driven testing; checks constant folding/propagation equivalence.
- TQS: Testing Join Optimizations via DSG + ground truth + coverage-driven random walk.
- NoREC: Non-optimizing reference engine construction; compares optimized vs unoptimized counts.
- DQP: Differential Query Plans; same query under different plan hints/vars should match.
- CERT: Cardinality Estimation Regression Test; monotonicity check on estimated rows.
- TLP: Ternary Logic Partitioning; partition predicates and compare results.
- DQE: Differential Query Execution; compare DML effects via partitioned predicates.
- Plan Replayer: TiDB tool for capturing plans for reproduction.
