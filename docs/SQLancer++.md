## SQLancer++ (Adaptive SQL Generator for Scalable DBMS Testing)

### What it is
SQLancer++ is a testing platform that aims to **scale automated DBMS testing** across many SQL dialects. Its core contribution is an **adaptive SQL statement generator** that learns which SQL features are supported by a target DBMS and focuses generation on those features over time.

### Key idea
Generators are expensive to build per DBMS because SQL dialects vary. SQLancer++ treats SQL features (e.g., joins, window functions, specific clause combinations) as **learnable capabilities** and uses execution feedback to infer which features are supported. The generator adapts to maximize valid statements without hand-coding DBMS-specific generators.

### Architecture (high level)
1. **State building**: generate DDL/DML to build a database state.
2. **Adaptive query generation**: emit queries with various features.
3. **Feedback mechanism**:
   - DDL/DML features: mark unsupported if repeated failures exceed a threshold.
   - Query features: use a Bayesian inference model to estimate support likelihood.
4. **Oracle validation**: apply testing oracles (e.g., TLP, NoREC).
5. **Bug prioritization**: compare feature sets of new failures against prior bug-inducing cases to reduce duplicates.

### Evaluation highlights (paper claims)
- Evaluated across **18 DBMSs**.
- Discovered **196 unique bugs**, **180** of which were fixed after reporting.
- Achieved high validity rates quickly by adapting feature selection.

### Why it matters here
SQLancer++ provides a blueprint for **adaptive generation** that could reduce manual tuning in Shiro, especially for heterogeneous SQL dialects and evolving feature sets. Its feature-feedback loop is relevant to improving statement validity while preserving oracle coverage.
