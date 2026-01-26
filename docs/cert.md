# CERT: Finding Performance Issues in Database Systems Through the Lens of Cardinality Estimation

## Background
Performance regressions often stem from cardinality estimation errors. Even when results are correct, bad estimates lead to poor plan choices. Traditional correctness oracles do not catch this class of problems.

## Core Idea
CERT treats the intuition "estRows should be monotonic under tighter predicates" as an oracle. It adds a restrictive predicate to a query and checks whether EXPLAIN's estimated rows violate monotonicity.

## Key Mechanics
1. Build a base query Q and a restricted query Q' (e.g., add an extra filter).
2. Use EXPLAIN only to obtain estimated rows, avoiding execution noise.
3. If estRows(Q') is significantly larger than estRows(Q), flag a potential estimation bug.

## Oracle Form
- Q:  SELECT ... WHERE p
- Q': SELECT ... WHERE p AND p'
- If estRows(Q') > estRows(Q) * (1 + tolerance), flag a bug.

## Scope and Limitations
- Targets performance issues, not result correctness.
- Must avoid unstable statistics or tiny samples that introduce noise.
- Complex queries (joins, subqueries, aggregates) can be noisy and should be constrained.

## Impact
CERT provides an automated, approximation-based oracle for detecting optimizer and statistics regressions.
