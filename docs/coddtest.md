# Constant Optimization Driven Database System Testing (CODDTest)

## Background
Optimizers perform constant folding and propagation at compile time. If these optimizations are flawed, queries can return wrong results, yet equivalence-based testing may not reliably trigger them.

## Core Idea
CODDTest turns constant optimization into an oracle. It constructs semantically equivalent query pairs (one relying on constant optimization and one explicitly expanded) and compares their results.

## Key Mechanics
1. Select a query with predicates or expressions as a seed.
2. Generate a "constantized" version, e.g., expand expressions via CASE.
3. Compare result signatures or row sets.

## Oracle Form
- Q:  SELECT ... WHERE p(x)
- Q': SELECT ... WHERE CASE WHEN p(x) THEN TRUE ELSE FALSE END
- If results differ, flag a bug.

## Scope and Limitations
- Sensitive to NULL semantics; three-valued logic can break equivalence.
- Avoid non-deterministic functions and implicit casts that add noise.
- Best for expression simplification and constant propagation paths.

## Impact
CODDTest provides a strong oracle for optimizer expression handling, systematically exercising constant-related logic.
