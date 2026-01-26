# Detecting Optimization Bugs in Database Engines via Non-Optimizing Reference Engine Construction (NoREC)

## Background
Optimizer bugs can return wrong results without errors. Traditional differential testing relies on complex equivalence rewrites, which are limited in coverage and hard to scale to complex queries.

## Core Idea
NoREC constructs a non-optimizing reference query that forces row-by-row predicate evaluation. If the optimized query count differs from the reference count, it likely exposes an optimizer logic bug.

## Key Mechanics
1. Rewrite the original query into a form that evaluates the predicate per row and counts TRUE results, avoiding optimizer rewrites.
2. Use the original query count as the baseline and the reference query count as the control.
3. Compare counts only to avoid noise from ordering and projection differences.

## Oracle Form
- Original: SELECT ... WHERE predicate
- Reference: SELECT COUNT(*) FROM (SELECT predicate IS TRUE AS p FROM ...) t WHERE p
- If counts differ, flag a bug.

## Scope and Limitations
- Targets optimizer logic errors such as predicate pushdown, logical rewrites, and expression simplification.
- Assumes predicates are safe to evaluate and project; avoid side effects or non-deterministic functions.
- Count-only comparison may miss wrong-row-content bugs with identical counts.

## Impact
NoREC bypasses equivalence rewrite complexity by constructing a non-optimizing control, improving discovery of optimizer logic errors.
