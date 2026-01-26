# Pinolo: Detecting Logical Bugs in DBMS with Approximate Query Synthesis

## Background
Logical bugs are among the hardest DBMS bugs to catch: queries return wrong results without errors. Traditional equivalence rewrites have limited coverage and are hard to construct for complex SQL.

## Core Idea
Pinolo uses approximate query synthesis as an oracle. It generates over-approximate and under-approximate variants that should satisfy containment relationships. Violations indicate bugs.

## Key Mechanics
1. Define approximate mutators that weaken or strengthen relations, predicates, and comparisons.
2. Traverse the AST top-down to generate Q_over and Q_under.
3. Check containment:
   - Over-approx: R(Q) should be contained in R(Q_over).
   - Under-approx: R(Q_under) should be contained in R(Q).

## Oracle Form
- Let the original query be Q:
  - Q_over: weaken predicates or expand relations, expect R(Q) ⊆ R(Q_over)
  - Q_under: strengthen predicates or shrink relations, expect R(Q_under) ⊆ R(Q)
- If containment fails, flag a bug.

## Example Mutations
- Relation: UNION ALL ↔ UNION, add/remove DISTINCT.
- Predicate: replace WHERE/HAVING/ON with TRUE or FALSE.
- Comparison: > ↔ >=, < ↔ <=, ANY ↔ ALL.
- IN list: add NULL to weaken the constraint.

## Assumptions and Limits
- Proof assumes NULL-free data; three-valued logic can break containment.
- Non-deterministic functions, outer joins, and window functions require guards.

## Impact
Pinolo provides a stronger oracle than pure equivalence by leveraging containment, covering broader SQL structures and optimizer/executor paths.
