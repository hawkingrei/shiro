## EET (Equivalent Expression Transformation)

### What it is
Equivalent Expression Transformation (EET) is a logic-bug testing approach for DBMSs that transforms **expressions** inside a query into semantically equivalent forms and then checks whether the transformed query returns the same result as the original.

### Key idea
Instead of rewriting whole queries (which often requires restricting query shapes), EET rewrites **expressions** at the AST level. If each expression rewrite is semantics-preserving, then the entire query should remain equivalent:

```
E ≡ E'  =>  DB(Q) ≡ DB(Q'), where Q' = Q with E replaced by E'
```

This makes the method applicable to **arbitrary queries**, not just pattern-limited ones.

### Core transformations (paper examples)
- **Determined boolean expressions**: replace boolean expressions with equivalent forms that preserve truth tables.
- **Redundant branch structures**: introduce equivalent conditional structures that should not change results.

### Architecture (high level)
1. Parse query to AST.
2. Traverse expressions.
3. Apply equivalence-preserving rewrites.
4. Emit transformed query.
5. Compare results of original vs. transformed query.

### Evaluation highlights (paper claims)
- Evaluated on MySQL, PostgreSQL, SQLite, ClickHouse, and TiDB.
- Found **66 unique bugs**, **35** of which are logic bugs.
- Reported and confirmed most findings; **37** were fixed at the time of writing.

### Why it matters here
EET shows that **expression-level** rewrites can test complex SQL without over-constraining query shapes. This aligns with Shiro's goals of maintaining broad query coverage while still using strong equivalence oracles.
