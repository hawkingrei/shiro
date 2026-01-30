# Equivalent Expression Transformation (EET)

## Background
Expression-level rewrites can test optimizer correctness without constraining query shapes. Instead of rewriting whole queries, EET rewrites expressions inside a query into semantically equivalent forms and checks whether results match.

## Oracle Form
- Original: `SELECT ... WHERE P`
- Transformed: `SELECT ... WHERE T(P)`
- Compare signatures (row count + checksum) between the two.

## Implementation in Shiro
- Operates on `SELECT` statements with `WHERE`, `HAVING`, or `JOIN ON` predicates.
- Requires deterministic predicates and disallows subqueries for now.
- Uses TiDB parser AST to rewrite predicates.

## Rewrite Set (current)
Boolean identities:
- `P` -> `NOT (NOT P)`
- `P` -> `P AND TRUE`
- `P` -> `P OR FALSE`

Type-aware literal identities (first-match within predicates):
- Numeric literal `N` -> `N + 0`
- String literal `S` -> `CONCAT(S, '')`
- Date/time literal `D` -> `ADDDATE(D, INTERVAL 0 DAY)`

## Rewrite Weights
EET rewrite selection is weighted. Configure via:

```yaml
oracles:
  eet_rewrites:
    double_not: 4
    and_true: 3
    or_false: 3
    numeric_identity: 2
    string_identity: 2
    date_identity: 2
```

Unavailable literal kinds are masked to weight 0. If all weights are zero, EET falls back to boolean rewrites.

These are three-valued logic safe and do not change predicate semantics.

## Guardrails
- Only applies to deterministic predicates.
- Requires at least one predicate in `WHERE`, `HAVING`, or `JOIN ON`.
- Skips non-`SELECT` statements or parse failures.

## Expected Coverage
Good at catching optimizer bugs in predicate evaluation and simplification without relying on query-shape constraints.

## TODO
- Extend rewrites to `HAVING` and `JOIN ON` conditions.
- Add type-aware expression rewrites (e.g., numeric/boolean identities).
- Consider safe rewrites involving `IS TRUE/FALSE` and `CASE` once guardrails are in place.
