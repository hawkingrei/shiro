# Feature Notes

## Report UI formatting
- Align EXPLAIN output by tab-separated columns; preserve original schema.sql text and fold it by default.
- Reordered report layout: schema/data at top, Expected/Actual blocks next, EXPLAIN blocks after, diffs at the bottom.
- EXPLAIN diffs render via `react-diff-viewer-continued` (split view, diff-only, word-level highlights, extra context lines).
- Diff blocks scroll horizontally and content width is capped for readability.

## Web linting
- Added ESLint flat config using `eslint-config-next` and wired `npm run lint` to `eslint .`.
- Added CI step to run `npm run lint` in `web`.

## Generator randomness
- Randomized DATE/DATETIME/TIMESTAMP literals across year/month/day and full time range (2023-2026), with leap-year aware day bounds.
- TQS randomValue now uses the same broader date/time range with leap-year handling.
- DATE/DATETIME/TIMESTAMP equality predicates prefer sampled INSERT values to keep match rates after randomization.

## Generator observability
- Added subquery coverage logging (allowed/disabled/has/attempted/built/failed) plus disallow-reason stats per interval.
- Added per-oracle subquery coverage logging to isolate DQP/TLP overrides.
- Added IN(subquery)/NOT IN(subquery) counters in interval logs.

## EET oracle
- Skip EET cases where ORDER BY is constant under LIMIT to avoid nondeterministic sampling.
- Relaxed EET/CODDTest builder subquery constraint; predicates still gate subquery forms.
- Predicate guard allows EXISTS/IN subquery forms.

## Generator tuning
- Increased PredicateSubqueryScale and PredicateExistsProb to raise EXISTS coverage.

## Group By ordinals
- Added optional `GROUP BY 1,2` rendering by wrapping group keys as ordinals while retaining the base expressions for semantic checks.
- Group-by ordinal probability is controlled by the feature bandit (not config).

## Report UI
- Added a flaky tag when expected/actual EXPLAIN match but signatures differ.
