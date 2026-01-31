# Feature Notes

## Report UI formatting
- Align EXPLAIN output by tab-separated columns; preserve original schema.sql text and fold it by default.
- Reordered report layout: schema/data at top, Expected/Actual blocks next, EXPLAIN blocks after, diffs at the bottom.
- EXPLAIN diffs render via `react-diff-viewer-continued` (split view, diff-only, word-level highlights, extra context lines).
- Diff blocks scroll horizontally and content width is capped for readability.

## Web linting
- Added ESLint flat config using `eslint-config-next` and wired `npm run lint` to `eslint .`.
- Added CI step to run `npm run lint` in `web`.
