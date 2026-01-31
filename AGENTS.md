# Shiro Fuzz Tool - Agent Notes

Project notes are now documented under `docs/notes/` with supporting references below.

Links:
- [Notes Summary](docs/notes/summary.md)
- [Experience Notes](docs/notes/experience.md)
- [Follow-ups](docs/notes/follow-ups.md)
- [Roadmap](docs/roadmap.md)
- [Oracles](docs/oracles/README.md)
- [Glossary](docs/glossary.md)
- [Architecture Decisions](docs/decisions/README.md)

## Workflow guardrails

- After each task completes, review and update `AGENTS.md` and `docs/todo.md`, removing completed items and syncing current progress.
- Documentation must be written in English.

## Recent updates

- Added a constraints-based SelectQueryBuilder, tightened DSG join alignment, and improved GroundTruth/CERT generation guardrails (2026-01-30).
- Fixed JOIN ON scope validation, added CERT scope checks, and updated tests (2026-01-30).
- TLP: added error_reason/whitelist skips, fixed UNION/ORDER BY 1221, rewrote USING to ON with column qualification, removed unused helpers, and updated tests/logging (2026-01-30).
- CERT sampling: removed weight config, fixed sampling at a constant rate, added sampling ratio logs, and refined bandit/lock handling with safe fallbacks (2026-01-30).
- DQP: treat LEADING mismatches as TiDB bugs; subqueries remain disabled via oracle overrides (2026-01-30).
- EXISTS metrics: regex matching for whitespace/newlines; TODO to improve EXISTS/NOT EXISTS coverage (2026-01-30).
- Reviewed logs/reports, captured CERT/TLP/DQP examples, and fixed PR description line breaks (2026-01-30).
- Ran `go test ./...` (2026-01-29, 2026-01-30).
- Added paper notes: EET and SQLancer++ summaries under docs (2026-01-30).
- Added the EET oracle using AST-level predicate rewrites and signature comparison (2026-01-30).
- Expanded EET to cover HAVING/JOIN ON and added type-aware literal identities (2026-01-30).
- Added weighted EET rewrite selection to tune boolean vs literal identities (2026-01-30).
- Added TODO to track EET rewrite coverage logging needs (2026-01-30).
- Addressed EET review fixes (date rewrite form, parser driver imports, predicate target shuffle, doc TODO updates) (2026-01-30).
- Loosened EET predicate guards (allow NOT/IS NULL) and raised DML/EXISTS subquery probabilities (2026-01-31).
- Frontend: aligned EXPLAIN output columns and added EXPLAIN diff views (2026-01-31).
- Frontend: preserve logical plan tree lines while aligning EXPLAIN columns (2026-01-31).
- Frontend: keep EXPLAIN first column indentation when aligning columns (2026-01-31).
- Frontend: render EXPLAIN diffs side-by-side (2026-01-31).
- Frontend: align EXPLAIN columns without tree lines affecting widths; render diffs in a two-column table (2026-01-31).
- Frontend: keep tree lines splittable for column alignment; hide unchanged EXPLAIN diff lines by default (2026-01-31).
- Frontend: align EXPLAIN content using tab-separated columns in text output (2026-01-31).
- Frontend: align Expected/Actual EXPLAIN headings by pairing blocks in a two-column grid (2026-01-31).
- Frontend: wrap EXPLAIN diff tables to avoid clipping and enable horizontal scroll (2026-01-31).
- Frontend: pair add/del lines in EXPLAIN diffs and increase diff contrast (2026-01-31).
- Frontend: widen EXPLAIN diff tables to split columns evenly (2026-01-31).
- Frontend: make paired blocks span the full grid width for diff readability (2026-01-31).
- Frontend: keep diff blocks in the left two columns so data stays in the right column (2026-01-31).
- Frontend: switch EXPLAIN diff rendering to react-diff-viewer-continued (2026-01-31).
- Frontend: move diff blocks to a full-width row below the three-column layout (2026-01-31).
- Frontend: move schema/data to the left column and render data before schema (2026-01-31).
- Frontend: render schema.sql without formatting and tighten block spacing (2026-01-31).
- Frontend: tune diff viewer (diffWordsWithSpace, word-level highlights, context lines) (2026-01-31).
- Frontend: render schema above data in the left column (2026-01-31).
- Frontend: split Expected/Actual rows above EXPLAIN blocks, then diff below (2026-01-31).
- Frontend: stack meta (schema/data) above Expected/Actual rows, then diff at the bottom (2026-01-31).
- Frontend: cap report content width for readability (2026-01-31).
- Frontend: fold schema.sql by default (2026-01-31).
