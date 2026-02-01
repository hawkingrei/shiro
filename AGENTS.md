# Shiro Fuzz Tool - Agent Notes

Project notes are now documented under `docs/notes/` with supporting references below.

Links:
- [Notes Summary](docs/notes/summary.md)
- [Experience Notes](docs/notes/experience.md)
- [Feature Notes](docs/notes/feature.md)
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
- Report UI formatting updates moved to docs/notes/summary.md (2026-01-31).
- Web linting notes moved to docs/notes/feature.md (2026-01-31).
- CI linting updates are tracked in docs/notes/feature.md (2026-01-31).
- Generator randomness updates are tracked in docs/notes/feature.md (2026-01-31).
- TQS randomValue date/time randomness updates are tracked in docs/notes/feature.md (2026-01-31).
- Date sample reuse for predicate equality is tracked in docs/notes/feature.md (2026-01-31).
- Subquery coverage logging and disallow-reason stats are tracked in docs/notes/feature.md (2026-02-01).
- EET skips constant ORDER BY with LIMIT to avoid nondeterministic sampling (2026-02-01).
- Enabled subqueries for non-DQP/TLP oracles via runner overrides (2026-02-01).
- Enabled NOT EXISTS/NOT IN for non-DQP/TLP oracles via runner overrides (2026-02-01).
- Relaxed EET/CODDTest builder subquery constraints and raised PredicateExistsProb to 80 (2026-02-01).
- Added per-oracle subquery coverage logs to avoid DQP/TLP override confusion (2026-02-01).
- Raised PredicateSubqueryScale/PredicateExistsProb and allowed EET predicate subqueries (2026-02-01).
- Added IN(subquery)/NOT IN(subquery) counters to interval logs (2026-02-01).
- Added flaky tag for cases with matching EXPLAIN plans but signature mismatches (2026-02-01).
