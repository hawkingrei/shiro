# Shiro Fuzz Tool - Agent Notes

Project notes are now documented under `docs/notes/` with supporting references below.

Links:
- [Journal](docs/journal/AGENTS.md)
- [Notes Summary](docs/notes/summary.md)
- [Experience Notes](docs/notes/experience.md)
- [Feature Notes](docs/notes/feature.md)
- [Follow-ups](docs/notes/follow-ups.md)
- [Roadmap](docs/roadmap.md)
- [PQS Notes](docs/pqs.md)
- [PQS Runtime 1105 Triage](docs/notes/pqs-runtime-1105-triage.md)
- [Argus Notes](docs/argus.md)
- [Oracles](docs/oracles/README.md)
- [Glossary](docs/glossary.md)
- [Architecture Decisions](docs/decisions/README.md)

## Workflow guardrails

- Documentation must be written in English.
- Do not use `web/public/cases` as a report source for bug triage; only use local `reports/` case artifacts.
- When updating TiDB issues, collapse large SQL blocks (apply schema/load data) in `<details>` and format the "Run the query" SQL for readability.
- When labeling TiDB issues, treat wrong-result and query-fails-to-execute bugs as `severity/major`; use `severity/moderate` for complex-query planner/compatibility issues that are not confirmed wrong-result or execution-blocking failures.

## Workflow

1. Clarify scope and success criteria from the user request.
2. Stay within requested scope:
   If the user asks for documentation/workflow edits, do not run unrelated code/log analysis.
3. Gather improvement candidates from one of: papers, `docs/todo.md`, or runtime outputs (`logs/` and `reports/`).
4. Select one concrete improvement (or one new fuzz strategy) and define measurable acceptance signals.
5. Implement code changes and required tests.
6. Run checks and linters relevant to the touched scope.
7. Re-review the diff for correctness, scope alignment, and unintended behavior changes.
8. If `reports/` exists, triage for potential TiDB bugs and file TiDB issues for qualified reproducible cases.
9. After completing the task, write a journal entry in `docs/journal/<YYYY-MM-DD>-<topic>.md`.
10. Add new actionable follow-up improvements to `docs/todo.md`.
11. Keep `docs/todo.md` open-items-only; move completed details to `docs/journal/*`.

## Signal policy

- Non-reproducible cases are still valid signals: do not suppress case capture only because current minimize/replay cannot reproduce yet.
- Inapplicable/invalid optimizer hints are also test signals: keep observing and logging these outcomes instead of filtering them out by default, because behavior changes over time can make previously inapplicable hints become applicable.
- Prefer generation-time constraints over runtime skip growth: when a skip class is predictable from query/schema context, add guardrails in generator/query-builder/oracle profile constraints first to reduce skip volume at the source.
- For unavoidable skips, keep observability first-class: require stable `skip_reason` taxonomy plus interval/detail diagnostics and counters so skip drift remains measurable in logs/reports.
