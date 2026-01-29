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

## Recent updates

- Added a constraints-based SelectQueryBuilder to centralize oracle query requirements and expose builder skip reasons.
- Tightened DSG join key alignment (k0 or k{idx}) and adjusted GroundTruth join extraction to reduce edge mismatch.
- Improved CERT generation by retrying base rows and allowing base-without-where fallback.
