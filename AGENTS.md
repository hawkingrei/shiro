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

- Added a constraints-based SelectQueryBuilder to centralize oracle query requirements and expose builder skip reasons.
- Tightened DSG join key alignment (k0 or k{idx}) and adjusted GroundTruth join extraction to reduce edge mismatch.
- Improved CERT generation by retrying base rows and allowing base-without-where fallback.
- DSG: fall back from index-prefix join columns to k* columns and classify missing join keys separately from DSG mismatch.
- Enabled Impo init for non-recursive WITH clauses and added mutation coverage counters.
- Ran `go test ./...` (2026-01-29).
- Reviewed `logs/shiro.log` (2026-01-30) and captured CERT reports plus error/skip stats.
- Extracted `cert:base_explain_error` / `Unknown column` occurrences (2026-01-30).
- Checked join/CTE scope and view/CTE column reference issues (2026-01-30).
- Tightened JOIN ON scope validation, added CERT scope validation, and added tests (2026-01-30).
- Rechecked recent `logs/shiro.log` windows (2026-01-30), focusing on WARN and CERT reports.
- Added TLP error_reason + whitelist skips and tests (2026-01-30).
- Recorded TLP error example SQL for attribution (2026-01-30).
- Reviewed latest logs and `reports/case_*` (2026-01-30).
- Fixed TLP UNION/ORDER BY 1221 errors and added tests (2026-01-30).
- Fixed TLP USING column qualification 1054 errors and added tests (2026-01-30).
- Normalized USING merged column references and allowed TLP to proceed (2026-01-30).
- Rewrote TLP USING to ON and qualified unqualified columns (2026-01-30).
- Reviewed `logs/shiro.log` after latest run (2026-01-30).
- Removed unused TLP helper functions and reran `go test ./...` (2026-01-30).
- Added a TLP ORDER BY/UNION comment, removed the temporary TLP config file, and cleaned the roadmap item list (2026-01-30).
- Removed the CERT weight configuration, fixed CERT sampling at a constant rate, and excluded CERT from oracle bandit selection (2026-01-30).
- Ran `go test ./...` after CERT sampling changes (2026-01-30).
- Added CERT sampling ratio logging and documented fixed sampling behavior (2026-01-30).
- Ran `go test ./...` after adding CERT sampling logging (2026-01-30).
- Fixed PR description line breaks by editing PR body via `gh pr edit --body-file` (2026-01-30).
- Refined CERT sampling counter updates to reduce lock contention and avoid counting fallback picks (2026-01-30).
- Minimized lock contention by keeping bandit picks under a single statsMu scope and refactoring non-CERT selection helpers (2026-01-30).
