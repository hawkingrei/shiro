# PQS Runtime 1105 Triage

Date: 2026-02-19
Status: Closed (waiting for new reproducible sample)

## Scope

This note closes the follow-up item:

- Triage PQS join runtime MySQL 1105 (`index out of range`) from the 2026-02-09 run.

## Evidence

1. Historical signal exists in project notes:
- `AGENTS.md` records one PQS runtime error (`Error 1105: index out of range`) from the 2026-02-09 run and marks it as non-reproducible during minimize.
- `docs/todo.md` and `docs/notes/follow-ups.md` previously tracked the same item.

2. Current repository reports do not contain this signature:
- Current `reports/*/summary.json` count: 9
- Matches for `bug_hint=tidb:runtime_error`, `runtime_1105`, `index out of range`: 0

3. Runner classification is now explicit:
- `internal/runner/runner_errors.go` maps PQS+1105+`index out of range` to `error_reason=pqs:runtime_1105` and `bug_hint=tidb:runtime_error`.

## Decision

Do not file an upstream TiDB issue yet.

Reasoning:

- The only known historical hit was non-reproducible.
- No current local report sample matches this signature.
- We now have stable error classification for future aggregation and replay triage.

## Reopen Criteria

Reopen this triage and prepare an upstream issue only when at least one reproducible sample is available with:

1. `error_reason=pqs:runtime_1105` (or equivalent 1105 `index out of range` runtime signature).
2. Reproducible base replay (not `base_replay_not_reproducible`).
3. Attached minimal artifact set:
- `schema.sql`
- `data.tsv`
- `case.sql`
- `replay.sql` (if generated)
- runtime info (`run_info`, TiDB version/commit if available)

## Operational Follow-up

Keep this signature monitored in report aggregation:

- `error_reason == "pqs:runtime_1105"`
- `bug_hint == "tidb:runtime_error"`

If new hits appear, prioritize minimize/replay stability before opening an upstream issue.
