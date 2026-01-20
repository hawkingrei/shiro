---
name: shiro_analyze_report
description: Inspect and triage Shiro reports under a reports/ directory, review per-case findings, and attempt fixes in the current TiDB workspace.
---

# Shiro Report Analysis

Use this skill when a user asks to read Shiro reports (per-case directories under `reports/`) and fix issues in the current workspace (often a TiDB repo).

## Quick workflow

1. Locate the reports directory relative to the current working directory.
   - Prefer `reports/` in repo root; if missing, ask for the correct path.
   - Example path: `/path/to/reports` (for example, a TiDB workspace `reports/` directory).
2. Enumerate case directories and pick the one referenced by the user (or the latest by mtime).
3. Inspect the report files inside the case directory.
   - If JSON: summarize key fields (issue type, file path, line, message).
   - If HTML/text: extract the same essentials.
4. If the report points to a TiDB code path, open the file, reason about the bug, and prepare a minimal fix.
5. Propose targeted tests only if the fix touches logic with existing tests in that package.

## Commands to use (examples)

```bash
ls -la reports
ls -lat reports/<case>
rg -n "file|path|line|error|panic|stack" reports/<case>
```

For JSON reports:

```bash
jq '.' reports/<case>/<report>.json
jq -r '.findings[] | "\(.path):\(.line) \(.message)"' reports/<case>/<report>.json
```

For HTML/text reports:

```bash
rg -n "ERROR|WARN|panic|nil pointer|stack|file" reports/<case>
```

## Triage rules

- Always extract: file path, line, error summary, and reproducer (if present).
- Prefer `summary.json` as the primary signal; it includes `oracle`, `expected`, `actual`, and `details`.
- Use `case.sql` for the SQL sequence; `schema.sql` and `inserts.sql` for reproduction.
- `README.md` describes the reproduction order and any plan replayer artifact if present.
- Plan replayer zip may be absent; do not assume it exists. When present, it helps guard against extra DB-side changes (for example, session/global config) and records bindings, configs, stats, and version info.
- If multiple findings exist, order by severity (crash > incorrect result > perf > style).
- If a report path is outside the current repo, ask before making changes.

## Fix rules

- Keep fixes minimal and focused on the reported issue.
- Update only relevant files; do not reformat unrelated code.
- If the report references a generated file, trace the generator and fix the source.

## Output expectations

Provide:
- A brief summary of the finding(s).
- The exact file paths/lines you inspected.
- The proposed fix and why it addresses the report.
- Suggested tests or why tests were skipped.
