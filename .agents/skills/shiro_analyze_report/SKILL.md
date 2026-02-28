---
name: shiro_analyze_report
description: "Create a TiDB bug issue from Shiro report artifacts and add a short root-cause analysis. Use when a Shiro report directory (summary.json/case.sql/plan_replayer.zip) is available and you need a ready-to-file GitHub issue body."
---

# shiro_analyze_report

## Goal

Produce a TiDB bug issue body from a Shiro report, then add a concise **Analysis** section with 1–3 likely causes.

## Required inputs

- Shiro report directory (e.g., `reports/case_0001_...`)
- `summary.json`

## Optional inputs

- `case.sql`, `schema.sql`, `data.tsv`, `inserts.sql`
- `plan_replayer.zip`

## Output

- A filled issue body using `references/bug-report.md`
- An appended **Analysis** section

## Workflow (deterministic)

1. Read `summary.json` and list:
   - SQL (expected/actual)
   - Expected vs actual checksums/counts
   - Oracle and rewrite kind
   - TiDB version
2. Read `case.sql` and `schema.sql` (if present) for minimal repro steps.
3. Fill `references/bug-report.md` fields with concrete values.
4. Append **Analysis**:
   - 1–3 likely causes
   - Evidence from SQL or plan signature differences
5. If asked, run `gh issue create`; otherwise return the issue body text.

## Constraints

- Issue body must be in English.
- Keep Analysis short and evidence-backed.
- If `plan_replayer.zip` exists, mention it in repro steps.
- Do not modify the template front matter/title in `references/bug-report.md`; only fill the report content sections.
- Severity labeling rule:
  - Wrong-result bugs: use `severity/major`.
  - Query fails to execute (for example internal planner/runtime error on valid SQL): use `severity/major`.
  - Complex-query planner/compatibility issues that are not confirmed wrong-result and not execution-blocking: use `severity/moderate`.
- For planner bugs from Shiro cases, default labels should include `type/bug`, `sig/planner`, `AI-Testing`, and `fuzz/shiro`, then apply severity per the rule above.
