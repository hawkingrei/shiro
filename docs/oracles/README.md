# Oracles Boundary Notes

This directory documents each oracle's scope, limitations, and common false positives.

Current oracle notes live under `docs/` (for example `docs/norec.md`, `docs/dqp.md`, `docs/impo.md`).
If you move them here, add per-oracle files and update references.

Suggested template:

## Scope

- Supported SQL shapes and features.
- Explicitly excluded syntax or semantics.

## Guardrails

- Pre-run filters.
- Runtime limits (row caps, time budgets).

## False Positives

- Common causes.
- Minimal reproducing example and why it triggers.

## Comparison Model

- Core assumptions for optimized vs unoptimized comparison.
- Conditions that break the assumptions.

## Example

```sql
-- Example that should be skipped
SELECT COUNT(*) FROM t WHERE rand() > 0.5;
```
