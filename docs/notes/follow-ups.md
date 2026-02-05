# Follow-ups

- CERT: add stronger guardrails for DISTINCT/ORDER BY/ONLY_FULL_GROUP_BY.
- DQP/TLP: reduce predicate_guard frequency without weakening semantic assumptions.
- DQP: expand plan-hint coverage and add optimizer variables if needed.
- Consider increasing `groundtruth_max_rows` to reduce `groundtruth:table_rows_exceeded` skips.
- Consider lowering DSG per-table row counts to stay under the GroundTruth cap.
- Split join-only vs join+filter predicates into explicit strategies with separate weights and observability.
- Wire GroundTruth join key extraction into oracle execution for JoinEdge building.
- Refactor per-oracle generator overrides into data-driven capability profiles to reduce duplicated toggles.
- Reporting: build a report index and switch the UI to on-demand summary loading (S3/HTTP friendly).
- Centralize tuning knobs for template sampling/weights and QPG template overrides (enable prob, weights, TTLs, thresholds).
- Add an adaptive feature capability model (SQLancer++ style) to learn DBMS support and auto-tune generator/oracle gating.
- Centralize query feature analysis + EXPLAIN parsing to avoid duplicated AST walks and plan parsing (shared by QPG/DQP/CERT/report).
- Add KQE-lite join-graph coverage guidance to bias join generation toward under-covered structures.
- Unify expression rewrite/mutation registries for EET/CODDTest/Impo with shared type inference and NULL-safety policies.
- Refine type compatibility and implicit cast rules using SQL standard guidance to reduce benign type errors.
