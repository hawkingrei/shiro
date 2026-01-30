# TODO

This file tracks current tasks and should stay aligned with `docs/notes/follow-ups.md` to avoid stale plans.
最近检查：2026-01-30，暂无已完成项；已补充 join scope 测试与 CERT 校验，复查最新日志，增加 TLP error 细分，并记录 TLP 错误样例，查看最新 reports，修复 TLP UNION/ORDER BY 错误，修复 TLP USING 列名限定错误，并归一化 USING 合并列引用与重写 ON。
Last review: 2026-01-30. Checked shiro.log; TLP errors were 0 after 12:32.

## Generator / Oracles

1. CERT: add stronger guardrails for DISTINCT/ORDER BY/ONLY_FULL_GROUP_BY.
2. DQP/TLP: reduce predicate_guard frequency without weakening semantic assumptions.
3. CODDTest: extend to multi-table dependent expressions while preserving NULL semantics.
4. GroundTruth: reduce edge_mismatch by aligning join edge extraction with generator (USING/AND handling).
4. Consider making `CTECountMax` configurable for resource-sensitive runs.
5. Consider increasing `groundtruth_max_rows` to reduce `groundtruth:table_rows_exceeded` skips.
6. Consider lowering DSG per-table row counts to stay under the GroundTruth table cap.

## Reporting / Aggregation

1. Add frontend aggregation views (commit/bug type) and export.
2. Add S3/report incremental merging and multi-source aggregation.

## Coverage / Guidance

1. Centralize tuning knobs for template sampling weights and QPG template overrides (enable prob/weights/TTLs/thresholds).
