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
- 运行 go test ./...（2026-01-29）。
- 查看 logs/shiro.log（2026-01-30），记录 CERT 报告与 error/skip 统计。
- 抽取 cert:base_explain_error/Unknown column 命中（2026-01-30）。
- 检查 join/CTE 作用域与 view/CTE 列引用问题（2026-01-30）。
- 收紧 JOIN ON 作用域校验并为 CERT 添加范围校验与补充测试（2026-01-30）。
- 复查 logs/shiro.log 近期区间（2026-01-30），关注 WARN 与 CERT 报告。
- 为 TLP 增加 error_reason/白名单跳过并补充单测（2026-01-30）。
- 记录 TLP 错误样例 SQL 便于归因（2026-01-30）。
- 查看最新 logs 与 reports/case_*（2026-01-30）。
- 修复 TLP UNION/ORDER BY 触发的 1221 错误并补充测试（2026-01-30）。
- 修复 TLP USING 后列名限定导致的 1054 错误并补充测试（2026-01-30）。
- 归一化 USING 合并列引用并放开 TLP 继续执行（2026-01-30）。
- TLP USING 重写为 ON 并补齐未限定列名（2026-01-30）。
- Reviewed logs/shiro.log after latest run (2026-01-30).
