# Oracles Boundary Notes

本目录用于记录每个 oracle 的适用边界、已知限制与常见误报场景。请按 oracle 名称新增单独文件，例如：

- `docs/oracles/norec.md`
- `docs/oracles/dqp.md`
- `docs/oracles/impo.md`

建议使用以下模板：

## Scope

- 适用的 SQL 形态与特性。
- 明确排除的语法/函数/语义组合。

## Guardrails

- 运行前的过滤条件。
- 运行中的限制条件（例如行数上限、时间预算）。

## False Positives

- 常见误报来源。
- 最小示例与触发原因。

## Comparison Model

- Optimized vs Unoptimized 对比的核心假设。
- 可能破坏对比假设的条件。

## Example

```sql
-- Example that should be skipped
SELECT COUNT(*) FROM t WHERE rand() > 0.5;
```
