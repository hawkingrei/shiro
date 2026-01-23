# CERT: Finding Performance Issues in Database Systems Through the Lens of Cardinality Estimation

## 问题背景
性能回退常由优化器的基数估计错误引起。即使结果正确，错误的估计也会导致选择低效执行计划。传统逻辑正确性 oracle 无法覆盖这一类性能问题。

## 核心思路
CERT 将“基数估计随谓词收紧应单调不增”的直觉作为测试 oracle。通过对同一查询添加更严格的谓词，观察 EXPLAIN 中的估计行数是否出现违反单调性的异常，从而捕获潜在性能问题。

## 关键机制
1. 构造基础查询 Q，并生成一个“更严格”的查询 Q'（例如添加额外过滤条件）。
2. 仅使用 EXPLAIN 获取估计行数，避免实际执行干扰。
3. 若 estRows(Q') 明显大于 estRows(Q)，则违反单调性假设，提示估计错误或优化器问题。

## Oracle 形式
- Q: SELECT ... WHERE p
- Q': SELECT ... WHERE p AND p'
- 若 estRows(Q') > estRows(Q) * (1 + tolerance) 则触发 bug。

## 适用范围与限制
- 面向性能问题而非结果正确性。
- 需要避免统计信息不稳定或样本过小导致的噪声。
- 对复杂查询（多表 JOIN、子查询、聚合）可能引入额外估计噪声，需要适度约束生成策略。

## 价值与影响
CERT 让性能问题具备可自动化检测的“近似 oracle”，适用于优化器与统计模块的回归测试。
