# Pinolo: Detecting Logical Bugs in DBMS with Approximate Query Synthesis

## 问题背景
逻辑错误是最难捕获的 DBMS bug 之一：查询不会报错，但结果错误。传统等价查询变换覆盖有限，且对复杂 SQL 难以稳定构造。

## 核心思路
Pinolo 引入“近似查询合成（approximate query synthesis）”作为 oracle：通过系统性地对查询结构进行“过近似/欠近似”变换，构造一组应当满足包含关系的查询集合。若包含关系被破坏，则触发 bug。

## 关键机制
1. 定义近似变换（Approximate Mutator）：对关系、谓词、比较表达式进行“弱化/强化”。
2. 基于 AST 自顶向下遍历，对每个结构进行可证明的近似变换，生成 Q_over 与 Q_under。
3. 利用包含关系作为 oracle：
   - 对于过近似查询，结果应当包含原查询结果。
   - 对于欠近似查询，结果应当被原查询结果包含。

## Oracle 形式
- 设原查询为 Q：
  - Q_over: 通过弱化谓词或扩张关系得到，应满足 R(Q) ⊆ R(Q_over)
  - Q_under: 通过强化谓词或收缩关系得到，应满足 R(Q_under) ⊆ R(Q)
- 若包含关系不成立，则触发 bug。

## 关键变换示例
- 关系层：UNION ALL ↔ UNION、DISTINCT 的添加/移除。
- 谓词层：WHERE/HAVING/ON 子句替换为 TRUE 或 FALSE。
- 比较层：> ↔ >=、< ↔ <=、ANY ↔ ALL 等强弱替换。
- IN 列表：添加 NULL 以弱化约束。

## 理论前提与限制
- 论文证明依赖“数据库无 NULL 值”，否则三值逻辑会破坏包含关系。
- 非确定性函数、外连接、窗口函数等需在合成前移除或规避。

## 价值与影响
Pinolo 提供了比等价变换更强的 oracle 体系，通过“近似关系”覆盖更广的 SQL 结构与优化路径，在优化器与执行器层面均能发现逻辑错误。
