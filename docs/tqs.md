# Detecting Logic Bugs of Join Optimizations in DBMS (TQS)

本文是对论文 **"Detecting Logic Bugs of Join Optimizations in DBMS"** 的教学式笔记，强调核心算法（DSG / Ground-Truth / KQE）及其可实现要点。

---

## 1. 论文目标（一句话版）

这篇论文专门解决“多表 JOIN 优化中的逻辑错误”：它通过**数据驱动的 schema + 可计算真值**，生成复杂 JOIN 查询，并用真值验证优化器结果是否正确。

---

## 2. 为什么传统 SQL fuzz 不够？

传统随机 SQL fuzz 的问题：

- 很难系统性覆盖多表 JOIN 的结构空间（例如 4+ 表、混合 JOIN 类型）。
- 缺少“真值”，只能做差分，很容易漏掉“全体一致但全体错误”的问题。

这篇论文的核心贡献：

1) DSG：数据驱动的 schema + query 生成  
2) Ground-Truth：对 JOIN 结果的“真值”计算  
3) KQE：覆盖驱动的 join 查询探索

---

## 3. DSG：Data-guided Schema & Query Generation

### 3.1 宽表视角（Wide Table）

假设原始数据集可视作一个宽表：

```
WideTable(id, a, b, c, d, e)
```

其中存在函数依赖（FD），例如：

- `a -> b`
- `c -> d`

### 3.2 规范化拆表（Normalization）

根据 FD 规范化为多个表：

```
T1(a, b)
T2(c, d)
T3(id, a, c, e)
```

这样生成的 schema 天然适合多表 JOIN：

```
T3 JOIN T1 ON T3.a = T1.a
T3 JOIN T2 ON T3.c = T2.c
```

### 3.3 RowID 映射（Traceability）

对宽表每一行保留 RowID：

- RowID = 宽表中的行号
- 拆表时记录每一行对应的 RowID 集合

### 3.4 Bitmap Index（真值核心）

为每个拆表行维护 RowID 位图（bitmap）：

```
Bitmap(T1.a = x) = {RowIDs where a = x}
```

这样可以直接对 bitmap 做交并，得到 JOIN 结果的真值。

---

## 4. JOIN 查询生成（Random Walk）

### 4.1 Schema Graph

将 schema 看作图：

- 节点：表  
- 边：PK/FK 关系

例如：

```
T1 -- T3 -- T2
```

### 4.2 随机游走生成 JOIN

从某表出发随机游走，生成 JOIN 链：

```
T3 -> T1 -> T2 -> T4
```

对应 SQL：

```sql
SELECT ...
FROM T3
JOIN T1 ON T3.a = T1.a
JOIN T2 ON T3.c = T2.c
JOIN T4 ON ...
```

支持的 JOIN 类型：

- INNER / LEFT / RIGHT
- SEMI / ANTI
- CROSS

---

## 5. Ground-Truth 计算（最关键）

### 5.1 核心思想

把 JOIN 查询映射回 WideTable，通过 RowID bitmap 运算获得**逻辑真值**。

### 5.2 例子

查询：

```sql
SELECT *
FROM T3
JOIN T1 ON T3.a = T1.a
WHERE T1.b > 10;
```

真值计算步骤：

1) 取 `T1.b > 10` 对应 RowID bitmap  
2) 取 `T3` RowID bitmap  
3) JOIN = bitmap intersection  
4) 对应的 WideTable 行即为真值结果

这样不依赖 DBMS 执行器，就能得到正确结果。

---

## 6. KQE：Knowledge-guided Query Exploration

### 6.1 Plan-Iterative Graph

- 节点：表/列  
- 边：JOIN 类型 / 谓词关系  
- 记录覆盖情况

### 6.2 Adaptive Random Walk

在生成 JOIN 时优先走“覆盖不足”的路径。  
覆盖度越低，概率越大。

效果：

- 避免重复相似 JOIN 结构  
- 更快覆盖复杂 JOIN 空间

---

## 7. 论文启发的可落地要点

1) 用 DSG-lite 提升多表 JOIN 复杂度  
2) 增加 Ground-Truth Oracle 做绝对真值校验  
3) 用 KQE-lite 做结构覆盖驱动的 JOIN 生成  

---

## 8. 小结

这篇论文的核心优势在于：

- 复杂多表 JOIN 生成覆盖更系统  
- Ground-Truth 能发现“全体一致但全体错误”  
- KQE 让 JOIN 空间探索更高效
