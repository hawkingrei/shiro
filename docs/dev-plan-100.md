# Shiro Roadmap: 100-Step Plan

1. Add Impo oracle metrics to stats logging (counts, skips, truncations).
2. Add Impo-specific skip reasons to dynamic_state.json for observability.
3. Add Impo replay_kind to minimizer for base/mutated comparison.
4. Capture Impo mutated SQL in case artifacts separately (e.g., impo_mutated.sql).
5. Add Impo seed vs. init SQL to summary fields for quick filtering.
6. Add Impo-specific config for max mutations per seed (cap).
7. Add Impo-specific timeout budget per mutation batch.
8. Add Impo skip if row count exceeds configurable threshold before fetching.
9. Add Impo skip when base query has non-deterministic functions.
10. Add Impo skip when base query includes plan cache hints or session vars.
11. Add Impo switch to disable stage1 rewrites (for differential coverage).
12. Add Impo switch to retain LEFT/RIGHT JOINs for experimental runs.
13. Add Impo support for ANY/ALL mutation in compare-subquery expressions.
14. Add Impo mutation for BETWEEN with safe strengthening/weakening rules.
15. Add Impo mutation for IN-list expansion/shrink (non-NULL).
16. Add Impo mutation for EXISTS/NOT EXISTS with polarity-aware flagging.
17. Add Impo mutation for UNION/INTERSECT/MINUS if supported by TiDB.
18. Add Impo mutation for DISTINCT on nested subqueries (if safe).
19. Add Impo mutation for HAVING with GROUP BY retention (optional).
20. Add Impo mutation for ORDER BY removal when not affecting semantics.
21. Add Impo mutation for LIMIT expansion (beyond stage1 hard cap).
22. Add Impo mutation to add redundant predicate (tautology/contradiction).
23. Add Impo mutation to remove redundant predicate in CNF/DNF cases.
24. Add Impo mutation to rewrite NOT(p) with De Morgan (guarded).
25. Add Impo mutation for comparison operator normalization (<= vs <+1) in safe domains.
26. Add Impo mutation for boolean columns using IS TRUE/IS FALSE toggles.
27. Add Impo NULL-handling gate by checking schema nullability.
28. Add Impo data generator switch to avoid NULLs per-oracle.
29. Add Impo mode to allow NULLs with three-valued logic soundness checks.
30. Add Impo result comparison that includes per-row type normalization.
31. Add Impo result comparison that is order-insensitive but column-name aware.
32. Add Impo result comparison with stable hashing to reduce memory.
33. Add Impo optional row sampling for large results with false-positive guard.
34. Add Impo diff artifact: missing vs. extra rows (sampled).
35. Add Impo max row size guard to avoid huge row payloads.
36. Add Impo configurable mutation selector (random subset vs. exhaustive).
37. Add Impo mutation weights tuned for optimizer coverage.
38. Add Impo skip if query uses window functions after stage1.
39. Add Impo skip if query uses aggregation with GROUP BY still present.
40. Add Impo skip for recursive CTEs.
41. Add Impo support for CTE-aware mutation and restoration.
42. Add Impo with plan-guided mutation selection (tie into QPG signals).
43. Add Impo coverage counters by mutation type.
44. Add Impo case tag in report UI for filtering.
45. Add Impo detail view in frontend (seed/init/mutated SQL, cmp).
46. Add Impo compare-explain view (base vs mutated explain) in frontend.
47. Add Impo UI diff for row samples when available.
48. Add Impo CSV export for mutation coverage stats.
49. Add Impo regression test: fixed query pair with known containment.
50. Add Impo regression test: mutation set size and determinism.
51. Add Impo fuzz test for mutation restore stability.
52. Add Impo minimizer support for reducing mutated SQL separately.
53. Add Impo replayer spec to validate containment in minimized cases.
54. Add Impo compatibility check against TiDB version features.
55. Add Impo skip when SQL includes unstable functions list (extend rmUncertain).
56. Implement rmUncertain to drop/replace non-deterministic functions.
57. Add config to toggle rmUncertain enforcement per oracle.
58. Add new oracle statistics per worker to avoid contention.
59. Add per-oracle isolation to reduce cross-case contamination.
60. Add report grouping by oracle and mutation type.
61. Add case deduplication by (oracle + mutation + plan signature).
62. Add multi-run aggregation for Impo results.
63. Add per-oracle seed persistence for repro.
64. Add ability to replay Impo cases without stage1 rewrite.
65. Add impomysql removal after port completion and references updated.
66. Add migration note in docs for Impo oracle integration.
67. Add docs: Pinolo mutation taxonomy with examples.
68. Add docs: Impo oracle limitations and null semantics.
69. Add docs: How to tune Impo mutation weights for optimizer focus.
70. Add docs: Repro recipe for Impo cases.
71. Add config example for Impo-only runs.
72. Add CLI flag to prioritize Impo oracle.
73. Add config to disable LIKE/REGEXP mutations if too noisy.
74. Add config for LIKE/REGEXP mutation probability.
75. Add guard for REGEXP patterns with empty strings.
76. Add guard for LIKE pattern all-wildcard corner cases.
77. Add guard for regex engine differences across TiDB versions.
78. Add mutation for string collation-related comparisons (guarded).
79. Add mutation for implicit type cast comparison (guarded).
80. Add mutation for numeric boundary conditions (overflow-aware).
81. Add mutation for decimal precision rounding (guarded).
82. Add mutation for date/time boundary shifts (guarded).
83. Add mutation for boolean normalization (0/1/TRUE/FALSE).
84. Add mutation for IN subquery with ANY/ALL rewriting (guarded).
85. Add mutation for semi-join rewriting hints (optimizer stress).
86. Add mutation for join reordering hints (optimizer stress).
87. Add Impo mode to target plan cache queries (prepare/execute).
88. Add Impo skip for plan cache artifacts to avoid interference.
89. Add adaptive selection between Impo and DQP based on plan coverage.
90. Add dashboard panel for mutation yield vs. bug yield.
91. Add CI smoke test for Impo oracle path.
92. Add linter rule for oracle naming consistency.
93. Add report field for Impo mutation seed.
94. Add report field for Impo truncation status.
95. Add report field for Impo max rows used.
96. Add report field for Impo compare mode (full vs sample).
97. Add self-check for Impo oracle invariants when cmp==2.
98. Add fallback to signature compare when rowset too large (configurable).
99. Add per-mutation timeout instrumentation and logging.
100. Add tuning guide for overall oracle mix with Impo enabled.
