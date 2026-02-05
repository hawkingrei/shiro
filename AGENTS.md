# Shiro Fuzz Tool - Agent Notes

Project notes are now documented under `docs/notes/` with supporting references below.

Links:
- [Notes Summary](docs/notes/summary.md)
- [Experience Notes](docs/notes/experience.md)
- [Feature Notes](docs/notes/feature.md)
- [Follow-ups](docs/notes/follow-ups.md)
- [Roadmap](docs/roadmap.md)
- [Oracles](docs/oracles/README.md)
- [Glossary](docs/glossary.md)
- [Architecture Decisions](docs/decisions/README.md)

## Workflow guardrails

- After each task completes, review and update `AGENTS.md` and `docs/todo.md`, removing completed items and syncing current progress.
- Documentation must be written in English.

## Recent updates

- Added a constraints-based SelectQueryBuilder, tightened DSG join alignment, and improved GroundTruth/CERT generation guardrails (2026-01-30).
- Fixed JOIN ON scope validation, added CERT scope checks, and updated tests (2026-01-30).
- TLP: added error_reason/whitelist skips, fixed UNION/ORDER BY 1221, rewrote USING to ON with column qualification, removed unused helpers, and updated tests/logging (2026-01-30).
- CERT sampling: removed weight config, fixed sampling at a constant rate, added sampling ratio logs, and refined bandit/lock handling with safe fallbacks (2026-01-30).
- DQP: treat LEADING mismatches as TiDB bugs (2026-01-30).
- EXISTS metrics: regex matching for whitespace/newlines; TODO to improve EXISTS/NOT EXISTS coverage (2026-01-30).
- Reviewed logs/reports, captured CERT/TLP/DQP examples, and fixed PR description line breaks (2026-01-30).
- Ran `go test ./...` (2026-01-29, 2026-01-30).
- Added paper notes: EET and SQLancer++ summaries under docs (2026-01-30).
- Added the EET oracle using AST-level predicate rewrites and signature comparison (2026-01-30).
- Expanded EET to cover HAVING/JOIN ON and added type-aware literal identities (2026-01-30).
- Added weighted EET rewrite selection to tune boolean vs literal identities (2026-01-30).
- Added TODO to track EET rewrite coverage logging needs (2026-01-30).
- Addressed EET review fixes (date rewrite form, parser driver imports, predicate target shuffle, doc TODO updates) (2026-01-30).
- Loosened EET predicate guards (allow NOT/IS NULL) and raised DML/EXISTS subquery probabilities (2026-01-31).
- Report UI formatting updates moved to docs/notes/summary.md (2026-01-31).
- Web linting notes moved to docs/notes/feature.md (2026-01-31).
- CI linting updates are tracked in docs/notes/feature.md (2026-01-31).
- Generator randomness updates are tracked in docs/notes/feature.md (2026-01-31).
- TQS randomValue date/time randomness updates are tracked in docs/notes/feature.md (2026-01-31).
- Date sample reuse for predicate equality is tracked in docs/notes/feature.md (2026-01-31).
- Subquery coverage logging and disallow-reason stats are tracked in docs/notes/feature.md (2026-02-01).
- EET skips constant ORDER BY with LIMIT to avoid nondeterministic sampling (2026-02-01).
- Enabled subqueries for non-DQP/TLP oracles via runner overrides (2026-02-01).
- Enabled NOT EXISTS/NOT IN for non-DQP/TLP oracles via runner overrides (2026-02-01).
- Relaxed EET/CODDTest builder subquery constraints and raised PredicateExistsProb to 90 (2026-02-01).
- Added per-oracle subquery coverage logs to avoid DQP/TLP override confusion (2026-02-01).
- Raised PredicateSubqueryScale/PredicateExistsProb and allowed EET predicate subqueries (2026-02-01).
- Added IN(subquery)/NOT IN(subquery) counters to interval logs (2026-02-01).
- Added flaky tag for cases with matching EXPLAIN plans but signature mismatches (2026-02-01).
- Added optional GROUP BY ordinals (GROUP BY 1,2) using ordinal wrappers; sampling is controlled by the feature bandit (2026-02-01).
- Added GroupByOrdinalExpr build tests and stricter invalid-state guard (2026-02-01).
- Switched EXISTS/NOT EXISTS and IN(list) counters to AST parsing for generator and plan-cache SQL (2026-02-02).
- Treated NOT (IN(subquery)) as NOT IN in SQL AST parsing and extended EET USING-qualified guard to CTEs (2026-02-02).
- Split generator SQL feature counts into a dedicated interval log line to keep plan-cache ratios consistent (2026-02-02).
- Switched IN(subquery) counters to generator AST features, added oracle-variant AST counting, and skipped EET USING-qualified column cases (2026-02-02).
- Ensured scalar subquery disallow flags are respected in predicate generation and template predicates (2026-02-03).
- EET now rewrites USING to ON before guards and accepts any predicate target that matches the policy (2026-02-03).
- QPG seen SQL stats now use a monotonic added counter to avoid negative deltas (2026-02-03).
- GroundTruth join key extraction now handles USING/AND and unqualified columns; added tests (2026-02-03).
- GroundTruth now falls back to AST parsing for join keys (alias-aware) to reduce key_missing (2026-02-03).
- GroundTruth now supports composite join keys in truth evaluation and SQL joins (2026-02-03).
- GroundTruth key_missing now logs per-reason breakdown in oracle_skip_reasons (2026-02-03).
- Report UI: added summary aggregation panel and truncation pills for expected/actual rows (2026-02-05).
- CODDTest: treat predicates without column dependencies as valid for no-null guard (2026-02-05).
- Enabled subqueries for DQP/TLP via oracle overrides (2026-02-03).
- GroundTruth join key extraction now accepts NOT NOT and NULL-safe equality for ON clauses (2026-02-03).
- Added join_on_policy/join_using_prob config knobs and GroundTruth bias toward USING; key-missing now distinguishes no-column ON predicates (2026-02-03).
- Forced TLP to use complex JOIN ON while keeping GroundTruth simple (2026-02-03).
- Join ON now falls back to USING-derived equality before emitting constant false predicates (2026-02-03).
- Added joinConditionFromUsing fallback test for USING-derived ON equality (2026-02-03).
- Plan cache now treats unknown column errors in WHERE as whitelisted (2026-02-03).
- Simplified join predicate unwrap to satisfy revive lint (2026-02-03).
- Added generator window feature counters in interval logs and split DQP window skip reasons (2026-02-03).
- EET now classifies signature missing-column errors separately for clearer reporting (2026-02-04).
- GroupBy generation now emits at least two columns when available to avoid single-key instability (2026-02-04).
- EET now skips ORDER BY with constants or fewer than two distinct columns (2026-02-04).
- Generator ORDER BY now enforces at least two distinct columns when available (2026-02-04).
- Generator ORDER BY now prefers stable distinct columns and falls back to ordinals (2026-02-04).
- Report summaries now include limited result-set snapshots instead of checksum strings (2026-02-04).
- GroundTruth retries query generation when join ON has no columns to reduce key_missing noise (2026-02-04).
- Metrics log now includes predicate join pair counts alongside the ratio (2026-02-04).
- Lowered view selection probabilities to reduce view frequency (2026-02-04).
- CERT now uses column-only select lists to avoid ONLY_FULL_GROUP_BY and DISTINCT/ORDER BY instability (2026-02-04).
- DQP/TLP predicate guards now allow NOT/IS NULL to reduce skip rate without changing core semantics (2026-02-04).
- Added mismatch/explain-same and GroundTruth skip counters to interval metrics (2026-02-04).
- DSG wide_rows now respects GroundTruth row caps to reduce table_rows_exceeded skips (2026-02-04).
- Added `weights.features.cte_count_max` to cap CTE generation for resource-sensitive runs (2026-02-04).
- CODDTest now supports multi-table dependent mappings with join-aware column binding (2026-02-04).
- DQE now retries predicate generation to bias EXISTS/NOT EXISTS coverage (2026-02-05).
- EET now records per-rewrite skip reasons for coverage accounting (2026-02-05).
- EET now supports type-aware column identity rewrites using schema-based inference (2026-02-05).
- Shared ORDER BY ordinal parsing helpers between generator tests and EET (2026-02-04).
- Report row sampling now detects truncation via LIMIT maxRows+1 and documents SQL safety (2026-02-04).
