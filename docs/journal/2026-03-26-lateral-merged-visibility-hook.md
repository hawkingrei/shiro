# LATERAL merged-column visibility hook

## What changed

- Upgraded TiDB and parser dependencies to a version that includes `LATERAL` grammar support.
- Added a `features.lateral_joins` flag and preserved `LATERAL` in generator/oracle SQL rendering.
- Restricted generated support to `CROSS JOIN LATERAL` and `JOIN LATERAL ... ON (1 = 1)`.
- Added a dedicated bug-mining hook for merged-column visibility after `USING` or `NATURAL` joins under `LATERAL`.
- Added focused scope, validator, and minimize tests for both the correlated `ORDER BY + LIMIT` hook and the merged-column visibility hook.

## Why

- The parser bump was required before Shiro could emit or validate any `LATERAL` query shapes.
- The first implementation slice intentionally stays narrow so generator, validator, oracle SQL rebuild, and minimization all agree on the supported syntax surface.
- The merged-column visibility hook specifically targets planner/name-resolution boundaries where outer merged columns must remain visible to lateral subqueries only as unqualified references.

## Validation

- `go test ./internal/config -run TestLoadLateralJoinFeatureFlag -count=1`
- `go test ./internal/generator -run 'Test(BuildInnerLateralJoin|ValidateQueryScopeLateralJoinAllowsLeftReferences|ValidateQueryScopeLateralJoinRejectsFutureTableReferences)' -count=1`
- `go test ./internal/oracle -run 'TestBuildFromCrossLateralJoin' -count=1`
- `go test ./internal/validator -run 'TestValidateLateralJoinSQL' -count=1`
- `go test ./internal/runner -run 'TestSelectCandidatesHandleLateralJoin' -count=1`
- `go test ./internal/generator -run 'Test(BuildCorrelatedOrderLimitLateralQuery|GenerateSelectQueryExercisesLateralOrderLimitHook|BuildMergedColumnVisibilityLateralHookQueryUsing|BuildMergedColumnVisibilityLateralHookQueryNatural|GenerateSelectQueryExercisesMergedColumnLateralHook|ValidateQueryScopeLateralJoinUsingAllowsUnqualifiedMergedColumn|ValidateQueryScopeLateralJoinUsingRejectsQualifiedMergedColumn)' -count=1`
- `go test ./internal/validator -run 'TestValidateLateral(Join|MergedColumn)SQL' -count=1`
- `go test ./internal/runner -run 'TestSelectCandidatesHandle(LateralJoin|MergedColumnLateralJoin)' -count=1`

## Outcome

- Shiro can now generate and validate a minimal `LATERAL` surface.
- The merged-column visibility hook is exercised successfully and remains parseable through validation and minimization.
- No concrete TiDB runtime or wrong-result repro has been isolated from the merged-column hook yet.

## Follow-up

- Mine correlated aggregates over multi-table left inputs under `LATERAL` now that merged-column visibility coverage is in place.
