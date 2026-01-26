# Keep It Simple: Testing Databases via Differential Query Plans (DQP)

## Background
Optimizers choose among multiple plan paths. Traditional equivalence-based testing requires complex rewrites and has limited coverage. Executing the same query under different plans and comparing results can expose optimizer logic bugs more directly.

## Core Idea
DQP uses "same query, different plans" as an oracle. It forces alternative plans via hints or session variables and compares result signatures.

## Key Mechanics
1. Generate a query Q.
2. Build plan variants Q1, Q2, ... using hints or session variables.
3. Execute variants and compare signatures or row sets. Differences indicate a bug.

## Oracle Form
- Q:  SELECT ...
- Q1: SELECT /*+ hintA */ ...
- Q2: SELECT /*+ hintB */ ...
- If results differ, flag a bug.

## Scope and Limitations
- Requires plan hints or controllable optimizer switches.
- Keep hint sets minimal to reduce unrelated noise.
- Ensure variants are semantically equivalent and deterministic.

## Impact
DQP triggers plan divergence with minimal query changes and is effective for plan-selection and operator-implementation bugs.
