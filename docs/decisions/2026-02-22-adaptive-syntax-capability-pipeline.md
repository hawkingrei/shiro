# Adaptive Syntax Capability Expansion Pipeline

Date: 2026-02-22

## Context
- We want to increase SQL syntax coverage (generator + oracle execution paths) without destabilizing fuzz quality.
- The current system already has useful building blocks: per-feature config flags, per-oracle override profiles, and interval observability.
- Recent runs show a consistency gap in error taxonomy ownership:
  - `internal/oracle/pqs.go` pre-fills `details.error_reason` from `sqlErrorReason("pqs", err)`.
  - `internal/runner/runner_errors.go` has canonical runtime special-case mapping (`pqs:runtime_1105`) but only applies it when `details.error_reason` is not already set.
  - Result: `Error 1105 ... index out of range` can still be reported as `pqs:sql_error_1105` in summaries, reducing triage quality.
- Feature rollout is still mostly static/config-driven. We lack a first-class capability state that learns from runtime outcomes.

## Decision
- Introduce a staged, low-risk syntax expansion pipeline with three pillars:
1. Capability registry: explicit per-feature support state (global + per-oracle) with evidence counters.
2. Gate evaluation: deterministic pre-build and pre-run gates driven by capability state and profile constraints.
3. Canonical error ownership: runner is the final classifier for report-level `error_reason`/`bug_hint`; oracles provide stage metadata, not final taxonomy authority.

## Design

### 1) Capability Registry
- Add a capability model keyed by `feature_id` and optional `oracle`.
- Suggested states:
  - `unknown`: no evidence yet.
  - `learning`: sampled at low probability, collecting evidence.
  - `enabled`: stable and generally allowed.
  - `guarded`: temporarily throttled due to quality regression.
  - `disabled`: explicitly blocked.
- Suggested counters:
  - `attempts`, `successes`, `skips`, `errors`, `timeouts`.
  - `error_reason_topk` (small bounded map).
  - `last_seen_at`, `updated_at`.
- Persist into `dynamic_state.json` (append-compatible schema extension).

### 2) Gate Evaluation Pipeline
- Gate points:
  - `generator gate`: before query build (feature availability + complexity budget).
  - `runner gate`: before oracle execution (oracle profile + throughput/infra status + capability state).
- Keep existing profile constraints as hard boundaries; capability state only adjusts probabilities and soft throttles inside valid boundaries.
- Add gate metadata to run details for explainability:
  - `feature_gate_decision`, `feature_gate_reason`, `feature_gate_state`.

### 3) Error Taxonomy Canonicalization
- Runner owns final report taxonomy fields:
  - `details.error_reason`
  - `details.bug_hint`
- Oracle modules should keep stage metadata (`*_error_stage`, `*_query_sql`, `error_code`) but avoid hard-coding final reason strings where runner has stronger normalization.
- Immediate fix target:
  - Ensure `PQS + MySQL 1105 + "index out of range"` is always normalized to `pqs:runtime_1105` at report level, even if oracle pre-filled a generic `pqs:sql_error_1105`.

### 4) Coverage Matrix and Rollout Policy
- Add a maintained matrix for each syntax feature:
  - `parser_supported`
  - `generator_supported`
  - `oracle_supported_by_name`
  - `minimizer_replay_supported`
  - `default_gate_state`
- Rollout strategy:
  1. `unknown -> learning` at low sample (1%-5%).
  2. Promote to `enabled` only if quality thresholds pass.
  3. Move to `guarded` automatically on sustained regression windows.

## Implementation Plan

### Phase 0: Baseline and Matrix
- Add syntax feature matrix doc and map existing features to current support states.
- Record baseline metrics on current workloads (sql validity, skip structure, minimize reproducibility).

### Phase 1: Taxonomy Consistency Fix
- Unify `runtime_1105` classification path so report summaries are stable.
- Add regression tests for pre-filled oracle `error_reason` precedence behavior.

### Phase 2: Capability Registry (Read-Only)
- Introduce registry data model and persistence without changing generation behavior.
- Emit observability logs for registry updates.

### Phase 3: Capability-Driven Gating
- Enable learning-mode gates for selected features.
- Use conservative thresholds and guardrails under throughput/infra pressure.

### Phase 4: Incremental Feature Expansion
- Promote selected features by oracle in small batches with explicit acceptance checks.

## Validation
- Unit tests:
  - taxonomy precedence and canonicalization.
  - capability state transitions.
  - gate decision determinism.
- Integration tests:
  - short-run fuzz regression with fixed seed and oracle mix.
- Acceptance thresholds (initial):
  - `sql_valid_ratio >= 0.995`
  - no sustained growth in `:timeout` and infra-related reasons
  - no significant regression in minimize success ratio versus baseline window

## Alternatives Considered
- Keep pure static config-only gating:
  - simpler but does not adapt to DB/runtime drift.
- Let each oracle own final taxonomy:
  - faster local iteration but leads to cross-oracle inconsistency in reports.

## Consequences
- Benefits:
  - safer syntax expansion with explicit observability and rollback controls.
  - cleaner report taxonomy for triage, especially runtime bug channels.
  - less manual tuning drift over long fuzz campaigns.
- Trade-offs:
  - additional state and transition logic in runner.
  - more regression tests required for taxonomy and gating behavior.
