# Wrong-Result-Oriented Oracle Bandit Reward

## What changed

- Reworked the oracle bandit reward so it no longer treats every `!result.OK` outcome as equally valuable.
- Stable wrong-result mismatches now keep the highest reward.
- Explain-same mismatches are explicitly down-weighted because they are more likely to be determinism noise than engine-facing wrong-result bugs.
- Execution-error bug cases and panic-like failures still contribute reward, but much less than stable mismatches.
- Small positive reward for plain successful execution was reduced so high-throughput oracles do not dominate selection as easily.

## Stats alignment

- Tightened runner-side mismatch accounting to count only true wrong-result mismatches (`!result.OK && result.Err == nil`).
- Added per-oracle `ExplainSame` tracking in the oracle funnel so batch reward updates can separate stable mismatches from explain-same mismatches.
- This prevents the periodic oracle-bandit funnel update from double-counting execution-error cases as if they were wrong-result mismatches.

## Files

- `internal/runner/runner.go`
- `internal/runner/runner_bandit.go`
- `internal/runner/runner_stats.go`
- `internal/runner/runner_bandit_reward_test.go`

## Validation

- `go test ./internal/runner -run 'TestOracleBanditImmediateReward|TestOracleBanditFunnelReward' -count=1`
- `go test ./internal/runner -count=1`

## Why this is the current best first step

- The recent wrong-result-shaped line is still dominated by `EET`, but prior triage showed at least part of that line is explain-same / flaky noise rather than strong engine-facing signal.
- The previous reward path biased the oracle bandit toward generic bug capture and generic execution effectiveness, not toward stable wrong-result discovery.
- This change is a low-risk, localized first move that improves the optimization target before the next fresh rerun lands.

## Follow-up

- Compare fresh rerun logs/reports after this reward shift and check whether:
  - stable wrong-result mismatch share increases
  - explain-same / flaky mismatch share decreases
  - oracle selection drifts away from noisy mismatch producers toward stronger wrong-result signal
