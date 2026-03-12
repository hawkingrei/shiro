# Issue Record Policy

## What changed

- Removed the local issue draft file under `docs/issues/`.
- Updated `AGENTS.md` to state that qualified GitHub issues should be filed directly without keeping repository-side issue draft records.
- Added a follow-up item to create a reusable issue filing helper/template for `gh issue create`.

## Why

The repository should not accumulate local issue draft artifacts for already-filed bugs. Keeping the policy in `AGENTS.md` makes the expected workflow explicit for future runs.

## Validation

- Confirmed that `docs/issues/` no longer contains tracked issue draft files.
- No code or test changes were involved.

## Follow-up

- Add a reusable issue filing helper/template so direct issue creation stays detailed without repository-side drafts.
