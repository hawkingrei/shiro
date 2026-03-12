package runner

import "shiro/internal/oracle"

const (
	replayNoErrorReason = "replay:no_error"
	replayNoErrorText   = "no error"
)

type replayAttemptResult struct {
	matched bool
	diag    replayFailureDiagnostic
}

type replayFailureDiagnostic struct {
	replayKind             string
	outcome                string
	failureStage           string
	lastOp                 string
	lastSQL                string
	expectedError          string
	expectedErrorReason    string
	expectedErrorSignature string
	expectedErrorCode      uint16
	actualError            string
	actualErrorReason      string
	actualErrorSignature   string
	actualErrorCode        uint16
}

func (d replayFailureDiagnostic) isZero() bool {
	return d == (replayFailureDiagnostic{})
}

func failReplayAttempt(result oracle.Result, spec replaySpec, trace *replayTrace, stage, outcome string, actualErr error) replayAttemptResult {
	return replayAttemptResult{
		diag: newReplayFailureDiagnostic(result, spec, trace, stage, outcome, actualErr),
	}
}

func newReplayFailureDiagnostic(
	result oracle.Result,
	spec replaySpec,
	trace *replayTrace,
	stage string,
	outcome string,
	actualErr error,
) replayFailureDiagnostic {
	diag := replayFailureDiagnostic{
		replayKind:   spec.kind,
		outcome:      outcome,
		failureStage: stage,
	}
	if trace != nil {
		diag.lastOp = trace.lastOp
		diag.lastSQL = trace.lastSQL
	}
	expectedReason := effectiveResultErrorReason(result)
	if expectedReason != "" {
		diag.expectedErrorReason = expectedReason
	}
	expectedText := effectiveResultErrorText(result)
	expectedSignature := buildErrorSignature(expectedReason, result.Err, expectedText)
	if expectedText != "" {
		diag.expectedError = abbrevText(expectedText, replayErrorMessageMax)
	}
	if expectedSignature != "" {
		diag.expectedErrorSignature = expectedSignature
	}
	if result.Err != nil {
		if code, ok := mysqlErrCode(result.Err); ok {
			diag.expectedErrorCode = code
		}
	}
	if actualErr != nil {
		actualReason, _ := classifyResultError(result.Oracle, actualErr)
		diag.actualErrorReason = normalizeErrorReason(actualReason)
		actualText := actualErr.Error()
		diag.actualError = abbrevText(actualText, replayErrorMessageMax)
		diag.actualErrorSignature = buildErrorSignature(diag.actualErrorReason, actualErr, actualText)
		if code, ok := mysqlErrCode(actualErr); ok {
			diag.actualErrorCode = code
		}
	} else if outcome == "error_mismatch" {
		diag.actualError = replayNoErrorText
		diag.actualErrorReason = replayNoErrorReason
		diag.actualErrorSignature = buildErrorSignature(replayNoErrorReason, nil, replayNoErrorText)
	}
	return diag
}

func (d replayFailureDiagnostic) toDetails(attempts, successes, required int) map[string]any {
	details := map[string]any{
		"minimize_base_replay_attempts":  attempts,
		"minimize_base_replay_successes": successes,
		"minimize_base_replay_required":  required,
	}
	if d.replayKind != "" {
		details["minimize_base_replay_kind"] = d.replayKind
	}
	if d.outcome != "" {
		details["minimize_base_replay_outcome"] = d.outcome
	}
	if d.failureStage != "" {
		details["minimize_base_replay_failure_stage"] = d.failureStage
	}
	if d.lastOp != "" {
		details["minimize_base_replay_last_op"] = d.lastOp
	}
	if d.lastSQL != "" {
		details["minimize_base_replay_last_sql"] = d.lastSQL
	}
	if d.expectedError != "" {
		details["minimize_base_replay_expected_error"] = d.expectedError
	}
	if d.expectedErrorReason != "" {
		details["minimize_base_replay_expected_error_reason"] = d.expectedErrorReason
	}
	if d.expectedErrorSignature != "" {
		details["minimize_base_replay_expected_error_signature"] = d.expectedErrorSignature
	}
	if d.expectedErrorCode != 0 {
		details["minimize_base_replay_expected_error_code"] = d.expectedErrorCode
	}
	if d.actualError != "" {
		details["minimize_base_replay_actual_error"] = d.actualError
	}
	if d.actualErrorReason != "" {
		details["minimize_base_replay_actual_error_reason"] = d.actualErrorReason
	}
	if d.actualErrorSignature != "" {
		details["minimize_base_replay_actual_error_signature"] = d.actualErrorSignature
	}
	if d.actualErrorCode != 0 {
		details["minimize_base_replay_actual_error_code"] = d.actualErrorCode
	}
	return details
}
