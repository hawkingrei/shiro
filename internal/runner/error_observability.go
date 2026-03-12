package runner

import (
	"fmt"
	"strings"

	"shiro/internal/oracle"
)

func detailString(details map[string]any, keys ...string) string {
	if details == nil {
		return ""
	}
	for _, key := range keys {
		value, ok := details[key]
		if !ok {
			continue
		}
		text, ok := value.(string)
		if !ok {
			continue
		}
		text = strings.TrimSpace(text)
		if text != "" {
			return text
		}
	}
	return ""
}

func effectiveResultErrorReason(result oracle.Result) string {
	if reason := detailString(result.Details, "error_reason", "skip_error_reason"); reason != "" {
		return normalizeErrorReason(reason)
	}
	if result.Err == nil {
		return ""
	}
	reason, _ := classifyResultError(result.Oracle, result.Err)
	return normalizeErrorReason(reason)
}

func effectiveResultErrorText(result oracle.Result) string {
	if result.Err != nil {
		return strings.TrimSpace(result.Err.Error())
	}
	return detailString(result.Details, "skip_error", "error")
}

func annotateEffectiveErrorMetadata(result *oracle.Result) {
	if result == nil {
		return
	}
	if result.Details == nil {
		result.Details = map[string]any{}
	}
	if signature := buildErrorSignature(
		effectiveResultErrorReason(*result),
		result.Err,
		effectiveResultErrorText(*result),
	); signature != "" {
		result.Details["error_signature"] = signature
	}
}

func buildErrorSignature(reason string, err error, errText string) string {
	reason = normalizeErrorReason(reason)
	shape := errorMessageShape(errText)
	if reason != "" {
		if shape == "" || errorReasonSuffix(reason) == shape {
			return reason
		}
		return reason + "|" + shape
	}
	if shape != "" {
		if code, ok := mysqlErrCode(err); ok {
			return fmt.Sprintf("%s|mysql_%d", shape, code)
		}
		return shape
	}
	if code, ok := mysqlErrCode(err); ok {
		return fmt.Sprintf("mysql_%d", code)
	}
	if keywords := errorKeywords(errText); len(keywords) > 0 {
		return strings.Join(keywords, "+")
	}
	return ""
}

func errorReasonSuffix(reason string) string {
	normalized := normalizeErrorReason(reason)
	if normalized == "" {
		return ""
	}
	_, suffix, ok := strings.Cut(normalized, ":")
	if !ok {
		return normalized
	}
	return suffix
}

func errorMessageShape(text string) string {
	lower := strings.ToLower(strings.TrimSpace(text))
	switch {
	case lower == "":
		return ""
	case strings.Contains(lower, "can't find column") && strings.Contains(lower, "in schema"):
		return "cant_find_column_in_schema"
	case strings.Contains(lower, "cannot find the reference from its child"):
		return "plan_reference_missing"
	case strings.Contains(lower, "some columns of topn"):
		return "topn_column_reference_missing"
	case strings.Contains(lower, "unknown column") && strings.Contains(lower, "where clause"):
		return "unknown_column_where"
	case strings.Contains(lower, "unknown column"):
		return "unknown_column"
	case strings.Contains(lower, "index out of range"):
		return "index_out_of_range"
	case strings.Contains(lower, "lock wait timeout"):
		return "lock_wait_timeout"
	case strings.Contains(lower, "deadlock"):
		return "deadlock"
	case strings.Contains(lower, "connection reset"),
		strings.Contains(lower, "connection refused"),
		strings.Contains(lower, "broken pipe"),
		strings.Contains(lower, "bad connection"):
		return "connection_error"
	case strings.Contains(lower, "context deadline exceeded"),
		strings.Contains(lower, "deadline exceeded"),
		strings.Contains(lower, "timeout"),
		strings.Contains(lower, "timed out"):
		return "timeout"
	case strings.Contains(lower, "syntax error"),
		strings.Contains(lower, "parse error"):
		return "syntax_error"
	case strings.Contains(lower, "runtime error"):
		return "runtime_error"
	}
	if keywords := errorKeywords(lower); len(keywords) > 0 {
		return strings.Join(keywords, "+")
	}
	return ""
}

func formatBaseReplayLogSuffix(details map[string]any) string {
	if detailString(details, "minimize_reason") != minimizeReasonBaseReplayNotReproducible {
		return ""
	}
	parts := make([]string, 0, 6)
	if value := detailString(details, "minimize_base_replay_kind"); value != "" {
		parts = append(parts, "base_replay_kind="+value)
	}
	if value := detailString(details, "minimize_base_replay_outcome"); value != "" {
		parts = append(parts, "base_replay_outcome="+value)
	}
	if value := detailString(details, "minimize_base_replay_failure_stage"); value != "" {
		parts = append(parts, "base_replay_stage="+value)
	}
	if value := detailString(details, "minimize_base_replay_last_op"); value != "" {
		parts = append(parts, "base_replay_last_op="+value)
	}
	if value := detailString(details, "minimize_base_replay_expected_error_reason"); value != "" {
		parts = append(parts, "base_replay_expected_reason="+value)
	}
	if value := detailString(details, "minimize_base_replay_actual_error_reason"); value != "" {
		parts = append(parts, "base_replay_actual_reason="+value)
	}
	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, " ")
}
