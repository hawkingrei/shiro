package runner

import (
	"errors"
	"fmt"
	"strings"

	"shiro/internal/oracle"
	"shiro/internal/util"

	"github.com/go-sql-driver/mysql"
)

func isPanicError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "panic") || strings.Contains(msg, "assert") || strings.Contains(msg, "internal error")
}

func isRuntimeError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "runtime error")
}

// sqlErrorWhitelist lists MySQL error codes considered fuzz-tool faults.
// 1064 is the generic SQL syntax error, common for malformed generated SQL.
// 1292 is a type truncation error triggered by type-mismatched predicates.
// 1451 is a foreign key constraint failure when deleting/updating parent rows.
// 1452 is a foreign key constraint failure during child insert/update.
var sqlErrorWhitelist = map[uint16]struct{}{
	1064: {},
	1292: {},
	1451: {},
	1452: {},
}

func isWhitelistedSQLError(err error) (uint16, bool) {
	if err == nil {
		return 0, false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		_, ok := sqlErrorWhitelist[mysqlErr.Number]
		return mysqlErr.Number, ok
	}
	return 0, false
}

func logWhitelistedSQLError(sqlText string, err error, verbose bool) bool {
	if isUnknownColumnWhereErr(err) {
		if verbose {
			util.Detailf("sql error whitelisted reason=unknown_column_where sql=%s err=%v", sqlText, err)
		}
		return true
	}
	code, ok := isWhitelistedSQLError(err)
	if !ok {
		return false
	}
	if verbose {
		util.Detailf("sql error whitelisted code=%d sql=%s err=%v", code, sqlText, err)
	}
	return true
}

func isMySQLError(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr)
}

func isUnknownColumnWhereErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown column") && strings.Contains(msg, "in where clause")
}

func mysqlErrCode(err error) (uint16, bool) {
	if err == nil {
		return 0, false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number, true
	}
	return 0, false
}

func errorReasonPrefix(oracleName string) string {
	name := strings.ToLower(strings.TrimSpace(oracleName))
	if name == "" {
		return "oracle"
	}
	return name
}

func classifyInfraIssue(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "region is unavailable"):
		return "region_unavailable", true
	case strings.Contains(msg, "information schema is out of date"),
		strings.Contains(msg, "schema failed to update in 1 lease"):
		return "schema_out_of_date", true
	case strings.Contains(msg, "connect to tikv"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "connection reset by peer"):
		return "tikv_connectivity", true
	default:
		return "", false
	}
}

func classifyResultError(oracleName string, err error) (reason string, bugHint string) {
	if err == nil {
		return "", ""
	}
	prefix := errorReasonPrefix(oracleName)
	if oracle.IsSchemaColumnMissingErr(err) {
		return prefix + ":missing_column", "tidb:schema_column_missing"
	}
	if oracle.IsPlanRefMissingErr(err) {
		return prefix + ":plan_ref_missing", "tidb:plan_reference_missing"
	}
	if infraReason, ok := classifyInfraIssue(err); ok {
		return prefix + ":" + infraReason, "tidb:infra_unhealthy"
	}
	if isUnknownColumnWhereErr(err) {
		return prefix + ":unknown_column_where", ""
	}
	if isRuntimeError(err) {
		return prefix + ":runtime_error", "tidb:runtime_error"
	}
	if code, ok := mysqlErrCode(err); ok {
		return fmt.Sprintf("%s:sql_error_%d", prefix, code), ""
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "context deadline exceeded") || strings.Contains(msg, "timeout") {
		return prefix + ":timeout", ""
	}
	return prefix + ":sql_error", ""
}

func shouldDowngradeMissingColumn(oracleName string) bool {
	switch strings.ToLower(strings.TrimSpace(oracleName)) {
	case "eet", "impo", "pqs":
		return true
	default:
		return false
	}
}

func downgradeMissingColumnFalsePositive(result *oracle.Result) bool {
	if result == nil || result.Err == nil {
		return false
	}
	if !oracle.IsSchemaColumnMissingErr(result.Err) {
		return false
	}
	if !shouldDowngradeMissingColumn(result.Oracle) {
		return false
	}
	if result.Details == nil {
		result.Details = map[string]any{}
	}
	prefix := errorReasonPrefix(result.Oracle)
	if _, ok := result.Details["skip_reason"]; !ok {
		result.Details["skip_reason"] = prefix + ":missing_column"
	}
	result.Details["skip_error_reason"] = prefix + ":missing_column"
	if _, ok := result.Details["skip_error"]; !ok {
		result.Details["skip_error"] = result.Err.Error()
	}
	delete(result.Details, "error_reason")
	delete(result.Details, "bug_hint")
	result.OK = true
	result.Err = nil
	return true
}

func downgradeGroundTruthLowConfidenceFalsePositive(result *oracle.Result) bool {
	if result == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(result.Oracle), "GroundTruth") {
		return false
	}
	if result.Details == nil {
		return false
	}
	confidence, _ := result.Details["groundtruth_confidence"].(string)
	if confidence != "fallback_dsg" {
		return false
	}
	// Keep strict DSG mismatches reportable, but downgrade bounded fallback picks
	// to skip so low-confidence captures do not dominate reports.
	if result.Truth == nil || !result.Truth.Enabled || !result.Truth.Mismatch {
		return false
	}
	if _, ok := result.Details["skip_reason"]; !ok {
		result.Details["skip_reason"] = "groundtruth:low_confidence_fallback"
	}
	result.Details["skip_error_reason"] = "groundtruth:count_mismatch"
	if _, ok := result.Details["skip_error"]; !ok {
		result.Details["skip_error"] = fmt.Sprintf("%s != %s", result.Expected, result.Actual)
	}
	delete(result.Details, "error_reason")
	delete(result.Details, "bug_hint")
	result.OK = true
	result.Err = nil
	return true
}

func shouldDowngradeDQPTimeout(oracleName string) bool {
	return strings.EqualFold(strings.TrimSpace(oracleName), "dqp")
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if code, ok := mysqlErrCode(err); ok {
		switch code {
		case 1317, 3024:
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "timeout")
}

func downgradeDQPTimeoutFalsePositive(result *oracle.Result) bool {
	if result == nil || result.Err == nil {
		return false
	}
	if !shouldDowngradeDQPTimeout(result.Oracle) {
		return false
	}
	if !isTimeoutError(result.Err) {
		return false
	}
	if result.Details == nil {
		result.Details = map[string]any{}
	}
	if _, ok := result.Details["skip_reason"]; !ok {
		result.Details["skip_reason"] = "dqp:timeout"
	}
	result.Details["skip_error_reason"] = "dqp:timeout"
	if _, ok := result.Details["skip_error"]; !ok {
		result.Details["skip_error"] = result.Err.Error()
	}
	delete(result.Details, "error_reason")
	delete(result.Details, "bug_hint")
	result.OK = true
	result.Err = nil
	return true
}

func annotateResultForReporting(result *oracle.Result) {
	if result == nil {
		return
	}
	if result.Details == nil {
		result.Details = map[string]any{}
	}
	if result.Err != nil {
		reason, hint := classifyResultError(result.Oracle, result.Err)
		if reason != "" {
			if _, ok := result.Details["error_reason"]; !ok {
				result.Details["error_reason"] = reason
			}
			result.OK = false
		}
		if hint != "" {
			if _, ok := result.Details["bug_hint"]; !ok {
				result.Details["bug_hint"] = hint
			}
		}
		return
	}
	if skip, ok := result.Details["skip_reason"].(string); ok && strings.TrimSpace(skip) != "" {
		return
	}
	if strings.EqualFold(result.Oracle, "GroundTruth") && result.Truth != nil && result.Truth.Enabled && result.Truth.Mismatch {
		if _, ok := result.Details["error_reason"]; !ok {
			result.Details["error_reason"] = "groundtruth:count_mismatch"
		}
		if _, ok := result.Details["bug_hint"]; !ok {
			result.Details["bug_hint"] = "tidb:result_inconsistency"
		}
	}
}
