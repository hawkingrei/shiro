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

func classifyResultError(oracleName string, err error) (reason string, bugHint string) {
	if err == nil {
		return "", ""
	}
	prefix := errorReasonPrefix(oracleName)
	if oracle.IsSchemaColumnMissingErr(err) {
		return prefix + ":missing_column", "tidb:schema_column_missing"
	}
	if oracle.IsPlanRefMissingErr(err) {
		return prefix + ":plan_ref_missing", "tidb:planner_ref_missing"
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
	if strings.EqualFold(result.Oracle, "GroundTruth") && result.Truth != nil && result.Truth.Enabled && result.Truth.Mismatch {
		if _, ok := result.Details["error_reason"]; !ok {
			result.Details["error_reason"] = "groundtruth:count_mismatch"
		}
		if _, ok := result.Details["bug_hint"]; !ok {
			result.Details["bug_hint"] = "tidb:result_inconsistency"
		}
	}
}
