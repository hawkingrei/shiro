package oracle

import (
	"fmt"
	"strings"
)

// IsSchemaColumnMissingErr reports whether the error indicates a missing column.
func IsSchemaColumnMissingErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "can't find column")
}

// IsPlanRefMissingErr reports whether the error indicates a plan column reference failure.
func IsPlanRefMissingErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "cannot find the reference from its child") {
		return true
	}
	return strings.Contains(msg, "some columns of topn")
}

func sqlErrorReason(prefix string, err error) (string, uint16) {
	if err == nil {
		return prefix + ":sql_error", 0
	}
	if IsSchemaColumnMissingErr(err) {
		return prefix + ":missing_column", 0
	}
	if IsPlanRefMissingErr(err) {
		return prefix + ":plan_ref_missing", 0
	}
	if code, ok := mysqlErrCode(err); ok {
		return fmt.Sprintf("%s:sql_error_%d", prefix, code), code
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "context deadline exceeded") || strings.Contains(msg, "timeout") {
		return prefix + ":timeout", 0
	}
	return prefix + ":sql_error", 0
}
