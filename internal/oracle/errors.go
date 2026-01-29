package oracle

import "strings"

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
