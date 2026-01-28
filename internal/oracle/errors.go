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
