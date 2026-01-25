package oracle

import "strings"

func isSchemaColumnMissingErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "can't find column") && strings.Contains(msg, "in schema column")
}
