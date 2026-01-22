package runner

import (
	"fmt"
	"strings"
)

func formatPrepareSQL(sqlText string) string {
	return fmt.Sprintf("PREPARE stmt FROM '%s'", strings.ReplaceAll(sqlText, "'", "''"))
}

func formatExecuteSQLWithVars(name string, args []any) []string {
	if len(args) == 0 {
		return []string{fmt.Sprintf("EXECUTE %s", name)}
	}
	values := formatArgs(args)
	setParts := make([]string, len(values))
	useParts := make([]string, len(values))
	for i, v := range values {
		varName := fmt.Sprintf("@p%d", i+1)
		setParts[i] = fmt.Sprintf("%s=%s", varName, v)
		useParts[i] = varName
	}
	return []string{
		"SET " + strings.Join(setParts, ", "),
		fmt.Sprintf("EXECUTE %s USING %s", name, strings.Join(useParts, ", ")),
	}
}

func formatArgs(args []any) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		switch v := arg.(type) {
		case nil:
			out = append(out, "NULL")
		case string:
			out = append(out, fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")))
		default:
			out = append(out, fmt.Sprintf("%v", v))
		}
	}
	return out
}

func materializeSQL(sqlText string, args []any) string {
	if len(args) == 0 {
		return sqlText
	}
	formatted := formatArgs(args)
	var b strings.Builder
	argIdx := 0
	for _, r := range sqlText {
		if r == '?' && argIdx < len(formatted) {
			b.WriteString(formatted[argIdx])
			argIdx++
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
