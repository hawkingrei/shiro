package generator

import "strings"

// SQLBuilder builds SQL with parameter tracking.
type SQLBuilder struct {
	sb   strings.Builder
	args []any
}

// Write appends raw SQL text to the builder.
func (b *SQLBuilder) Write(s string) {
	b.sb.WriteString(s)
}

// WriteArg appends a parameter value and writes a placeholder.
func (b *SQLBuilder) WriteArg(v any) {
	b.args = append(b.args, v)
	b.sb.WriteString("?")
}

// String returns the assembled SQL statement.
func (b *SQLBuilder) String() string {
	return b.sb.String()
}

// Args returns the accumulated parameter arguments.
func (b *SQLBuilder) Args() []any {
	return b.args
}
