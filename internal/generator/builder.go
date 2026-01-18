package generator

import "strings"

// SQLBuilder builds SQL with parameter tracking.
type SQLBuilder struct {
	sb   strings.Builder
	args []any
}

func (b *SQLBuilder) Write(s string) {
	b.sb.WriteString(s)
}

func (b *SQLBuilder) WriteArg(v any) {
	b.args = append(b.args, v)
	b.sb.WriteString("?")
}

func (b *SQLBuilder) String() string {
	return b.sb.String()
}

func (b *SQLBuilder) Args() []any {
	return b.args
}
