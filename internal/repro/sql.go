package repro

import "strings"

func splitSQL(input string) []string {
	var out []string
	var buf strings.Builder
	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false
	escaped := false
	prev := byte(0)
	for i := 0; i < len(input); i++ {
		ch := input[i]
		next := byte(0)
		if i+1 < len(input) {
			next = input[i+1]
		}
		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			buf.WriteByte(ch)
			prev = ch
			continue
		}
		if inBlockComment {
			if ch == '*' && next == '/' {
				inBlockComment = false
				buf.WriteByte(ch)
				buf.WriteByte(next)
				i++
				prev = next
				continue
			}
			buf.WriteByte(ch)
			prev = ch
			continue
		}
		if !inSingle && !inDouble && !inBacktick {
			if ch == '-' && next == '-' && (i == 0 || isSpace(prev)) {
				inLineComment = true
				buf.WriteByte(ch)
				buf.WriteByte(next)
				i++
				prev = next
				continue
			}
			if ch == '#' {
				inLineComment = true
				buf.WriteByte(ch)
				prev = ch
				continue
			}
			if ch == '/' && next == '*' {
				inBlockComment = true
				buf.WriteByte(ch)
				buf.WriteByte(next)
				i++
				prev = next
				continue
			}
		}
		if inSingle {
			if ch == '\\' && !escaped {
				escaped = true
				buf.WriteByte(ch)
				prev = ch
				continue
			}
			if ch == '\'' && !escaped {
				inSingle = false
			}
			buf.WriteByte(ch)
			escaped = false
			prev = ch
			continue
		}
		if inDouble {
			if ch == '\\' && !escaped {
				escaped = true
				buf.WriteByte(ch)
				prev = ch
				continue
			}
			if ch == '"' && !escaped {
				inDouble = false
			}
			buf.WriteByte(ch)
			escaped = false
			prev = ch
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			buf.WriteByte(ch)
			prev = ch
			continue
		}
		if ch == '\'' {
			inSingle = true
			buf.WriteByte(ch)
			prev = ch
			continue
		}
		if ch == '"' {
			inDouble = true
			buf.WriteByte(ch)
			prev = ch
			continue
		}
		if ch == '`' {
			inBacktick = true
			buf.WriteByte(ch)
			prev = ch
			continue
		}
		if ch == ';' && !inSingle && !inDouble && !inBacktick {
			stmt := strings.TrimSpace(buf.String())
			if stmt != "" {
				out = append(out, stmt)
			}
			buf.Reset()
			prev = ch
			continue
		}
		buf.WriteByte(ch)
		prev = ch
	}
	stmt := strings.TrimSpace(buf.String())
	if stmt != "" {
		out = append(out, stmt)
	}
	return out
}

func isSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}
