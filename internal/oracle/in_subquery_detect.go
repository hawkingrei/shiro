package oracle

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

// SQLSubqueryFeatures captures IN/EXISTS usage in a SQL statement.
type SQLSubqueryFeatures struct {
	HasInSubquery     bool
	HasNotInSubquery  bool
	HasInList         bool
	HasNotInList      bool
	HasExistsSubquery bool
	HasNotExists      bool
}

// ShouldDetectSubqueryFeaturesSQL is a fast-path guard to avoid parser overhead
// when the SQL text doesn't appear to reference IN/EXISTS patterns. It skips
// string literals and comments, but may still return true (causing a full
// parse) when IN/EXISTS only appear in identifiers. This function prioritizes
// avoiding false negatives over avoiding false positives.
func ShouldDetectSubqueryFeaturesSQL(sqlText string) bool {
	if strings.TrimSpace(sqlText) == "" {
		return false
	}
	upper := normalizeSQLForKeywordScan(sqlText)
	if upper == "" {
		return false
	}
	if !strings.Contains(upper, "IN") && !strings.Contains(upper, "EXISTS") {
		return false
	}
	if containsKeywordToken(upper, "EXISTS") {
		return true
	}
	if strings.Contains(upper, " NOT IN(") || strings.Contains(upper, " NOT IN (") ||
		strings.HasPrefix(upper, "NOT IN(") || strings.HasPrefix(upper, "NOT IN (") {
		return true
	}
	if strings.Contains(upper, " IN(") || strings.Contains(upper, " IN (") ||
		strings.HasPrefix(upper, "IN(") || strings.HasPrefix(upper, "IN (") {
		return true
	}
	return false
}

func containsKeywordToken(text string, keyword string) bool {
	if text == "" || keyword == "" {
		return false
	}
	for idx := 0; idx < len(text); {
		pos := strings.Index(text[idx:], keyword)
		if pos < 0 {
			return false
		}
		pos += idx
		beforeIdx := pos - 1
		afterIdx := pos + len(keyword)
		beforeOK := beforeIdx < 0 || !isIdentByte(text[beforeIdx])
		afterOK := afterIdx >= len(text) || !isIdentByte(text[afterIdx])
		if beforeOK && afterOK {
			return true
		}
		idx = pos + len(keyword)
	}
	return false
}

func isIdentByte(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '$'
}

func normalizeSQLForKeywordScan(sqlText string) string {
	var b strings.Builder
	b.Grow(len(sqlText))
	pendingSpace := false
	inString := false
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(sqlText); {
		if inLineComment {
			if sqlText[i] == '\n' || sqlText[i] == '\r' {
				inLineComment = false
			}
			i++
			continue
		}
		if inBlockComment {
			if sqlText[i] == '*' && i+1 < len(sqlText) && sqlText[i+1] == '/' {
				inBlockComment = false
				i += 2
				continue
			}
			i++
			continue
		}
		if inString {
			if sqlText[i] == '\'' {
				if i+1 < len(sqlText) && sqlText[i+1] == '\'' {
					i += 2
					continue
				}
				inString = false
			}
			i++
			continue
		}
		if sqlText[i] == '-' && i+1 < len(sqlText) && sqlText[i+1] == '-' {
			inLineComment = true
			i += 2
			continue
		}
		if sqlText[i] == '/' && i+1 < len(sqlText) && sqlText[i+1] == '*' {
			inBlockComment = true
			i += 2
			continue
		}
		if sqlText[i] == '\'' {
			inString = true
			i++
			continue
		}

		r, size := utf8.DecodeRuneInString(sqlText[i:])
		if r == utf8.RuneError && size == 1 {
			i++
			continue
		}
		if unicode.IsSpace(r) {
			pendingSpace = true
			i += size
			continue
		}
		if pendingSpace && b.Len() > 0 {
			b.WriteByte(' ')
		}
		pendingSpace = false
		b.WriteRune(unicode.ToUpper(r))
		i += size
	}
	return b.String()
}

// DetectSubqueryFeaturesSQL parses SQL and reports IN/EXISTS usage.
func DetectSubqueryFeaturesSQL(sqlText string) (features SQLSubqueryFeatures) {
	if strings.TrimSpace(sqlText) == "" {
		return SQLSubqueryFeatures{}
	}
	p := parser.New()
	stmt, err := p.ParseOneStmt(sqlText, "", "")
	if err != nil {
		return SQLSubqueryFeatures{}
	}
	visitor := &subqueryFeatureVisitor{}
	stmt.Accept(visitor)
	return visitor.features
}

// DetectInSubquerySQL reports whether the SQL contains IN/NOT IN with subquery.
func DetectInSubquerySQL(sqlText string) (hasInSubquery bool, hasNotInSubquery bool) {
	features := DetectSubqueryFeaturesSQL(sqlText)
	return features.HasInSubquery, features.HasNotInSubquery
}

type subqueryFeatureVisitor struct {
	features SQLSubqueryFeatures
}

func (v *subqueryFeatureVisitor) Enter(node ast.Node) (ast.Node, bool) {
	if v.features.HasInSubquery &&
		v.features.HasNotInSubquery &&
		v.features.HasInList &&
		v.features.HasNotInList &&
		v.features.HasExistsSubquery &&
		v.features.HasNotExists {
		return node, true
	}
	switch expr := node.(type) {
	case *ast.UnaryOperationExpr:
		if expr.Op == opcode.Not {
			if inExpr := unwrapPatternIn(expr.V); inExpr != nil {
				if patternInHasSubquery(inExpr) {
					v.features.HasNotInSubquery = true
					return node, true
				}
			}
		}
	case *ast.PatternInExpr:
		if patternInHasSubquery(expr) {
			if expr.Not {
				v.features.HasNotInSubquery = true
			} else {
				v.features.HasInSubquery = true
			}
		} else {
			if expr.Not {
				v.features.HasNotInList = true
			} else {
				v.features.HasInList = true
			}
		}
	case *ast.ExistsSubqueryExpr:
		if expr.Not {
			v.features.HasNotExists = true
		} else {
			v.features.HasExistsSubquery = true
		}
	}
	return node, false
}

func (v *subqueryFeatureVisitor) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

func unwrapPatternIn(node ast.ExprNode) *ast.PatternInExpr {
	switch expr := node.(type) {
	case *ast.PatternInExpr:
		return expr
	case *ast.ParenthesesExpr:
		return unwrapPatternIn(expr.Expr)
	default:
		return nil
	}
}

func patternInHasSubquery(expr *ast.PatternInExpr) bool {
	if expr == nil {
		return false
	}
	if expr.Sel != nil {
		return true
	}
	for _, item := range expr.List {
		if _, ok := item.(*ast.SubqueryExpr); ok {
			return true
		}
	}
	return false
}
