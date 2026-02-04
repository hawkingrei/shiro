package generator

// OrderByOrdinalIndex returns the ordinal index (1-based) when expr is a valid ORDER BY ordinal.
func OrderByOrdinalIndex(expr Expr, itemCount int) (int, bool) {
	if itemCount <= 0 {
		return 0, false
	}
	lit, ok := expr.(LiteralExpr)
	if !ok {
		return 0, false
	}
	ordinal, ok := literalInt(lit.Value)
	if !ok {
		return 0, false
	}
	if ordinal < 1 || ordinal > itemCount {
		return 0, false
	}
	return ordinal, true
}

func literalInt(value any) (int, bool) {
	maxInt := int(^uint(0) >> 1)
	switch v := value.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		if v > int64(maxInt) || v < -int64(maxInt)-1 {
			return 0, false
		}
		return int(v), true
	case uint:
		if v > uint(maxInt) {
			return 0, false
		}
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		if v > uint32(maxInt) {
			return 0, false
		}
		return int(v), true
	case uint64:
		if v > uint64(maxInt) {
			return 0, false
		}
		return int(v), true
	default:
		return 0, false
	}
}
