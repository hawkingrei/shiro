package groundtruth

import (
	"strconv"
	"time"

	"shiro/internal/schema"
)

// TypeFamily collapses column types into coarse categories for keying.
func TypeFamily(t schema.ColumnType) string {
	switch t {
	case schema.TypeInt, schema.TypeBigInt, schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
		return "number"
	case schema.TypeVarchar:
		return "string"
	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return "time"
	case schema.TypeBool:
		return "bool"
	default:
		return "other"
	}
}

// EncodeValue normalizes a scalar into a string key for RowID lookups.
func EncodeValue(t schema.ColumnType, v any) (TypedValue, bool) {
	if v == nil {
		return TypedValue{Type: TypeFamily(t), Value: "NULL"}, true
	}
	switch t {
	case schema.TypeInt, schema.TypeBigInt:
		switch n := v.(type) {
		case int:
			return TypedValue{Type: "number", Value: strconv.FormatInt(int64(n), 10)}, true
		case int64:
			return TypedValue{Type: "number", Value: strconv.FormatInt(n, 10)}, true
		case uint64:
			return TypedValue{Type: "number", Value: strconv.FormatUint(n, 10)}, true
		}
	case schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
		switch n := v.(type) {
		case float64:
			return TypedValue{Type: "number", Value: strconv.FormatFloat(n, 'g', -1, 64)}, true
		case float32:
			return TypedValue{Type: "number", Value: strconv.FormatFloat(float64(n), 'g', -1, 64)}, true
		}
	case schema.TypeVarchar:
		if s, ok := v.(string); ok {
			return TypedValue{Type: "string", Value: s}, true
		}
	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		switch tv := v.(type) {
		case time.Time:
			return TypedValue{Type: "time", Value: tv.Format(time.RFC3339Nano)}, true
		case string:
			return TypedValue{Type: "time", Value: tv}, true
		}
	case schema.TypeBool:
		if b, ok := v.(bool); ok {
			if b {
				return TypedValue{Type: "bool", Value: "1"}, true
			}
			return TypedValue{Type: "bool", Value: "0"}, true
		}
	}
	return TypedValue{Type: TypeFamily(t), Value: ""}, false
}
