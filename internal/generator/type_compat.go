package generator

import "shiro/internal/schema"

func compatibleColumnType(left, right schema.ColumnType) bool {
	return TypeCategory(left) == TypeCategory(right)
}

func TypeCategory(t schema.ColumnType) int {
	switch t {
	case schema.TypeInt, schema.TypeBigInt, schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
		return 0
	case schema.TypeVarchar:
		return 1
	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return 2
	case schema.TypeBool:
		return 3
	default:
		return 4
	}
}
