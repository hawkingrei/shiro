package schema

import (
	"fmt"
)

// ColumnType enumerates column data types.
type ColumnType int

const (
	TypeInt ColumnType = iota
	TypeBigInt
	TypeFloat
	TypeDouble
	TypeDecimal
	TypeVarchar
	TypeDate
	TypeDatetime
	TypeTimestamp
	TypeBool
)

// Column describes a table column.
type Column struct {
	Name     string
	Type     ColumnType
	Nullable bool
	HasIndex bool
}

// Table describes a database table.
type Table struct {
	Name    string
	Columns []Column
	HasPK   bool
	NextID  int64
}

// State tracks the current schema state.
type State struct {
	Tables []Table
}

// SQLType returns the SQL type string for this column.
func (c Column) SQLType() string {
	switch c.Type {
	case TypeInt:
		return "INT"
	case TypeBigInt:
		return "BIGINT"
	case TypeFloat:
		return "FLOAT"
	case TypeDouble:
		return "DOUBLE"
	case TypeDecimal:
		return "DECIMAL(12,2)"
	case TypeVarchar:
		return "VARCHAR(64)"
	case TypeDate:
		return "DATE"
	case TypeDatetime:
		return "DATETIME"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeBool:
		return "BOOLEAN"
	default:
		return "INT"
	}
}

// ColumnByName returns a column by name if present.
func (t Table) ColumnByName(name string) (Column, bool) {
	for _, col := range t.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return Column{}, false
}

// TableByName returns a table by name if present.
func (s State) TableByName(name string) (Table, bool) {
	for _, tbl := range s.Tables {
		if tbl.Name == name {
			return tbl, true
		}
	}
	return Table{}, false
}

// HasTables reports whether any tables exist in the schema state.
func (s State) HasTables() bool {
	return len(s.Tables) > 0
}

// ColumnRef builds a fully qualified column reference.
func ColumnRef(table, column string) string {
	return fmt.Sprintf("%s.%s", table, column)
}
