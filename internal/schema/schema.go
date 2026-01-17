package schema

import (
	"fmt"
)

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

type Column struct {
	Name     string
	Type     ColumnType
	Nullable bool
	HasIndex bool
}

type Table struct {
	Name    string
	Columns []Column
	HasPK   bool
	NextID  int64
}

type State struct {
	Tables []Table
}

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

func (t Table) ColumnByName(name string) (Column, bool) {
	for _, col := range t.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return Column{}, false
}

func (s State) TableByName(name string) (Table, bool) {
	for _, tbl := range s.Tables {
		if tbl.Name == name {
			return tbl, true
		}
	}
	return Table{}, false
}

func (s State) HasTables() bool {
	return len(s.Tables) > 0
}

func ColumnRef(table, column string) string {
	return fmt.Sprintf("%s.%s", table, column)
}
