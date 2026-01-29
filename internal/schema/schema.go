// Package schema defines schema state and helpers for SQL generation.
package schema

import (
	"fmt"
)

// ColumnType enumerates column data types.
type ColumnType int

// Column type constants for schema generation.
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

// Index describes a (potentially multi-column) index.
type Index struct {
	Name    string
	Columns []string
}

// ForeignKey describes a single-column foreign key.
type ForeignKey struct {
	Name      string
	Table     string
	Column    string
	RefTable  string
	RefColumn string
}

// Table describes a database table.
type Table struct {
	Name           string
	Columns        []Column
	Indexes        []Index
	ForeignKeys    []ForeignKey
	HasPK          bool
	NextID         int64
	Partitioned    bool
	PartitionCount int
	IsView         bool
}

// State tracks the current schema state.
type State struct {
	Tables []Table
}

// SplitTablesByView separates base tables from views.
func SplitTablesByView(tables []Table) (base []Table, views []Table) {
	base = make([]Table, 0, len(tables))
	views = make([]Table, 0, len(tables))
	for _, tbl := range tables {
		if tbl.IsView {
			views = append(views, tbl)
		} else {
			base = append(base, tbl)
		}
	}
	return base, views
}

// BaseTables returns non-view tables in creation order.
func (s State) BaseTables() []Table {
	out := make([]Table, 0, len(s.Tables))
	for _, tbl := range s.Tables {
		if tbl.IsView {
			continue
		}
		out = append(out, tbl)
	}
	return out
}

// HasBaseTables reports whether any non-view tables exist.
func (s State) HasBaseTables() bool {
	for _, tbl := range s.Tables {
		if !tbl.IsView {
			return true
		}
	}
	return false
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
