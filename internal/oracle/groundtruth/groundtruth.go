package groundtruth

// Package groundtruth implements join truth computation for TQS.
//
// RowID is a stable identifier for a row in the wide table. Each normalized
// table row maps back to one or more RowIDs, and join truth is computed by
// bitmap operations over RowID sets.

// WideTable describes the row universe used for truth evaluation.
type WideTable struct {
	RowCount int
}

// RowIDMap stores the RowID bitmap for each normalized row key.
// The key format is intentionally flexible and can be derived from schema data.
type RowIDMap map[string]Bitmap

// TableRows captures RowID mappings for a normalized table.
type TableRows struct {
	TableName string
	// Columns maps column name to value->RowID bitmap.
	Columns map[string]RowIDMap
	// Rows is a fallback value->RowID bitmap when column granularity is not available.
	Rows RowIDMap
	// RowsData stores normalized rows for exact join counting.
	RowsData []map[string]TypedValue
}

// SchemaTruth bundles the RowID universe and normalized tables.
type SchemaTruth struct {
	Wide   WideTable
	Tables map[string]TableRows
}

// JoinEdge describes a join between left and right tables.
type JoinEdge struct {
	LeftTable  string
	RightTable string
	LeftKey    string
	RightKey   string
	JoinType   JoinType
}

// JoinType enumerates join semantics used by truth executor.
type JoinType int

// Join type constants for truth evaluation.
const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinSemi
	JoinAnti
	JoinCross
)

// JoinTruthExecutor computes truth RowID bitmap for join chains.
type JoinTruthExecutor struct {
	Truth SchemaTruth
}

// Result captures a truth-evaluation summary.
type Result struct {
	OK         bool
	JoinCount  int
	JoinSig    string
	RowCount   int
	Details    map[string]any
	OracleHint string
}

// EvalJoinChain returns the resulting RowID bitmap for a join chain.
// For now, it assumes RowID keys exist on both sides and uses bitmap ops.
func (e *JoinTruthExecutor) EvalJoinChain(baseTable string, edges []JoinEdge) Bitmap {
	base := e.tableAllRows(baseTable)
	if base.IsEmpty() {
		return Bitmap{}
	}
	result := base
	for _, edge := range edges {
		// Join truth on RowID sets is approximate for outer joins because
		// null-extension does not create new RowIDs. We keep unmatched-side
		// RowIDs to avoid dropping rows, but projections are still derived
		// from the wide-table universe.
		leftAll := e.tableAllRows(edge.LeftTable)
		rightAll := e.tableAllRows(edge.RightTable)
		joinRows := e.equalityJoin(edge.LeftTable, edge.LeftKey, edge.RightTable, edge.RightKey)
		switch edge.JoinType {
		case JoinInner:
			result = result.And(joinRows)
		case JoinLeft:
			// RowID set does not represent null-extended rows.
			// Preserve left side rows to avoid dropping unmatched left rows.
			result = result.Or(leftAll)
		case JoinRight:
			result = result.Or(rightAll)
		case JoinSemi:
			result = result.And(joinRows)
		case JoinAnti:
			result = result.And(leftAll)
			result = result.Sub(joinRows)
		case JoinCross:
			result = result.Or(rightAll)
		default:
			result = result.And(joinRows)
		}
	}
	return result
}

const (
	defaultTableCap = 50
	defaultJoinCap  = 50
)

// EvalJoinChainExact counts join results with multiplicity using normalized rows.
// tableCap limits the number of rows per table; joinCap limits intermediate join rows.
// It returns ok=false when row data is missing or caps are exceeded.
func (e *JoinTruthExecutor) EvalJoinChainExact(baseTable string, edges []JoinEdge, tableCap int, joinCap int) (count int, ok bool, reason string) {
	baseRows := e.tableRowsData(baseTable)
	if len(baseRows) == 0 {
		return 0, false, "missing_rows"
	}
	if tableCap <= 0 {
		tableCap = defaultTableCap
	}
	if joinCap <= 0 {
		joinCap = defaultJoinCap
	}
	if len(baseRows) > tableCap {
		// Skip oversized tables to preserve exactness.
		return 0, false, "table_rows_exceeded"
	}
	composite := make([]map[string]map[string]TypedValue, 0, len(baseRows))
	for _, row := range baseRows {
		composite = append(composite, map[string]map[string]TypedValue{baseTable: row})
	}
	for _, edge := range edges {
		rightRows := e.tableRowsData(edge.RightTable)
		if len(rightRows) == 0 {
			return 0, false, "missing_rows"
		}
		if len(rightRows) > tableCap {
			return 0, false, "table_rows_exceeded"
		}
		var next []map[string]map[string]TypedValue
		switch edge.JoinType {
		case JoinCross:
			estimate := len(composite) * len(rightRows)
			if joinCap > 0 && estimate > joinCap {
				estimate = joinCap
			}
			next = make([]map[string]map[string]TypedValue, 0, estimate)
			for _, leftRow := range composite {
				for _, rightRow := range rightRows {
					if joinCap > 0 && len(next) >= joinCap {
						return 0, false, "join_rows_exceeded"
					}
					merged := copyComposite(leftRow)
					merged[edge.RightTable] = rightRow
					next = append(next, merged)
				}
			}
		case JoinRight:
			leftIndex := buildCompositeIndex(composite, edge.LeftTable, edge.LeftKey)
			next = make([]map[string]map[string]TypedValue, 0, len(rightRows))
			for _, rightRow := range rightRows {
				key, ok := rowKey(rightRow, edge.RightKey)
				if !ok {
					if joinCap > 0 && len(next) >= joinCap {
						return 0, false, "join_rows_exceeded"
					}
					next = append(next, map[string]map[string]TypedValue{edge.RightTable: rightRow})
					continue
				}
				matches := leftIndex[key]
				if len(matches) == 0 {
					if joinCap > 0 && len(next) >= joinCap {
						return 0, false, "join_rows_exceeded"
					}
					next = append(next, map[string]map[string]TypedValue{edge.RightTable: rightRow})
					continue
				}
				for _, leftRow := range matches {
					if joinCap > 0 && len(next) >= joinCap {
						return 0, false, "join_rows_exceeded"
					}
					merged := copyComposite(leftRow)
					merged[edge.RightTable] = rightRow
					next = append(next, merged)
				}
			}
		case JoinLeft, JoinSemi, JoinAnti, JoinInner:
			index := buildRowIndex(rightRows, edge.RightKey)
			next = make([]map[string]map[string]TypedValue, 0, len(composite))
			for _, leftRow := range composite {
				lrow := leftRow[edge.LeftTable]
				key, ok := rowKey(lrow, edge.LeftKey)
				if !ok {
					if edge.JoinType == JoinLeft || edge.JoinType == JoinAnti {
						if joinCap > 0 && len(next) >= joinCap {
							return 0, false, "join_rows_exceeded"
						}
						next = append(next, copyComposite(leftRow))
					}
					continue
				}
				matches := index[key]
				switch edge.JoinType {
				case JoinInner:
					for _, rightRow := range matches {
						if joinCap > 0 && len(next) >= joinCap {
							return 0, false, "join_rows_exceeded"
						}
						merged := copyComposite(leftRow)
						merged[edge.RightTable] = rightRow
						next = append(next, merged)
					}
				case JoinLeft:
					if len(matches) == 0 {
						if joinCap > 0 && len(next) >= joinCap {
							return 0, false, "join_rows_exceeded"
						}
						next = append(next, copyComposite(leftRow))
						continue
					}
					for _, rightRow := range matches {
						if joinCap > 0 && len(next) >= joinCap {
							return 0, false, "join_rows_exceeded"
						}
						merged := copyComposite(leftRow)
						merged[edge.RightTable] = rightRow
						next = append(next, merged)
					}
				case JoinSemi:
					if len(matches) > 0 {
						if joinCap > 0 && len(next) >= joinCap {
							return 0, false, "join_rows_exceeded"
						}
						next = append(next, copyComposite(leftRow))
					}
				case JoinAnti:
					if len(matches) == 0 {
						if joinCap > 0 && len(next) >= joinCap {
							return 0, false, "join_rows_exceeded"
						}
						next = append(next, copyComposite(leftRow))
					}
				}
			}
		default:
			return 0, false, "unsupported_join"
		}
		composite = next
		if len(composite) == 0 {
			return 0, true, ""
		}
	}
	return len(composite), true, ""
}

func (e *JoinTruthExecutor) tableAllRows(table string) Bitmap {
	tbl, ok := e.Truth.Tables[table]
	if !ok {
		return Bitmap{}
	}
	var out Bitmap
	for _, bm := range tbl.allRowBitmaps() {
		if out.words == nil {
			out = bm.Clone()
			continue
		}
		out.OrWith(bm)
	}
	return out
}

func (e *JoinTruthExecutor) equalityJoin(leftTable, leftKey, rightTable, rightKey string) Bitmap {
	leftCol := e.tableColumn(leftTable, leftKey)
	rightCol := e.tableColumn(rightTable, rightKey)
	if leftCol == nil || rightCol == nil {
		return Bitmap{}
	}
	var out Bitmap
	for key, leftBM := range leftCol {
		rightBM, ok := rightCol[key]
		if !ok {
			continue
		}
		inter := leftBM.And(rightBM)
		if out.words == nil {
			out = inter
			continue
		}
		out.OrWith(inter)
	}
	return out
}

func (e *JoinTruthExecutor) tableColumn(table, col string) RowIDMap {
	tbl, ok := e.Truth.Tables[table]
	if !ok {
		return nil
	}
	if tbl.Columns != nil {
		if m, ok := tbl.Columns[col]; ok {
			return m
		}
	}
	if tbl.Rows != nil {
		return tbl.Rows
	}
	return nil
}

func (e *JoinTruthExecutor) tableRowsData(table string) []map[string]TypedValue {
	tbl, ok := e.Truth.Tables[table]
	if !ok {
		return nil
	}
	return tbl.RowsData
}

func (t TableRows) allRowBitmaps() []Bitmap {
	if t.Columns != nil {
		merged := make([]Bitmap, 0, len(t.Columns))
		for _, m := range t.Columns {
			for _, bm := range m {
				merged = append(merged, bm)
			}
		}
		return merged
	}
	merged := make([]Bitmap, 0, len(t.Rows))
	for _, bm := range t.Rows {
		merged = append(merged, bm)
	}
	return merged
}

func rowKey(row map[string]TypedValue, col string) (string, bool) {
	tv, ok := row[col]
	if !ok {
		return "", false
	}
	if tv.Value == "NULL" {
		return "", false
	}
	return NewKey(tv.Type, tv.Value), true
}

func buildRowIndex(rows []map[string]TypedValue, col string) map[string][]map[string]TypedValue {
	index := make(map[string][]map[string]TypedValue, len(rows))
	for _, row := range rows {
		key, ok := rowKey(row, col)
		if !ok {
			continue
		}
		index[key] = append(index[key], row)
	}
	return index
}

func copyComposite(src map[string]map[string]TypedValue) map[string]map[string]TypedValue {
	out := make(map[string]map[string]TypedValue, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func buildCompositeIndex(rows []map[string]map[string]TypedValue, table, col string) map[string][]map[string]map[string]TypedValue {
	index := make(map[string][]map[string]map[string]TypedValue, len(rows))
	for _, row := range rows {
		tr := row[table]
		key, ok := rowKey(tr, col)
		if !ok {
			continue
		}
		index[key] = append(index[key], row)
	}
	return index
}

func minInt(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

// NewKey encodes a join key from typed values for RowIDMap lookup.
// It uses a stable, lossless encoding for basic scalar types.
func NewKey(typ string, val string) string {
	// Type prefix avoids collisions like "1" vs "01".
	return typ + ":" + val
}

// NewSchemaTruth initializes an empty truth holder with row count.
func NewSchemaTruth(rowCount int) SchemaTruth {
	return SchemaTruth{
		Wide:   WideTable{RowCount: rowCount},
		Tables: make(map[string]TableRows),
	}
}

// AddTable registers a table in truth storage.
func (s *SchemaTruth) AddTable(name string) {
	if s.Tables == nil {
		s.Tables = make(map[string]TableRows)
	}
	if _, ok := s.Tables[name]; ok {
		return
	}
	s.Tables[name] = TableRows{
		TableName: name,
		Columns:   make(map[string]RowIDMap),
		Rows:      make(RowIDMap),
		RowsData:  nil,
	}
}

// AddColumnValue associates a typed value with a RowID for a table column.
func (s *SchemaTruth) AddColumnValue(table, col, typ, val string, id RowID) {
	tbl := s.Tables[table]
	if tbl.Columns == nil {
		tbl.Columns = make(map[string]RowIDMap)
	}
	m := tbl.Columns[col]
	if m == nil {
		m = make(RowIDMap)
	}
	key := NewKey(typ, val)
	bm := m[key]
	bm.Set(id)
	m[key] = bm
	tbl.Columns[col] = m
	s.Tables[table] = tbl
}

// AddRowValue associates a typed value with a RowID at row-level granularity.
func (s *SchemaTruth) AddRowValue(table, typ, val string, id RowID) {
	tbl := s.Tables[table]
	if tbl.Rows == nil {
		tbl.Rows = make(RowIDMap)
	}
	key := NewKey(typ, val)
	bm := tbl.Rows[key]
	bm.Set(id)
	tbl.Rows[key] = bm
	s.Tables[table] = tbl
}

// AddRow adds a row with column values, updating column-level RowID maps.
func (s *SchemaTruth) AddRow(table string, id RowID, values map[string]TypedValue) {
	if len(values) == 0 {
		return
	}
	if _, ok := s.Tables[table]; !ok {
		s.AddTable(table)
	}
	for col, tv := range values {
		s.AddColumnValue(table, col, tv.Type, tv.Value, id)
	}
}

// AddRowData appends a normalized row for exact join counting.
func (s *SchemaTruth) AddRowData(table string, values map[string]TypedValue) {
	if len(values) == 0 {
		return
	}
	if _, ok := s.Tables[table]; !ok {
		s.AddTable(table)
	}
	tbl := s.Tables[table]
	tbl.RowsData = append(tbl.RowsData, values)
	s.Tables[table] = tbl
}

// TypedValue encodes a scalar value for RowIDMap keys.
type TypedValue struct {
	Type  string
	Value string
}

// TODO: Wire join key extraction into oracle execution for JoinEdge building.
