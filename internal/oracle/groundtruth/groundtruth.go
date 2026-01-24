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

// TypedValue encodes a scalar value for RowIDMap keys.
type TypedValue struct {
	Type  string
	Value string
}

// TODO: Wire join key extraction into oracle execution for JoinEdge building.
