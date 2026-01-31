package tqs

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"shiro/internal/config"
	"shiro/internal/oracle/groundtruth"
	"shiro/internal/schema"
)

// BuildResult contains DSG schema/data plus ground-truth bitmaps.
type BuildResult struct {
	State     *schema.State
	CreateSQL []string
	InsertSQL []string
	Truth     *groundtruth.SchemaTruth
}

// Build generates a DSG schema, inserts, and RowID bitmap truth.
func Build(cfg config.Config, r *rand.Rand) (BuildResult, error) {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	rows := cfg.TQS.WideRows
	if rows <= 0 {
		rows = cfg.MaxRowsPerTable
	}
	if rows <= 0 {
		rows = 50
	}
	dimTables := cfg.TQS.DimTables
	if dimTables <= 0 {
		dimTables = 3
	}
	if cfg.MaxTables > 0 && dimTables > cfg.MaxTables-1 {
		dimTables = cfg.MaxTables - 1
	}
	if dimTables < 1 {
		return BuildResult{}, fmt.Errorf("tqs: insufficient table count for DSG")
	}
	depCols := cfg.TQS.DepColumns
	if depCols <= 0 {
		depCols = 2
	}
	payloadCols := cfg.TQS.PayloadCols
	if payloadCols <= 0 {
		payloadCols = 2
	}

	keyTypes := make([]schema.ColumnType, 0, dimTables)
	for i := 0; i < dimTables; i++ {
		keyTypes = append(keyTypes, pickKeyType(r))
	}

	state := &schema.State{}
	createSQL := make([]string, 0, dimTables+1)
	insertSQL := make([]string, 0)
	truth := groundtruth.NewSchemaTruth(rows)

	base, payloadTypes := buildBaseTable(r, dimTables, keyTypes, payloadCols)
	state.Tables = append(state.Tables, base)
	createSQL = append(createSQL, createTableSQL(base))
	truth.AddTable(base.Name)

	dims := make([]schema.Table, 0, dimTables)
	dimDepTypes := make([][]schema.ColumnType, 0, dimTables)
	for i := 0; i < dimTables; i++ {
		dim, depTypes := buildDimTable(r, i, keyTypes[i], depCols, keyTypes[0])
		dims = append(dims, dim)
		dimDepTypes = append(dimDepTypes, depTypes)
		state.Tables = append(state.Tables, dim)
		createSQL = append(createSQL, createTableSQL(dim))
		truth.AddTable(dim.Name)
	}

	domains := make([][]typedValue, dimTables)
	depMaps := make([]map[string]map[string]typedValue, dimTables)
	for i := 0; i < dimTables; i++ {
		domainSize := min(10, max(2, rows/3))
		values := make([]typedValue, 0, domainSize)
		seen := map[string]struct{}{}
		for len(values) < domainSize {
			val := randomValue(r, keyTypes[i])
			tv := typedValue{Type: keyTypes[i], Value: val}
			key := tv.key()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			values = append(values, tv)
		}
		domains[i] = values
		depMaps[i] = make(map[string]map[string]typedValue)
		for _, tv := range values {
			deps := make(map[string]typedValue)
			for d := 0; d < depCols; d++ {
				colType := dimDepTypes[i][d]
				deps[fmt.Sprintf("d%d", d)] = typedValue{Type: colType, Value: randomValue(r, colType)}
			}
			if i > 0 {
				shared := domains[0][r.Intn(len(domains[0]))]
				deps["k0"] = shared
			}
			depMaps[i][tv.key()] = deps
		}
	}

	baseRows := make([]map[string]typedValue, 0, rows)
	dimRows := make([]map[string]map[string]typedValue, dimTables)
	for i := range dimRows {
		dimRows[i] = make(map[string]map[string]typedValue)
	}
	for i := 0; i < rows; i++ {
		row := make(map[string]typedValue)
		row["id"] = typedValue{Type: schema.TypeBigInt, Value: int64(i + 1)}
		for d := 0; d < dimTables; d++ {
			key := domains[d][r.Intn(len(domains[d]))]
			row[fmt.Sprintf("k%d", d)] = key
			deps := depMaps[d][key.key()]
			if _, ok := dimRows[d][key.key()]; !ok {
				dimRow := make(map[string]typedValue)
				dimRow["id"] = typedValue{Type: schema.TypeBigInt, Value: int64(len(dimRows[d]) + 1)}
				dimRow[fmt.Sprintf("k%d", d)] = key
				for name, v := range deps {
					dimRow[name] = v
				}
				truth.AddRowData(dims[d].Name, toTruthRow(dimRow))
				dimRows[d][key.key()] = dimRow
			}
			addTruthRow(&truth, dims[d].Name, deps, groundtruth.RowID(i))
			addTruthValue(&truth, dims[d].Name, fmt.Sprintf("k%d", d), key, groundtruth.RowID(i))
		}
		for p := 0; p < payloadCols; p++ {
			colType := payloadTypes[p]
			row[fmt.Sprintf("p%d", p)] = typedValue{Type: colType, Value: randomValue(r, colType)}
		}
		baseRows = append(baseRows, row)
		truth.AddRowData(base.Name, toTruthRow(row))
		addTruthRow(&truth, base.Name, row, groundtruth.RowID(i))
	}

	insertSQL = append(insertSQL, buildInsertStatements(base, baseRows, 50)...)
	for i := 0; i < dimTables; i++ {
		rows := make([]map[string]typedValue, 0, len(dimRows[i]))
		for _, row := range dimRows[i] {
			rows = append(rows, row)
		}
		insertSQL = append(insertSQL, buildInsertStatements(dims[i], rows, 50)...)
	}

	return BuildResult{
		State:     state,
		CreateSQL: createSQL,
		InsertSQL: insertSQL,
		Truth:     &truth,
	}, nil
}

type typedValue struct {
	Type  schema.ColumnType
	Value any
}

func (t typedValue) key() string {
	tv, ok := groundtruth.EncodeValue(t.Type, t.Value)
	if !ok {
		return fmt.Sprintf("%v", t.Value)
	}
	return groundtruth.NewKey(tv.Type, tv.Value)
}

func buildBaseTable(r *rand.Rand, dimTables int, keyTypes []schema.ColumnType, payloadCols int) (schema.Table, []schema.ColumnType) {
	cols := []schema.Column{{Name: "id", Type: schema.TypeBigInt, Nullable: false}}
	for i := 0; i < dimTables; i++ {
		cols = append(cols, schema.Column{Name: fmt.Sprintf("k%d", i), Type: keyTypes[i], Nullable: false})
	}
	payloadTypes := make([]schema.ColumnType, 0, payloadCols)
	for i := 0; i < payloadCols; i++ {
		colType := randomValueType(r)
		payloadTypes = append(payloadTypes, colType)
		cols = append(cols, schema.Column{Name: fmt.Sprintf("p%d", i), Type: colType, Nullable: false})
	}
	return schema.Table{
		Name:    "t0",
		Columns: cols,
		HasPK:   true,
		NextID:  1,
	}, payloadTypes
}

func buildDimTable(r *rand.Rand, idx int, keyType schema.ColumnType, depCols int, sharedKeyType schema.ColumnType) (schema.Table, []schema.ColumnType) {
	cols := []schema.Column{
		{Name: "id", Type: schema.TypeBigInt, Nullable: false},
		{Name: fmt.Sprintf("k%d", idx), Type: keyType, Nullable: false},
	}
	if idx > 0 {
		cols = append(cols, schema.Column{Name: "k0", Type: sharedKeyType, Nullable: false})
	}
	depTypes := make([]schema.ColumnType, 0, depCols)
	for i := 0; i < depCols; i++ {
		colType := randomValueType(r)
		depTypes = append(depTypes, colType)
		cols = append(cols, schema.Column{Name: fmt.Sprintf("d%d", i), Type: colType, Nullable: false})
	}
	return schema.Table{
		Name:    fmt.Sprintf("t%d", idx+1),
		Columns: cols,
		HasPK:   true,
		NextID:  1,
	}, depTypes
}

func addTruthRow(truth *groundtruth.SchemaTruth, table string, row map[string]typedValue, id groundtruth.RowID) {
	for name, tv := range row {
		addTruthValue(truth, table, name, tv, id)
	}
}

func addTruthValue(truth *groundtruth.SchemaTruth, table, col string, tv typedValue, id groundtruth.RowID) {
	encoded, ok := groundtruth.EncodeValue(tv.Type, tv.Value)
	if !ok {
		return
	}
	truth.AddColumnValue(table, col, encoded.Type, encoded.Value, id)
}

func toTruthRow(row map[string]typedValue) map[string]groundtruth.TypedValue {
	if len(row) == 0 {
		return nil
	}
	out := make(map[string]groundtruth.TypedValue, len(row))
	for name, tv := range row {
		encoded, ok := groundtruth.EncodeValue(tv.Type, tv.Value)
		if !ok {
			continue
		}
		out[name] = encoded
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildInsertStatements(tbl schema.Table, rows []map[string]typedValue, chunk int) []string {
	if len(rows) == 0 {
		return nil
	}
	if chunk <= 0 {
		chunk = 50
	}
	cols := make([]string, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		cols = append(cols, col.Name)
	}
	stmts := make([]string, 0)
	for i := 0; i < len(rows); i += chunk {
		end := i + chunk
		if end > len(rows) {
			end = len(rows)
		}
		values := make([]string, 0, end-i)
		for _, row := range rows[i:end] {
			vals := make([]string, 0, len(cols))
			for _, name := range cols {
				tv, ok := row[name]
				if !ok {
					vals = append(vals, "NULL")
					continue
				}
				vals = append(vals, sqlLiteral(tv.Type, tv.Value))
			}
			values = append(values, fmt.Sprintf("(%s)", joinCSV(vals)))
		}
		stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", quoteIdent(tbl.Name), joinCSV(quoteIdents(cols)), joinCSV(values))
		stmts = append(stmts, stmt)
	}
	return stmts
}

func createTableSQL(tbl schema.Table) string {
	parts := make([]string, 0, len(tbl.Columns)+1)
	for _, col := range tbl.Columns {
		line := fmt.Sprintf("%s %s", quoteIdent(col.Name), col.SQLType())
		if !col.Nullable {
			line += " NOT NULL"
		}
		parts = append(parts, line)
	}
	if tbl.HasPK {
		parts = append(parts, "PRIMARY KEY (`id`)")
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", quoteIdent(tbl.Name), joinCSV(parts))
}

func quoteIdent(name string) string {
	if name == "" {
		return "``"
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func quoteIdents(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, quoteIdent(name))
	}
	return out
}

func sqlLiteral(t schema.ColumnType, v any) string {
	if v == nil {
		return "NULL"
	}
	switch t {
	case schema.TypeInt, schema.TypeBigInt:
		return fmt.Sprintf("%v", v)
	case schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
		return fmt.Sprintf("%v", v)
	case schema.TypeBool:
		if b, ok := v.(bool); ok && b {
			return "1"
		}
		return "0"
	case schema.TypeDate:
		if tm, ok := v.(time.Time); ok {
			return fmt.Sprintf("'%s'", tm.Format("2006-01-02"))
		}
	case schema.TypeDatetime, schema.TypeTimestamp:
		if tm, ok := v.(time.Time); ok {
			return fmt.Sprintf("'%s'", tm.Format("2006-01-02 15:04:05"))
		}
	}
	return fmt.Sprintf("'%s'", escapeString(fmt.Sprintf("%v", v)))
}

func escapeString(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '\'' {
			out = append(out, '\'', '\'')
			continue
		}
		out = append(out, r)
	}
	return string(out)
}

func randomValueType(r *rand.Rand) schema.ColumnType {
	types := []schema.ColumnType{
		schema.TypeInt,
		schema.TypeBigInt,
		schema.TypeFloat,
		schema.TypeDouble,
		schema.TypeDecimal,
		schema.TypeVarchar,
		schema.TypeDate,
		schema.TypeDatetime,
		schema.TypeBool,
	}
	return types[r.Intn(len(types))]
}

func randIntRange(r *rand.Rand, min int, max int) int {
	if max <= min {
		return min
	}
	return min + r.Intn(max-min+1)
}

func isLeapYear(year int) bool {
	if year%400 == 0 {
		return true
	}
	if year%100 == 0 {
		return false
	}
	return year%4 == 0
}

func daysInMonth(year int, month int) int {
	switch month {
	case 2:
		if isLeapYear(year) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	default:
		return 31
	}
}

func randomDateParts(r *rand.Rand) (year int, month int, day int) {
	year = randIntRange(r, 2023, 2026)
	month = randIntRange(r, 1, 12)
	day = randIntRange(r, 1, daysInMonth(year, month))
	return year, month, day
}

func pickKeyType(r *rand.Rand) schema.ColumnType {
	types := []schema.ColumnType{
		schema.TypeInt,
		schema.TypeBigInt,
		schema.TypeVarchar,
		schema.TypeDate,
	}
	return types[r.Intn(len(types))]
}

func randomValue(r *rand.Rand, t schema.ColumnType) any {
	switch t {
	case schema.TypeInt:
		return int(r.Intn(200) - 100)
	case schema.TypeBigInt:
		return int64(r.Intn(1000))
	case schema.TypeFloat, schema.TypeDouble, schema.TypeDecimal:
		return float64(r.Intn(10000)) / 100.0
	case schema.TypeVarchar:
		return fmt.Sprintf("s%d", r.Intn(1000))
	case schema.TypeDate:
		year, month, day := randomDateParts(r)
		return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	case schema.TypeDatetime, schema.TypeTimestamp:
		year, month, day := randomDateParts(r)
		hour := randIntRange(r, 0, 23)
		minute := randIntRange(r, 0, 59)
		second := randIntRange(r, 0, 59)
		return time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	case schema.TypeBool:
		return r.Intn(2) == 0
	default:
		return int(r.Intn(100))
	}
}

func joinCSV(items []string) string {
	if len(items) == 0 {
		return ""
	}
	out := items[0]
	for i := 1; i < len(items); i++ {
		out += ", " + items[i]
	}
	return out
}
