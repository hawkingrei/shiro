package oracle

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
	"shiro/internal/util"

	"github.com/go-sql-driver/mysql"
)

type predicatePolicy struct {
	allowOr       bool
	allowNot      bool
	allowIsNull   bool
	allowSubquery bool
}

func predicatePolicyFor(gen *generator.Generator) predicatePolicy {
	level := strings.ToLower(strings.TrimSpace(gen.Config.Oracles.PredicateLevel))
	if level == "" {
		if gen.Config.Oracles.StrictPredicates {
			level = "strict"
		} else {
			level = "moderate"
		}
	}
	switch level {
	case "loose":
		return predicatePolicy{allowOr: true, allowNot: true, allowIsNull: true}
	case "moderate":
		return predicatePolicy{allowOr: true}
	default:
		return predicatePolicy{}
	}
}

func buildExpr(expr generator.Expr) string {
	b := generator.SQLBuilder{}
	expr.Build(&b)
	return b.String()
}

func buildFrom(query *generator.SelectQuery) string {
	from := buildTableFactor(query.From.BaseTable, query.From.BaseAlias, query.From.BaseQuery, false)
	for _, join := range query.From.Joins {
		if join.Natural {
			from += fmt.Sprintf(" NATURAL %s %s", join.Type, buildTableFactor(join.Table, join.TableAlias, join.TableQuery, join.Lateral))
			continue
		}
		from += fmt.Sprintf(" %s %s", join.Type, buildTableFactor(join.Table, join.TableAlias, join.TableQuery, join.Lateral))
		if len(join.Using) > 0 {
			from += " USING (" + strings.Join(join.Using, ", ") + ")"
		} else if join.On != nil {
			from += " ON " + buildExpr(join.On)
		}
	}
	return from
}

func buildTableFactor(tableName string, alias string, subquery *generator.SelectQuery, lateral bool) string {
	if subquery == nil {
		if alias == "" || alias == tableName {
			return tableName
		}
		return fmt.Sprintf("%s AS %s", tableName, alias)
	}
	if alias == "" {
		alias = tableName
	}
	if alias == "" {
		alias = "derived"
	}
	prefix := ""
	if lateral {
		prefix = "LATERAL "
	}
	return fmt.Sprintf("%s(%s) AS %s", prefix, subquery.SQLString(), alias)
}

func buildWith(query *generator.SelectQuery) string {
	if len(query.With) == 0 {
		return ""
	}
	parts := make([]string, 0, len(query.With))
	for _, cte := range query.With {
		parts = append(parts, fmt.Sprintf("%s AS (%s)", cte.Name, cte.Query.SQLString()))
	}
	prefix := "WITH "
	if query.WithRecursive {
		prefix = "WITH RECURSIVE "
	}
	return prefix + strings.Join(parts, ", ") + " "
}

func tablesForQuery(query *generator.SelectQuery, state *schema.State) []schema.Table {
	if state == nil {
		return nil
	}
	names := []string{query.From.BaseTable}
	for _, join := range query.From.Joins {
		names = append(names, join.Table)
	}
	tables := make([]schema.Table, 0, len(names))
	for _, name := range names {
		if tbl, ok := state.TableByName(name); ok {
			tables = append(tables, tbl)
		}
	}
	return tables
}

func queryHasAggregate(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	if query.Analysis != nil {
		return query.Analysis.HasAggregate
	}
	for _, cte := range query.With {
		if queryHasAggregate(cte.Query) {
			return true
		}
	}
	for _, op := range query.SetOps {
		if queryHasAggregate(op.Query) {
			return true
		}
	}
	if query.From.BaseQuery != nil && queryHasAggregate(query.From.BaseQuery) {
		return true
	}
	for _, item := range query.Items {
		if exprHasAggregate(item.Expr) {
			return true
		}
	}
	if query.Where != nil && exprHasAggregate(query.Where) {
		return true
	}
	if query.Having != nil && exprHasAggregate(query.Having) {
		return true
	}
	for _, expr := range query.GroupBy {
		if exprHasAggregate(expr) {
			return true
		}
	}
	for _, ob := range query.OrderBy {
		if exprHasAggregate(ob.Expr) {
			return true
		}
	}
	for _, join := range query.From.Joins {
		if join.TableQuery != nil && queryHasAggregate(join.TableQuery) {
			return true
		}
		if join.On != nil && exprHasAggregate(join.On) {
			return true
		}
	}
	return false
}

func queryHasSubquery(query *generator.SelectQuery) bool {
	if query == nil {
		return false
	}
	if query.Analysis != nil {
		return query.Analysis.HasSubquery
	}
	for _, cte := range query.With {
		if queryHasSubquery(cte.Query) {
			return true
		}
	}
	for _, op := range query.SetOps {
		if queryHasSubquery(op.Query) {
			return true
		}
	}
	if query.From.BaseQuery != nil {
		return true
	}
	for _, item := range query.Items {
		if exprHasSubquery(item.Expr) {
			return true
		}
	}
	if query.Where != nil && exprHasSubquery(query.Where) {
		return true
	}
	if query.Having != nil && exprHasSubquery(query.Having) {
		return true
	}
	for _, expr := range query.GroupBy {
		if exprHasSubquery(expr) {
			return true
		}
	}
	for _, ob := range query.OrderBy {
		if exprHasSubquery(ob.Expr) {
			return true
		}
	}
	for _, join := range query.From.Joins {
		if join.TableQuery != nil {
			return true
		}
		if join.On != nil && exprHasSubquery(join.On) {
			return true
		}
	}
	return false
}

type columnGuardScope struct {
	tables      map[string]schema.Table
	qualified   map[string]map[string]struct{}
	unqualified map[string]int
}

func newColumnGuardScope() *columnGuardScope {
	return &columnGuardScope{
		tables:      make(map[string]schema.Table),
		qualified:   make(map[string]map[string]struct{}),
		unqualified: make(map[string]int),
	}
}

func cloneColumnGuardScope(in *columnGuardScope) *columnGuardScope {
	if in == nil {
		return nil
	}
	out := newColumnGuardScope()
	for name, tbl := range in.tables {
		copied := tbl
		copied.Columns = append([]schema.Column{}, tbl.Columns...)
		out.tables[name] = copied
	}
	for name, cols := range in.qualified {
		colSet := make(map[string]struct{}, len(cols))
		for col := range cols {
			colSet[col] = struct{}{}
		}
		out.qualified[name] = colSet
	}
	for name, count := range in.unqualified {
		out.unqualified[name] = count
	}
	return out
}

func mergeColumnGuardScopes(left *columnGuardScope, right *columnGuardScope) *columnGuardScope {
	if left == nil {
		return cloneColumnGuardScope(right)
	}
	out := cloneColumnGuardScope(left)
	if right == nil {
		return out
	}
	for name, tbl := range right.tables {
		if _, ok := out.tables[name]; ok {
			continue
		}
		copied := tbl
		copied.Columns = append([]schema.Column{}, tbl.Columns...)
		out.tables[name] = copied
	}
	for name, cols := range right.qualified {
		if _, ok := out.qualified[name]; ok {
			continue
		}
		colSet := make(map[string]struct{}, len(cols))
		for col := range cols {
			colSet[col] = struct{}{}
		}
		out.qualified[name] = colSet
	}
	for name, count := range right.unqualified {
		out.unqualified[name] += count
	}
	return out
}

func (s *columnGuardScope) addTable(tbl schema.Table) {
	if s == nil || strings.TrimSpace(tbl.Name) == "" {
		return
	}
	copied := tbl
	copied.Columns = append([]schema.Column{}, tbl.Columns...)
	s.tables[copied.Name] = copied
	colSet := make(map[string]struct{}, len(copied.Columns))
	for _, col := range copied.Columns {
		colSet[col.Name] = struct{}{}
		s.unqualified[col.Name]++
	}
	s.qualified[copied.Name] = colSet
}

func (s *columnGuardScope) addJoinTable(tbl schema.Table, merged map[string]struct{}) {
	if s == nil || strings.TrimSpace(tbl.Name) == "" {
		return
	}
	copied := tbl
	copied.Columns = append([]schema.Column{}, tbl.Columns...)
	s.tables[copied.Name] = copied
	colSet := make(map[string]struct{}, len(copied.Columns))
	for _, col := range copied.Columns {
		if _, ok := merged[col.Name]; ok {
			s.unqualified[col.Name] = 1
			continue
		}
		colSet[col.Name] = struct{}{}
		s.unqualified[col.Name]++
	}
	s.qualified[copied.Name] = colSet
}

func (s *columnGuardScope) hideQualifiedColumns(tableNames []string, columns []string) {
	if s == nil {
		return
	}
	for _, tableName := range tableNames {
		colSet, ok := s.qualified[tableName]
		if !ok {
			continue
		}
		for _, col := range columns {
			delete(colSet, col)
		}
		s.qualified[tableName] = colSet
	}
}

func (s *columnGuardScope) hasTable(name string) bool {
	if s == nil || name == "" {
		return false
	}
	_, ok := s.tables[name]
	return ok
}

func (s *columnGuardScope) hasQualifiedColumn(tableName string, columnName string) bool {
	if s == nil || tableName == "" || columnName == "" {
		return false
	}
	if _, ok := s.tables[tableName]; !ok {
		return false
	}
	if cols, ok := s.qualified[tableName]; ok {
		_, ok = cols[columnName]
		return ok
	}
	return false
}

func (s *columnGuardScope) unqualifiedCount(name string) int {
	if s == nil || name == "" {
		return 0
	}
	return s.unqualified[name]
}

func projectedColumnsFromSelectItems(items []generator.SelectItem) []schema.Column {
	if len(items) == 0 {
		return nil
	}
	used := map[string]int{}
	cols := make([]schema.Column, 0, len(items))
	for i, item := range items {
		name := strings.TrimSpace(item.Alias)
		if name == "" {
			if col, ok := item.Expr.(generator.ColumnExpr); ok {
				name = col.Ref.Name
			} else {
				name = fmt.Sprintf("c%d", i)
			}
		}
		base := name
		if count, ok := used[base]; ok {
			count++
			used[base] = count
			name = fmt.Sprintf("%s_%d", base, count)
		} else {
			used[base] = 0
		}
		cols = append(cols, schema.Column{Name: name, Type: schema.TypeVarchar})
	}
	return cols
}

func resolveQuerySourceTable(sourceName string, visibleName string, subquery *generator.SelectQuery, state *schema.State) (schema.Table, bool, string) {
	name := strings.TrimSpace(visibleName)
	if name == "" {
		return schema.Table{}, false, "empty_table"
	}
	if subquery != nil {
		return schema.Table{Name: name, Columns: projectedColumnsFromSelectItems(subquery.Items)}, true, ""
	}
	if state == nil {
		return schema.Table{}, false, "nil_query_or_state"
	}
	tbl, ok := state.TableByName(sourceName)
	if !ok {
		return schema.Table{}, false, "unknown_table"
	}
	tbl.Name = name
	tbl.Columns = append([]schema.Column{}, tbl.Columns...)
	return tbl, true, ""
}

func visibleBaseName(from generator.FromClause) string {
	if strings.TrimSpace(from.BaseAlias) != "" {
		return from.BaseAlias
	}
	return from.BaseTable
}

func visibleJoinName(join generator.Join) string {
	if strings.TrimSpace(join.TableAlias) != "" {
		return join.TableAlias
	}
	return join.Table
}

func naturalJoinMergedColumns(scope *columnGuardScope, leftTables []string, right schema.Table) []string {
	if scope == nil || right.Name == "" || len(leftTables) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	merged := make([]string, 0)
	for _, col := range right.Columns {
		if scope.unqualifiedCount(col.Name) == 0 {
			continue
		}
		if _, ok := seen[col.Name]; ok {
			continue
		}
		seen[col.Name] = struct{}{}
		merged = append(merged, col.Name)
	}
	return merged
}

func queryColumnsValidWithScope(query *generator.SelectQuery, state *schema.State, outer *columnGuardScope) (bool, string) {
	if query == nil || state == nil {
		return false, "nil_query_or_state"
	}
	if len(query.With) > 0 {
		return false, "with_clause"
	}
	for _, op := range query.SetOps {
		if ok, reason := queryColumnsValidWithScope(op.Query, state, outer); !ok {
			return false, reason
		}
	}
	if query.From.BaseQuery != nil {
		if ok, reason := queryColumnsValidWithScope(query.From.BaseQuery, state, nil); !ok {
			return false, reason
		}
	}
	baseTbl, ok, reason := resolveQuerySourceTable(query.From.BaseTable, visibleBaseName(query.From), query.From.BaseQuery, state)
	if !ok {
		return false, reason
	}
	scope := newColumnGuardScope()
	scope.addTable(baseTbl)
	leftTables := []string{baseTbl.Name}

	checkColumn := func(ref generator.ColumnRef, local *columnGuardScope) (bool, string) {
		if ref.Name == "" {
			return false, "empty_column"
		}
		if ref.Table == "" {
			if local != nil {
				if count := local.unqualifiedCount(ref.Name); count > 0 {
					if count == 1 {
						return true, ""
					}
					return false, "unknown_column"
				}
			}
			if outer != nil && outer.unqualifiedCount(ref.Name) == 1 {
				return true, ""
			}
			return false, "unknown_column"
		}
		if local != nil && local.hasTable(ref.Table) {
			if local.hasQualifiedColumn(ref.Table, ref.Name) {
				return true, ""
			}
			return false, "unknown_column"
		}
		if outer != nil && outer.hasTable(ref.Table) {
			if outer.hasQualifiedColumn(ref.Table, ref.Name) {
				return true, ""
			}
			return false, "unknown_column"
		}
		return false, "unknown_table"
	}

	var checkExpr func(expr generator.Expr, local *columnGuardScope) (bool, string)
	checkExpr = func(expr generator.Expr, local *columnGuardScope) (bool, string) {
		if expr == nil {
			return true, ""
		}
		switch e := expr.(type) {
		case generator.ColumnExpr:
			return checkColumn(e.Ref, local)
		case generator.LiteralExpr, generator.ParamExpr:
			return true, ""
		case generator.UnaryExpr:
			return checkExpr(e.Expr, local)
		case generator.BinaryExpr:
			if ok, reason := checkExpr(e.Left, local); !ok {
				return false, reason
			}
			return checkExpr(e.Right, local)
		case generator.FuncExpr:
			for _, arg := range e.Args {
				if ok, reason := checkExpr(arg, local); !ok {
					return false, reason
				}
			}
			return true, ""
		case generator.GroupByOrdinalExpr:
			if e.Expr == nil {
				return true, ""
			}
			return checkExpr(e.Expr, local)
		case generator.CaseExpr:
			for _, w := range e.Whens {
				if ok, reason := checkExpr(w.When, local); !ok {
					return false, reason
				}
				if ok, reason := checkExpr(w.Then, local); !ok {
					return false, reason
				}
			}
			return checkExpr(e.Else, local)
		case generator.InExpr:
			if ok, reason := checkExpr(e.Left, local); !ok {
				return false, reason
			}
			for _, item := range e.List {
				if ok, reason := checkExpr(item, local); !ok {
					return false, reason
				}
			}
			return true, ""
		case generator.SubqueryExpr:
			return queryColumnsValidWithScope(e.Query, state, mergeColumnGuardScopes(local, outer))
		case generator.ExistsExpr:
			return queryColumnsValidWithScope(e.Query, state, mergeColumnGuardScopes(local, outer))
		case generator.WindowExpr:
			for _, arg := range e.Args {
				if ok, reason := checkExpr(arg, local); !ok {
					return false, reason
				}
			}
			for _, expr := range e.PartitionBy {
				if ok, reason := checkExpr(expr, local); !ok {
					return false, reason
				}
			}
			for _, ob := range e.OrderBy {
				if ok, reason := checkExpr(ob.Expr, local); !ok {
					return false, reason
				}
			}
			return true, ""
		default:
			for _, col := range expr.Columns() {
				if ok, reason := checkColumn(col, local); !ok {
					return false, reason
				}
			}
			return true, ""
		}
	}

	for _, join := range query.From.Joins {
		joinOuter := (*columnGuardScope)(nil)
		if join.Lateral {
			joinOuter = mergeColumnGuardScopes(scope, outer)
		}
		if join.TableQuery != nil {
			if ok, reason := queryColumnsValidWithScope(join.TableQuery, state, joinOuter); !ok {
				return false, reason
			}
		}
		rightTbl, ok, reason := resolveQuerySourceTable(join.Table, visibleJoinName(join), join.TableQuery, state)
		if !ok {
			return false, reason
		}
		joinScope := cloneColumnGuardScope(scope)
		joinScope.addTable(rightTbl)
		if len(join.Using) > 0 {
			for _, name := range join.Using {
				if _, ok := rightTbl.ColumnByName(name); !ok {
					return false, "unknown_using_column"
				}
				if scope.unqualifiedCount(name) == 0 {
					return false, "unknown_using_column"
				}
			}
		}
		if ok, reason := checkExpr(join.On, joinScope); !ok {
			return false, reason
		}
		mergedSet := make(map[string]struct{})
		if len(join.Using) > 0 {
			for _, name := range join.Using {
				if _, ok := rightTbl.ColumnByName(name); ok && scope.unqualifiedCount(name) > 0 {
					mergedSet[name] = struct{}{}
				}
			}
		} else if join.Natural {
			for _, name := range naturalJoinMergedColumns(scope, leftTables, rightTbl) {
				mergedSet[name] = struct{}{}
			}
		}
		mergedNames := make([]string, 0, len(mergedSet))
		for name := range mergedSet {
			mergedNames = append(mergedNames, name)
		}
		scope.addJoinTable(rightTbl, mergedSet)
		if len(mergedNames) > 0 {
			hideTables := append(append([]string{}, leftTables...), rightTbl.Name)
			scope.hideQualifiedColumns(hideTables, mergedNames)
		}
		leftTables = append(leftTables, rightTbl.Name)
	}

	for _, item := range query.Items {
		if ok, reason := checkExpr(item.Expr, scope); !ok {
			return false, reason
		}
	}
	if ok, reason := checkExpr(query.Where, scope); !ok {
		return false, reason
	}
	if ok, reason := checkExpr(query.Having, scope); !ok {
		return false, reason
	}
	for _, expr := range query.GroupBy {
		if ok, reason := checkExpr(expr, scope); !ok {
			return false, reason
		}
	}
	for _, ob := range query.OrderBy {
		if ok, reason := checkExpr(ob.Expr, scope); !ok {
			return false, reason
		}
	}
	return true, ""
}

func queryColumnsValid(query *generator.SelectQuery, state *schema.State, outerTables map[string]schema.Table) (bool, string) {
	outer := newColumnGuardScope()
	for name, tbl := range outerTables {
		copied := tbl
		copied.Name = name
		outer.addTable(copied)
	}
	return queryColumnsValidWithScope(query, state, outer)
}

func mysqlErrCode(err error) (uint16, bool) {
	if err == nil {
		return 0, false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number, true
	}
	return 0, false
}

func isWhitelistedSQLError(err error) (uint16, bool) {
	code, ok := mysqlErrCode(err)
	if !ok {
		return 0, false
	}
	switch code {
	case 1064, 1292, 1451, 1452:
		return code, true
	default:
		return code, false
	}
}

func sanitizeQueryColumns(query *generator.SelectQuery, state *schema.State) bool {
	if query == nil || state == nil {
		return false
	}
	return sanitizeQueryColumnsWithOuter(query, state, nil)
}

func sanitizeQueryColumnsWithOuter(query *generator.SelectQuery, state *schema.State, outerTables map[string]schema.Table) bool {
	if query == nil || state == nil {
		return false
	}
	if len(query.With) > 0 {
		return false
	}
	changed := false
	for i := range query.SetOps {
		if sanitizeQueryColumnsWithOuter(query.SetOps[i].Query, state, outerTables) {
			changed = true
		}
	}
	tableMap := map[string]schema.Table{}
	for name, tbl := range outerTables {
		tableMap[name] = tbl
	}
	if query.From.BaseTable == "" || !state.HasTables() {
		return false
	}
	base, ok := state.TableByName(query.From.BaseTable)
	if !ok {
		query.From.BaseTable = state.Tables[0].Name
		base = state.Tables[0]
		changed = true
	}
	tableMap[query.From.BaseTable] = base
	validJoins := make([]generator.Join, 0, len(query.From.Joins))
	for _, join := range query.From.Joins {
		if join.Table == "" {
			changed = true
			continue
		}
		tbl, ok := state.TableByName(join.Table)
		if !ok {
			changed = true
			continue
		}
		tableMap[join.Table] = tbl
		validJoins = append(validJoins, join)
	}
	if len(validJoins) != len(query.From.Joins) {
		query.From.Joins = validJoins
	}

	orderedTables := []schema.Table{base}
	for _, join := range query.From.Joins {
		tbl, ok := tableMap[join.Table]
		if !ok {
			continue
		}
		orderedTables = append(orderedTables, tbl)
	}

	pickFallbackRef := func(preferTable string) (generator.ColumnRef, bool) {
		if preferTable != "" {
			if tbl, ok := tableMap[preferTable]; ok && len(tbl.Columns) > 0 {
				return generator.ColumnRef{Table: tbl.Name, Name: tbl.Columns[0].Name}, true
			}
		}
		for _, tbl := range orderedTables {
			if len(tbl.Columns) > 0 {
				return generator.ColumnRef{Table: tbl.Name, Name: tbl.Columns[0].Name}, true
			}
		}
		return generator.ColumnRef{}, false
	}

	pickUnqualifiedColumnRef := func(name string) (generator.ColumnRef, bool) {
		if name == "" {
			return generator.ColumnRef{}, false
		}
		for _, tbl := range orderedTables {
			if _, ok := tbl.ColumnByName(name); ok {
				return generator.ColumnRef{Table: tbl.Name, Name: name}, true
			}
		}
		if len(outerTables) == 1 {
			for _, tbl := range outerTables {
				if _, ok := tbl.ColumnByName(name); ok {
					return generator.ColumnRef{Table: tbl.Name, Name: name}, true
				}
			}
		}
		return generator.ColumnRef{}, false
	}

	pickSharedColumn := func(left []schema.Table, right schema.Table) (string, bool) {
		for _, col := range right.Columns {
			for _, ltbl := range left {
				if _, ok := ltbl.ColumnByName(col.Name); ok {
					return col.Name, true
				}
			}
		}
		return "", false
	}

	checkColumn := func(ref generator.ColumnRef) bool {
		if ref.Name == "" {
			return false
		}
		if ref.Table == "" {
			return unqualifiedColumnAvailable(ref.Name, query, tableMap, outerTables)
		}
		tbl, ok := tableMap[ref.Table]
		if !ok {
			return false
		}
		_, ok = tbl.ColumnByName(ref.Name)
		return ok
	}

	trueExpr := func() generator.Expr {
		return generator.BinaryExpr{
			Left:  generator.LiteralExpr{Value: 1},
			Op:    "=",
			Right: generator.LiteralExpr{Value: 1},
		}
	}

	var sanitizeExpr func(expr generator.Expr) (generator.Expr, bool)
	sanitizeExpr = func(expr generator.Expr) (generator.Expr, bool) {
		if expr == nil {
			return nil, false
		}
		switch e := expr.(type) {
		case generator.ColumnExpr:
			if !checkColumn(e.Ref) {
				if e.Ref.Table == "" {
					if ref, ok := pickUnqualifiedColumnRef(e.Ref.Name); ok {
						e.Ref = ref
						return e, true
					}
				}
				if ref, ok := pickFallbackRef(e.Ref.Table); ok {
					e.Ref = ref
					return e, true
				}
				return nil, true
			}
			return e, false
		case generator.LiteralExpr, generator.ParamExpr:
			return expr, false
		case generator.UnaryExpr:
			inner, changed := sanitizeExpr(e.Expr)
			if inner == nil {
				return nil, true
			}
			e.Expr = inner
			return e, changed
		case generator.BinaryExpr:
			left, lchg := sanitizeExpr(e.Left)
			right, rchg := sanitizeExpr(e.Right)
			if left == nil && right == nil {
				return nil, true
			}
			if left == nil {
				return right, true
			}
			if right == nil {
				return left, true
			}
			e.Left = left
			e.Right = right
			return e, lchg || rchg
		case generator.FuncExpr:
			args := make([]generator.Expr, 0, len(e.Args))
			argChanged := false
			for _, arg := range e.Args {
				out, changed := sanitizeExpr(arg)
				if out == nil {
					argChanged = true
					continue
				}
				argChanged = argChanged || changed
				args = append(args, out)
			}
			if len(args) == 0 {
				return nil, true
			}
			e.Args = args
			return e, argChanged
		case generator.CaseExpr:
			whens := make([]generator.CaseWhen, 0, len(e.Whens))
			caseChanged := false
			for _, w := range e.Whens {
				whenExpr, whenChanged := sanitizeExpr(w.When)
				thenExpr, thenChanged := sanitizeExpr(w.Then)
				if whenExpr == nil || thenExpr == nil {
					caseChanged = true
					continue
				}
				caseChanged = caseChanged || whenChanged || thenChanged
				whens = append(whens, generator.CaseWhen{When: whenExpr, Then: thenExpr})
			}
			var elseExpr generator.Expr
			if e.Else != nil {
				out, changed := sanitizeExpr(e.Else)
				elseExpr = out
				caseChanged = caseChanged || changed
			}
			if len(whens) == 0 && elseExpr == nil {
				return nil, true
			}
			e.Whens = whens
			e.Else = elseExpr
			return e, caseChanged
		case generator.InExpr:
			left, lchg := sanitizeExpr(e.Left)
			if left == nil {
				return nil, true
			}
			list := make([]generator.Expr, 0, len(e.List))
			listChanged := false
			for _, item := range e.List {
				out, changed := sanitizeExpr(item)
				if out == nil {
					listChanged = true
					continue
				}
				listChanged = listChanged || changed
				list = append(list, out)
			}
			if len(list) == 0 {
				return nil, true
			}
			e.Left = left
			e.List = list
			return e, lchg || listChanged
		case generator.SubqueryExpr:
			if sanitizeQueryColumnsWithOuter(e.Query, state, tableMap) {
				changed = true
			}
			if ok, _ := queryColumnsValid(e.Query, state, tableMap); !ok {
				return nil, true
			}
			return e, true
		case generator.ExistsExpr:
			if sanitizeQueryColumnsWithOuter(e.Query, state, tableMap) {
				changed = true
			}
			if ok, _ := queryColumnsValid(e.Query, state, tableMap); !ok {
				return nil, true
			}
			return e, true
		case generator.WindowExpr:
			args := make([]generator.Expr, 0, len(e.Args))
			wChanged := false
			for _, arg := range e.Args {
				out, changed := sanitizeExpr(arg)
				if out == nil {
					wChanged = true
					continue
				}
				wChanged = wChanged || changed
				args = append(args, out)
			}
			part := make([]generator.Expr, 0, len(e.PartitionBy))
			for _, expr := range e.PartitionBy {
				out, changed := sanitizeExpr(expr)
				if out == nil {
					wChanged = true
					continue
				}
				wChanged = wChanged || changed
				part = append(part, out)
			}
			orders := make([]generator.OrderBy, 0, len(e.OrderBy))
			for _, ob := range e.OrderBy {
				out, changed := sanitizeExpr(ob.Expr)
				if out == nil {
					wChanged = true
					continue
				}
				wChanged = wChanged || changed
				orders = append(orders, generator.OrderBy{Expr: out, Desc: ob.Desc})
			}
			if len(args) == 0 {
				return nil, true
			}
			e.Args = args
			e.PartitionBy = part
			e.OrderBy = orders
			return e, wChanged
		default:
			for _, col := range expr.Columns() {
				if !checkColumn(col) {
					return nil, true
				}
			}
			return expr, false
		}
	}

	leftTables := []schema.Table{base}
	for i, join := range query.From.Joins {
		right, ok := tableMap[join.Table]
		if !ok {
			continue
		}
		if len(join.Using) > 0 {
			valid := make([]string, 0, len(join.Using))
			for _, name := range join.Using {
				if _, ok := right.ColumnByName(name); !ok {
					continue
				}
				found := false
				for _, ltbl := range leftTables {
					if _, ok := ltbl.ColumnByName(name); ok {
						found = true
						break
					}
				}
				if found {
					valid = append(valid, name)
				}
			}
			if len(valid) != len(join.Using) {
				changed = true
				join.Using = valid
			}
			if len(join.Using) == 0 && join.Type != generator.JoinCross {
				if name, ok := pickSharedColumn(leftTables, right); ok {
					join.Using = []string{name}
				} else {
					join.On = trueExpr()
				}
				changed = true
			}
		}
		if join.On != nil {
			onExpr, onChanged := sanitizeExpr(join.On)
			if onExpr == nil {
				if join.Type != generator.JoinCross {
					if name, ok := pickSharedColumn(leftTables, right); ok {
						left := leftTables[len(leftTables)-1]
						join.On = generator.BinaryExpr{
							Left:  generator.ColumnExpr{Ref: generator.ColumnRef{Table: left.Name, Name: name}},
							Op:    "=",
							Right: generator.ColumnExpr{Ref: generator.ColumnRef{Table: right.Name, Name: name}},
						}
					} else {
						join.On = trueExpr()
					}
				}
				changed = true
			} else {
				join.On = onExpr
				changed = changed || onChanged
			}
		}
		query.From.Joins[i] = join
		leftTables = append(leftTables, right)
	}

	items := make([]generator.SelectItem, 0, len(query.Items))
	for _, item := range query.Items {
		out, itemChanged := sanitizeExpr(item.Expr)
		if out == nil {
			item.Expr = generator.LiteralExpr{Value: 1}
			changed = true
		} else {
			item.Expr = out
			changed = changed || itemChanged
		}
		items = append(items, item)
	}
	query.Items = items
	if len(query.Items) == 0 {
		query.Items = []generator.SelectItem{{Expr: generator.LiteralExpr{Value: 1}, Alias: "c0"}}
		changed = true
	}

	if query.Where != nil {
		whereExpr, whereChanged := sanitizeExpr(query.Where)
		if whereExpr == nil {
			query.Where = nil
			changed = true
		} else {
			query.Where = whereExpr
			changed = changed || whereChanged
		}
	}
	if query.Having != nil {
		havingExpr, havingChanged := sanitizeExpr(query.Having)
		if havingExpr == nil {
			query.Having = nil
			changed = true
		} else {
			query.Having = havingExpr
			changed = changed || havingChanged
		}
	}
	groupBy := make([]generator.Expr, 0, len(query.GroupBy))
	for _, expr := range query.GroupBy {
		out, exprChanged := sanitizeExpr(expr)
		if out == nil {
			changed = true
			continue
		}
		changed = changed || exprChanged
		groupBy = append(groupBy, out)
	}
	query.GroupBy = groupBy
	orderBy := make([]generator.OrderBy, 0, len(query.OrderBy))
	for _, ob := range query.OrderBy {
		out, obChanged := sanitizeExpr(ob.Expr)
		if out == nil {
			changed = true
			continue
		}
		changed = changed || obChanged
		orderBy = append(orderBy, generator.OrderBy{Expr: out, Desc: ob.Desc})
	}
	query.OrderBy = orderBy
	return changed
}

func explainSQL(ctx context.Context, exec *db.DB, query string) (string, error) {
	rows, err := exec.QueryContext(ctx, "EXPLAIN "+query)
	if err != nil {
		return "", err
	}
	defer util.CloseWithErr(rows, "oracle explain rows")

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	values := make([][]byte, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var b strings.Builder
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return "", err
		}
		for i, v := range values {
			if i > 0 {
				b.WriteByte('\t')
			}
			if v == nil {
				b.WriteString("NULL")
			} else {
				b.Write(v)
			}
		}
		b.WriteByte('\n')
	}
	return b.String(), nil
}

func unqualifiedColumnAvailable(name string, query *generator.SelectQuery, tableMap map[string]schema.Table, outerTables map[string]schema.Table) bool {
	localCounts := unqualifiedColumnCounts(query, tableMap)
	if count := localCounts[name]; count > 0 {
		return count == 1
	}
	return rawUnqualifiedColumnCount(name, outerTables) == 1
}

func unqualifiedColumnCounts(query *generator.SelectQuery, tableMap map[string]schema.Table) map[string]int {
	counts := make(map[string]int)
	if query == nil {
		return counts
	}
	base, ok := tableMap[query.From.BaseTable]
	if !ok {
		return counts
	}
	for _, col := range base.Columns {
		counts[col.Name] = 1
	}
	for _, join := range query.From.Joins {
		right, ok := tableMap[join.Table]
		if !ok {
			continue
		}
		merged := make(map[string]struct{})
		if len(join.Using) > 0 {
			for _, name := range join.Using {
				if name == "" || counts[name] == 0 {
					continue
				}
				if _, ok := right.ColumnByName(name); ok {
					merged[name] = struct{}{}
				}
			}
		} else if join.Natural {
			for _, col := range right.Columns {
				if counts[col.Name] > 0 {
					merged[col.Name] = struct{}{}
				}
			}
		}
		for _, col := range right.Columns {
			if _, ok := merged[col.Name]; ok {
				counts[col.Name] = 1
				continue
			}
			counts[col.Name]++
		}
	}
	return counts
}

func rawUnqualifiedColumnCount(name string, tables map[string]schema.Table) int {
	count := 0
	for _, tbl := range tables {
		if _, ok := tbl.ColumnByName(name); ok {
			count++
		}
	}
	return count
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func queryDeterministic(query *generator.SelectQuery) bool {
	if query == nil {
		return true
	}
	if query.Analysis != nil {
		return query.Analysis.Deterministic
	}
	for _, cte := range query.With {
		if !queryDeterministic(cte.Query) {
			return false
		}
	}
	for _, op := range query.SetOps {
		if !queryDeterministic(op.Query) {
			return false
		}
	}
	if query.From.BaseQuery != nil && !queryDeterministic(query.From.BaseQuery) {
		return false
	}
	for _, item := range query.Items {
		if !item.Expr.Deterministic() {
			return false
		}
	}
	if query.Where != nil && !query.Where.Deterministic() {
		return false
	}
	if query.Having != nil && !query.Having.Deterministic() {
		return false
	}
	for _, def := range query.WindowDefs {
		for _, expr := range def.PartitionBy {
			if !expr.Deterministic() {
				return false
			}
		}
		for _, ob := range def.OrderBy {
			if !ob.Expr.Deterministic() {
				return false
			}
		}
	}
	for _, expr := range query.GroupBy {
		if !expr.Deterministic() {
			return false
		}
	}
	for _, ob := range query.OrderBy {
		if !ob.Expr.Deterministic() {
			return false
		}
	}
	for _, join := range query.From.Joins {
		if join.TableQuery != nil && !queryDeterministic(join.TableQuery) {
			return false
		}
		if join.On != nil && !join.On.Deterministic() {
			return false
		}
	}
	return true
}

func exprHasAggregate(expr generator.Expr) bool {
	switch e := expr.(type) {
	case generator.FuncExpr:
		if isAggregateFunc(e.Name) {
			return true
		}
		for _, arg := range e.Args {
			if exprHasAggregate(arg) {
				return true
			}
		}
		return false
	case generator.UnaryExpr:
		return exprHasAggregate(e.Expr)
	case generator.BinaryExpr:
		return exprHasAggregate(e.Left) || exprHasAggregate(e.Right)
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if exprHasAggregate(w.When) || exprHasAggregate(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return exprHasAggregate(e.Else)
		}
		return false
	case generator.InExpr:
		if exprHasAggregate(e.Left) {
			return true
		}
		for _, item := range e.List {
			if exprHasAggregate(item) {
				return true
			}
		}
		return false
	case generator.GroupByOrdinalExpr:
		if e.Expr == nil {
			return false
		}
		return exprHasAggregate(e.Expr)
	case generator.CompareSubqueryExpr:
		if exprHasAggregate(e.Left) {
			return true
		}
		return queryHasAggregate(e.Query)
	case generator.SubqueryExpr:
		return queryHasAggregate(e.Query)
	case generator.ExistsExpr:
		return queryHasAggregate(e.Query)
	default:
		return false
	}
}

func exprHasSubquery(expr generator.Expr) bool {
	switch e := expr.(type) {
	case generator.SubqueryExpr:
		return true
	case generator.ExistsExpr:
		return true
	case generator.InExpr:
		for _, item := range e.List {
			if exprHasSubquery(item) {
				return true
			}
		}
		return exprHasSubquery(e.Left)
	case generator.CompareSubqueryExpr:
		return true
	case generator.UnaryExpr:
		return exprHasSubquery(e.Expr)
	case generator.BinaryExpr:
		return exprHasSubquery(e.Left) || exprHasSubquery(e.Right)
	case generator.CaseExpr:
		for _, w := range e.Whens {
			if exprHasSubquery(w.When) || exprHasSubquery(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return exprHasSubquery(e.Else)
		}
		return false
	case generator.FuncExpr:
		for _, arg := range e.Args {
			if exprHasSubquery(arg) {
				return true
			}
		}
		return false
	case generator.GroupByOrdinalExpr:
		if e.Expr == nil {
			return false
		}
		return exprHasSubquery(e.Expr)
	default:
		return false
	}
}

func isAggregateFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}

func isSimplePredicate(expr generator.Expr) bool {
	switch e := expr.(type) {
	case generator.BinaryExpr:
		if strings.EqualFold(e.Op, "AND") {
			return isSimplePredicate(e.Left) && isSimplePredicate(e.Right)
		}
		if !isComparisonOp(e.Op) {
			return false
		}
		return isSimpleOperand(e.Left) && isSimpleOperand(e.Right)
	default:
		return false
	}
}

func predicateMatches(expr generator.Expr, policy predicatePolicy) bool {
	switch e := expr.(type) {
	case generator.ExistsExpr:
		return policy.allowSubquery
	case generator.InExpr:
		hasSubquery := false
		for _, item := range e.List {
			if _, ok := item.(generator.SubqueryExpr); ok {
				hasSubquery = true
				continue
			}
			if !isSimpleOperand(item) {
				return false
			}
		}
		if hasSubquery && !policy.allowSubquery {
			return false
		}
		return isSimpleOperand(e.Left)
	case generator.BinaryExpr:
		op := strings.ToUpper(strings.TrimSpace(e.Op))
		switch op {
		case "AND":
			return predicateMatches(e.Left, policy) && predicateMatches(e.Right, policy)
		case "OR":
			if !policy.allowOr {
				return false
			}
			return predicateMatches(e.Left, policy) && predicateMatches(e.Right, policy)
		case "IS", "IS NOT":
			if !policy.allowIsNull {
				return false
			}
			return isNullCheckOperand(e.Left) && isNullLiteral(e.Right)
		default:
			if !isComparisonOp(op) {
				return false
			}
			return isSimpleOperand(e.Left) && isSimpleOperand(e.Right)
		}
	case generator.UnaryExpr:
		if strings.EqualFold(strings.TrimSpace(e.Op), "NOT") {
			if !policy.allowNot {
				return false
			}
			return predicateMatches(e.Expr, policy)
		}
		return false
	default:
		return false
	}
}

func isComparisonOp(op string) bool {
	switch strings.TrimSpace(strings.ToUpper(op)) {
	case "=", "<>", "<", "<=", ">", ">=", "<=>":
		return true
	default:
		return false
	}
}

func isNullCheckOperand(expr generator.Expr) bool {
	_, ok := expr.(generator.ColumnExpr)
	return ok
}

func isNullLiteral(expr generator.Expr) bool {
	if lit, ok := expr.(generator.LiteralExpr); ok {
		return lit.Value == nil
	}
	return false
}

func isSimpleOperand(expr generator.Expr) bool {
	switch v := expr.(type) {
	case generator.ColumnExpr:
		return true
	case generator.LiteralExpr:
		return v.Value != nil
	default:
		return false
	}
}
