package generator

import "shiro/internal/schema"

type tableSet map[string]struct{}

type tableScope struct {
	tables  tableSet
	columns map[string]map[string]struct{}
}

type scopeManager struct {
	tableResolver func(query *SelectQuery) []schema.Table
}

func (m scopeManager) validateQuery(query *SelectQuery, scope tableScope, outer tableScope) bool {
	if query == nil {
		return true
	}
	for _, op := range query.SetOps {
		if op.Query == nil {
			continue
		}
		if !m.validateQuery(op.Query, m.scopeForQuery(op.Query), tableScope{}) {
			return false
		}
	}
	for _, cte := range query.With {
		if !m.validateQuery(cte.Query, m.scopeForQuery(cte.Query), tableScope{}) {
			return false
		}
	}
	if query.From.BaseQuery != nil && !m.validateQuery(query.From.BaseQuery, m.scopeForQuery(query.From.BaseQuery), tableScope{}) {
		return false
	}
	currentScope := scope
	if len(query.From.Joins) > 0 {
		visible := []string{}
		if baseName := query.From.baseName(); baseName != "" {
			visible = append(visible, baseName)
		}
		for _, join := range query.From.Joins {
			if join.TableQuery != nil && !m.validateQuery(join.TableQuery, m.scopeForQuery(join.TableQuery), tableScope{}) {
				return false
			}
			joinScope := scopeForTables(currentScope, visible)
			if joinName := join.tableName(); joinName != "" {
				joinScope = scopeForTables(currentScope, append(visible, joinName))
			}
			if join.On != nil && !m.validateExpr(join.On, joinScope, outer) {
				return false
			}
			if usingCols := joinUsingColumns(join); len(usingCols) > 0 {
				affected := append([]string{}, visible...)
				if joinName := join.tableName(); joinName != "" {
					affected = append(affected, joinName)
				}
				currentScope = hideQualifiedColumns(currentScope, affected, usingCols)
			}
			if joinName := join.tableName(); joinName != "" {
				visible = append(visible, joinName)
			}
		}
	}
	for _, item := range query.Items {
		if !m.validateExpr(item.Expr, currentScope, outer) {
			return false
		}
	}
	if query.Where != nil && !m.validateExpr(query.Where, currentScope, outer) {
		return false
	}
	for _, expr := range query.GroupBy {
		if !m.validateExpr(expr, currentScope, outer) {
			return false
		}
	}
	if query.Having != nil && !m.validateExpr(query.Having, currentScope, outer) {
		return false
	}
	for _, def := range query.WindowDefs {
		for _, expr := range def.PartitionBy {
			if !m.validateExpr(expr, currentScope, outer) {
				return false
			}
		}
		for _, ob := range def.OrderBy {
			if !m.validateExpr(ob.Expr, currentScope, outer) {
				return false
			}
		}
	}
	for _, ob := range query.OrderBy {
		if !m.validateExpr(ob.Expr, currentScope, outer) {
			return false
		}
	}
	return true
}

func (m scopeManager) validateExpr(expr Expr, scope tableScope, outer tableScope) bool {
	switch e := expr.(type) {
	case nil:
		return true
	case ColumnExpr:
		if e.Ref.Table == "" {
			return true
		}
		return columnAllowed(e.Ref, scope, outer)
	case LiteralExpr, ParamExpr:
		return true
	case UnaryExpr:
		return m.validateExpr(e.Expr, scope, outer)
	case BinaryExpr:
		return m.validateExpr(e.Left, scope, outer) && m.validateExpr(e.Right, scope, outer)
	case FuncExpr:
		for _, arg := range e.Args {
			if !m.validateExpr(arg, scope, outer) {
				return false
			}
		}
		return true
	case CaseExpr:
		for _, w := range e.Whens {
			if !m.validateExpr(w.When, scope, outer) || !m.validateExpr(w.Then, scope, outer) {
				return false
			}
		}
		if e.Else != nil && !m.validateExpr(e.Else, scope, outer) {
			return false
		}
		return true
	case GroupByOrdinalExpr:
		if e.Expr == nil {
			return true
		}
		return m.validateExpr(e.Expr, scope, outer)
	case SubqueryExpr:
		return m.validateQuery(e.Query, m.scopeForQuery(e.Query), mergeTableScopes(scope, outer))
	case ExistsExpr:
		return m.validateQuery(e.Query, m.scopeForQuery(e.Query), mergeTableScopes(scope, outer))
	case CompareSubqueryExpr:
		if !m.validateExpr(e.Left, scope, outer) {
			return false
		}
		return m.validateQuery(e.Query, m.scopeForQuery(e.Query), mergeTableScopes(scope, outer))
	case InExpr:
		if !m.validateExpr(e.Left, scope, outer) {
			return false
		}
		for _, item := range e.List {
			if !m.validateExpr(item, scope, outer) {
				return false
			}
		}
		return true
	case WindowExpr:
		for _, arg := range e.Args {
			if !m.validateExpr(arg, scope, outer) {
				return false
			}
		}
		for _, expr := range e.PartitionBy {
			if !m.validateExpr(expr, scope, outer) {
				return false
			}
		}
		for _, ob := range e.OrderBy {
			if !m.validateExpr(ob.Expr, scope, outer) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

func (m scopeManager) scopeForQuery(query *SelectQuery) tableScope {
	if query == nil {
		return scopeTablesForQuery(nil, nil)
	}
	if m.tableResolver == nil {
		return scopeTablesForQuery(query, nil)
	}
	return scopeTablesForQuery(query, m.tableResolver(query))
}

func scopeTablesForQuery(query *SelectQuery, tables []schema.Table) tableScope {
	scope := tableScope{
		tables:  tableSet{},
		columns: map[string]map[string]struct{}{},
	}
	if query == nil {
		return scope
	}
	// Set-operation operands are validated independently and are not visible to the
	// current query body; do not merge their scope here.
	for _, tbl := range tables {
		if tbl.Name == "" {
			continue
		}
		scope.tables[tbl.Name] = struct{}{}
		if len(tbl.Columns) > 0 {
			colSet := make(map[string]struct{}, len(tbl.Columns))
			for _, col := range tbl.Columns {
				colSet[col.Name] = struct{}{}
			}
			scope.columns[tbl.Name] = colSet
		}
	}
	if baseName := query.From.baseName(); baseName != "" {
		scope.tables[baseName] = struct{}{}
	}
	for _, join := range query.From.Joins {
		if joinName := join.tableName(); joinName != "" {
			scope.tables[joinName] = struct{}{}
		}
	}
	return scope
}

func scopeForTables(scope tableScope, tables []string) tableScope {
	out := tableScope{
		tables:  tableSet{},
		columns: map[string]map[string]struct{}{},
	}
	if len(tables) == 0 {
		return out
	}
	for _, name := range tables {
		if name == "" {
			continue
		}
		if _, ok := scope.tables[name]; !ok {
			continue
		}
		out.tables[name] = struct{}{}
		if cols, ok := scope.columns[name]; ok {
			out.columns[name] = cols
		}
	}
	return out
}

func mergeTableScopes(left tableScope, right tableScope) tableScope {
	if len(left.tables) == 0 && len(right.tables) == 0 {
		return tableScope{tables: tableSet{}, columns: map[string]map[string]struct{}{}}
	}
	out := tableScope{
		tables:  tableSet{},
		columns: map[string]map[string]struct{}{},
	}
	for k := range left.tables {
		out.tables[k] = struct{}{}
	}
	for k := range right.tables {
		out.tables[k] = struct{}{}
	}
	for k, v := range left.columns {
		out.columns[k] = v
	}
	for k, v := range right.columns {
		if _, ok := out.columns[k]; !ok {
			out.columns[k] = v
		}
	}
	return out
}

func cloneScope(scope tableScope) tableScope {
	out := tableScope{
		tables:  tableSet{},
		columns: map[string]map[string]struct{}{},
	}
	for name := range scope.tables {
		out.tables[name] = struct{}{}
	}
	for table, cols := range scope.columns {
		copied := make(map[string]struct{}, len(cols))
		for col := range cols {
			copied[col] = struct{}{}
		}
		out.columns[table] = copied
	}
	return out
}

func hideQualifiedColumns(scope tableScope, tables []string, columns []string) tableScope {
	if len(tables) == 0 || len(columns) == 0 {
		return scope
	}
	out := cloneScope(scope)
	for _, table := range tables {
		colSet, ok := out.columns[table]
		if !ok || len(colSet) == 0 {
			continue
		}
		for _, col := range columns {
			delete(colSet, col)
		}
		out.columns[table] = colSet
	}
	return out
}

func joinUsingColumns(join Join) []string {
	if len(join.Using) == 0 {
		return nil
	}
	cols := make([]string, 0, len(join.Using))
	seen := make(map[string]struct{}, len(join.Using))
	for _, col := range join.Using {
		if col == "" {
			continue
		}
		if _, ok := seen[col]; ok {
			continue
		}
		seen[col] = struct{}{}
		cols = append(cols, col)
	}
	return cols
}

func (g *Generator) validateQueryScope(query *SelectQuery) bool {
	tables := g.scopeTablesForQuery(query)
	return scopeManager{tableResolver: g.scopeTablesForQuery}.validateQuery(query, scopeTablesForQuery(query, tables), tableScope{})
}

// ValidateExprInQueryScope reports whether an expression only uses columns visible in query.
func (g *Generator) ValidateExprInQueryScope(expr Expr, query *SelectQuery) bool {
	if query == nil || expr == nil {
		return true
	}
	tables := g.scopeTablesForQuery(query)
	scope := scopeTablesForQuery(query, tables)
	return scopeManager{tableResolver: g.scopeTablesForQuery}.validateExpr(expr, scope, tableScope{})
}

// ValidateQueryScope reports whether query only uses columns visible in each scope.
func (g *Generator) ValidateQueryScope(query *SelectQuery) bool {
	return g.validateQueryScope(query)
}

// TablesForQueryScope returns the tables visible to a query, including CTE outputs.
func (g *Generator) TablesForQueryScope(query *SelectQuery) []schema.Table {
	return g.scopeTablesForQuery(query)
}

func (g *Generator) scopeTablesForQuery(query *SelectQuery) []schema.Table {
	if query == nil {
		return nil
	}
	byName := make(map[string]schema.Table)
	if g.State != nil {
		for _, tbl := range g.State.Tables {
			byName[tbl.Name] = tbl
		}
	}
	for _, cte := range query.With {
		cols := g.columnsFromSelectItems(cte.Query.Items)
		byName[cte.Name] = schema.Table{Name: cte.Name, Columns: cols}
	}
	if query.From.BaseQuery != nil {
		alias := query.From.baseName()
		byName[alias] = schema.Table{Name: alias, Columns: g.columnsFromSelectItems(query.From.BaseQuery.Items)}
	}
	names := []string{query.From.baseName()}
	for _, join := range query.From.Joins {
		if join.TableQuery != nil {
			alias := join.tableName()
			byName[alias] = schema.Table{Name: alias, Columns: g.columnsFromSelectItems(join.TableQuery.Items)}
		}
		names = append(names, join.tableName())
	}
	out := make([]schema.Table, 0, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		if tbl, ok := byName[name]; ok {
			out = append(out, tbl)
		}
	}
	return out
}

func columnAllowed(ref ColumnRef, scope tableScope, outer tableScope) bool {
	if ref.Table == "" {
		return true
	}
	if _, ok := scope.tables[ref.Table]; ok {
		if cols, ok := scope.columns[ref.Table]; ok {
			_, ok := cols[ref.Name]
			return ok
		}
		return true
	}
	if _, ok := outer.tables[ref.Table]; ok {
		if cols, ok := outer.columns[ref.Table]; ok {
			_, ok := cols[ref.Name]
			return ok
		}
		return true
	}
	return false
}
