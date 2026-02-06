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
	from := query.From.BaseTable
	for _, join := range query.From.Joins {
		from += fmt.Sprintf(" %s %s", join.Type, join.Table)
		if len(join.Using) > 0 {
			from += " USING (" + strings.Join(join.Using, ", ") + ")"
		} else if join.On != nil {
			from += " ON " + buildExpr(join.On)
		}
	}
	return from
}

func buildWith(query *generator.SelectQuery) string {
	if len(query.With) == 0 {
		return ""
	}
	parts := make([]string, 0, len(query.With))
	for _, cte := range query.With {
		parts = append(parts, fmt.Sprintf("%s AS (%s)", cte.Name, cte.Query.SQLString()))
	}
	return "WITH " + strings.Join(parts, ", ") + " "
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
	for _, item := range query.Items {
		if exprHasAggregate(item.Expr) {
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
	return false
}

func queryColumnsValid(query *generator.SelectQuery, state *schema.State, outerTables map[string]schema.Table) (bool, string) {
	if query == nil || state == nil {
		return false, "nil_query_or_state"
	}
	if len(query.With) > 0 {
		return false, "with_clause"
	}
	tableMap := map[string]schema.Table{}
	for name, tbl := range outerTables {
		tableMap[name] = tbl
	}
	names := []string{query.From.BaseTable}
	for _, join := range query.From.Joins {
		names = append(names, join.Table)
	}
	for _, name := range names {
		if name == "" {
			return false, "empty_table"
		}
		tbl, ok := state.TableByName(name)
		if !ok {
			return false, "unknown_table"
		}
		tableMap[name] = tbl
	}
	if len(query.From.Joins) > 0 {
		leftTables := make([]schema.Table, 0, len(query.From.Joins)+1)
		if base, ok := tableMap[query.From.BaseTable]; ok {
			leftTables = append(leftTables, base)
		}
		for _, join := range query.From.Joins {
			if len(join.Using) > 0 {
				right, ok := tableMap[join.Table]
				if !ok {
					return false, "unknown_table"
				}
				for _, col := range join.Using {
					if _, ok := right.ColumnByName(col); !ok {
						return false, "unknown_using_column"
					}
					found := false
					for _, left := range leftTables {
						if _, ok := left.ColumnByName(col); ok {
							found = true
							break
						}
					}
					if !found {
						return false, "unknown_using_column"
					}
				}
			}
			if tbl, ok := tableMap[join.Table]; ok {
				leftTables = append(leftTables, tbl)
			}
		}
	}
	checkColumn := func(ref generator.ColumnRef) (bool, string) {
		if ref.Table == "" || ref.Name == "" {
			return false, "empty_column"
		}
		tbl, ok := tableMap[ref.Table]
		if !ok {
			return false, "unknown_table"
		}
		if _, ok := tbl.ColumnByName(ref.Name); !ok {
			return false, "unknown_column"
		}
		return true, ""
	}
	var checkExpr func(expr generator.Expr) (bool, string)
	checkExpr = func(expr generator.Expr) (bool, string) {
		if expr == nil {
			return true, ""
		}
		switch e := expr.(type) {
		case generator.ColumnExpr:
			return checkColumn(e.Ref)
		case generator.LiteralExpr, generator.ParamExpr:
			return true, ""
		case generator.UnaryExpr:
			return checkExpr(e.Expr)
		case generator.BinaryExpr:
			if ok, reason := checkExpr(e.Left); !ok {
				return false, reason
			}
			return checkExpr(e.Right)
		case generator.FuncExpr:
			for _, arg := range e.Args {
				if ok, reason := checkExpr(arg); !ok {
					return false, reason
				}
			}
			return true, ""
		case generator.GroupByOrdinalExpr:
			if e.Expr == nil {
				return true, ""
			}
			return checkExpr(e.Expr)
		case generator.CaseExpr:
			for _, w := range e.Whens {
				if ok, reason := checkExpr(w.When); !ok {
					return false, reason
				}
				if ok, reason := checkExpr(w.Then); !ok {
					return false, reason
				}
			}
			if ok, reason := checkExpr(e.Else); !ok {
				return false, reason
			}
			return true, ""
		case generator.InExpr:
			if ok, reason := checkExpr(e.Left); !ok {
				return false, reason
			}
			for _, item := range e.List {
				if ok, reason := checkExpr(item); !ok {
					return false, reason
				}
			}
			return true, ""
		case generator.SubqueryExpr:
			return queryColumnsValid(e.Query, state, tableMap)
		case generator.ExistsExpr:
			return queryColumnsValid(e.Query, state, tableMap)
		case generator.WindowExpr:
			for _, arg := range e.Args {
				if ok, reason := checkExpr(arg); !ok {
					return false, reason
				}
			}
			for _, expr := range e.PartitionBy {
				if ok, reason := checkExpr(expr); !ok {
					return false, reason
				}
			}
			for _, ob := range e.OrderBy {
				if ok, reason := checkExpr(ob.Expr); !ok {
					return false, reason
				}
			}
			return true, ""
		default:
			for _, col := range expr.Columns() {
				if ok, reason := checkColumn(col); !ok {
					return false, reason
				}
			}
			return true, ""
		}
	}

	leftTables := []schema.Table{}
	base, ok := tableMap[query.From.BaseTable]
	if !ok {
		return false, "unknown_table"
	}
	leftTables = append(leftTables, base)
	for _, join := range query.From.Joins {
		right, ok := tableMap[join.Table]
		if !ok {
			return false, "unknown_table"
		}
		if len(join.Using) > 0 {
			for _, name := range join.Using {
				if _, ok := right.ColumnByName(name); !ok {
					return false, "unknown_using_column"
				}
				found := false
				for _, ltbl := range leftTables {
					if _, ok := ltbl.ColumnByName(name); ok {
						found = true
						break
					}
				}
				if !found {
					return false, "unknown_using_column"
				}
			}
		}
		if ok, reason := checkExpr(join.On); !ok {
			return false, reason
		}
		leftTables = append(leftTables, right)
	}

	for _, item := range query.Items {
		if ok, reason := checkExpr(item.Expr); !ok {
			return false, reason
		}
	}
	if ok, reason := checkExpr(query.Where); !ok {
		return false, reason
	}
	if ok, reason := checkExpr(query.Having); !ok {
		return false, reason
	}
	for _, expr := range query.GroupBy {
		if ok, reason := checkExpr(expr); !ok {
			return false, reason
		}
	}
	for _, ob := range query.OrderBy {
		if ok, reason := checkExpr(ob.Expr); !ok {
			return false, reason
		}
	}
	return true, ""
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
		if ref.Table == "" || ref.Name == "" {
			return false
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
