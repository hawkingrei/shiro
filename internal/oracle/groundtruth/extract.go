package groundtruth

import (
	"sort"
	"strings"

	"shiro/internal/generator"
	"shiro/internal/schema"
)

// JoinEdgesFromQuery extracts join edges from a generated query.
// It supports USING, and simple equality ON conditions with ANDs.
func JoinEdgesFromQuery(q *generator.SelectQuery, state *schema.State) []JoinEdge {
	if q == nil || state == nil {
		return nil
	}
	if q.From.BaseTable == "" {
		return nil
	}
	leftTables := []string{q.From.BaseTable}
	edges := make([]JoinEdge, 0, len(q.From.Joins))
	for _, join := range q.From.Joins {
		joinType := mapJoinType(join.Type)
		if joinType == JoinCross {
			edges = append(edges, JoinEdge{
				LeftTable:  pickUsingLeftTable(state, leftTables, ""),
				RightTable: join.Table,
				JoinType:   JoinCross,
			})
			leftTables = append(leftTables, join.Table)
			continue
		}
		if len(join.Using) > 0 {
			if edge, ok, reason := extractUsingEdge(state, leftTables, join.Table, join.Using, joinType); ok {
				edges = append(edges, edge)
			} else if reason != "" {
				edges = append(edges, JoinEdge{
					LeftTable:  pickUsingLeftTable(state, leftTables, ""),
					RightTable: join.Table,
					JoinType:   joinType,
					KeyReason:  reason,
				})
			}
			leftTables = append(leftTables, join.Table)
			continue
		}
		if join.On != nil {
			leftTable, rightTable, leftKeys, rightKeys, reason, ok := extractJoinKeys(join.On, state, leftTables, join.Table)
			if ok {
				edge := JoinEdge{
					LeftTable:  leftTable,
					RightTable: rightTable,
					LeftKeys:   leftKeys,
					RightKeys:  rightKeys,
					JoinType:   joinType,
				}
				if len(leftKeys) > 0 {
					edge.LeftKey = leftKeys[0]
				}
				if len(rightKeys) > 0 {
					edge.RightKey = rightKeys[0]
				}
				edges = append(edges, edge)
			} else {
				leftTable := pickUsingLeftTable(state, leftTables, "")
				edges = append(edges, JoinEdge{
					LeftTable:  leftTable,
					RightTable: join.Table,
					JoinType:   joinType,
					KeyReason:  reason,
				})
			}
		}
		leftTables = append(leftTables, join.Table)
	}
	return edges
}

func extractJoinKeys(expr generator.Expr, state *schema.State, leftTables []string, joinTable string) (leftTable string, rightTable string, leftKeys []string, rightKeys []string, reason string, ok bool) {
	candidates := collectJoinKeyCandidates(expr)
	if len(candidates) == 0 {
		if expr == nil || len(expr.Columns()) == 0 {
			return "", "", nil, nil, "no_equal_candidates:no_columns", false
		}
		return "", "", nil, nil, "no_equal_candidates", false
	}
	groups := make(map[string][]joinKeyCandidate)
	for _, cand := range candidates {
		lcol, lok := resolveJoinColumn(state, leftTables, joinTable, cand.left)
		rcol, rok := resolveJoinColumn(state, leftTables, joinTable, cand.right)
		if !lok || !rok {
			continue
		}
		if joinTable != "" && lcol.Table == joinTable && rcol.Table != joinTable {
			lcol, rcol = rcol, lcol
		}
		if joinTable != "" && rcol.Table != joinTable {
			continue
		}
		if joinTable != "" && lcol.Table == joinTable {
			continue
		}
		if !leftTablesContain(leftTables, lcol.Table) {
			continue
		}
		groups[lcol.Table] = append(groups[lcol.Table], joinKeyCandidate{left: lcol, right: rcol})
	}
	if len(groups) == 0 {
		return "", "", nil, nil, "unresolved_columns", false
	}
	var pickedTable string
	var picked []joinKeyCandidate
	tables := make([]string, 0, len(groups))
	for table := range groups {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		list := groups[table]
		if len(list) == 0 {
			continue
		}
		if pickedTable == "" || len(list) > len(picked) || (len(list) == len(picked) && table < pickedTable) {
			pickedTable = table
			picked = list
		}
	}
	if pickedTable == "" {
		return "", "", nil, nil, "empty_group", false
	}
	leftKeys = make([]string, 0, len(picked))
	rightKeys = make([]string, 0, len(picked))
	for _, cand := range picked {
		leftKeys = append(leftKeys, cand.left.Name)
		rightKeys = append(rightKeys, cand.right.Name)
	}
	return pickedTable, joinTable, leftKeys, rightKeys, "", true
}

type joinKeyCandidate struct {
	left  generator.ColumnRef
	right generator.ColumnRef
}

func collectJoinKeyCandidates(expr generator.Expr) []joinKeyCandidate {
	expr, ok := unwrapJoinPredicate(expr)
	if !ok {
		return nil
	}
	bin, ok := expr.(generator.BinaryExpr)
	if !ok {
		return nil
	}
	switch bin.Op {
	case "AND":
		left := collectJoinKeyCandidates(bin.Left)
		right := collectJoinKeyCandidates(bin.Right)
		return append(left, right...)
	case "=", "<=>":
		leftRef, okLeft := unwrapColumnExpr(bin.Left)
		rightRef, okRight := unwrapColumnExpr(bin.Right)
		if !okLeft || !okRight {
			return nil
		}
		return []joinKeyCandidate{{left: leftRef, right: rightRef}}
	default:
		return nil
	}
}

func unwrapJoinPredicate(expr generator.Expr) (generator.Expr, bool) {
	notCount := 0
	for {
		e, ok := expr.(generator.UnaryExpr)
		if ok {
			switch strings.ToUpper(e.Op) {
			case "+":
				expr = e.Expr
				continue
			case "NOT":
				notCount++
				expr = e.Expr
				continue
			}
		}
		break
	}
	if notCount%2 == 1 {
		return nil, false
	}
	return expr, true
}

func unwrapColumnExpr(expr generator.Expr) (generator.ColumnRef, bool) {
	switch e := expr.(type) {
	case generator.ColumnExpr:
		return e.Ref, true
	case generator.UnaryExpr:
		if e.Op == "+" {
			return unwrapColumnExpr(e.Expr)
		}
	case generator.FuncExpr:
		if len(e.Args) != 1 {
			return generator.ColumnRef{}, false
		}
		if !isAllowedJoinFunc(e.Name) {
			return generator.ColumnRef{}, false
		}
		return unwrapColumnExpr(e.Args[0])
	}
	return generator.ColumnRef{}, false
}

func isAllowedJoinFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "CAST", "CONVERT", "BINARY":
		return true
	default:
		return false
	}
}

func resolveJoinColumn(state *schema.State, leftTables []string, joinTable string, ref generator.ColumnRef) (generator.ColumnRef, bool) {
	if ref.Name == "" {
		return generator.ColumnRef{}, false
	}
	if ref.Table != "" {
		return ref, true
	}
	candidates := make([]string, 0, len(leftTables)+1)
	candidates = append(candidates, leftTables...)
	if joinTable != "" {
		candidates = append(candidates, joinTable)
	}
	var resolved string
	for _, table := range candidates {
		if state == nil {
			if resolved != "" {
				return generator.ColumnRef{}, false
			}
			resolved = table
			continue
		}
		tbl, ok := state.TableByName(table)
		if !ok {
			continue
		}
		if _, ok := tbl.ColumnByName(ref.Name); !ok {
			continue
		}
		if resolved != "" && resolved != table {
			return generator.ColumnRef{}, false
		}
		resolved = table
	}
	if resolved == "" {
		return generator.ColumnRef{}, false
	}
	ref.Table = resolved
	return ref, true
}

func extractUsingEdge(state *schema.State, leftTables []string, rightTable string, using []string, joinType JoinType) (JoinEdge, bool, string) {
	var leftTable string
	leftKeys := make([]string, 0, len(using))
	rightKeys := make([]string, 0, len(using))
	for _, name := range using {
		if state != nil && rightTable != "" {
			if tbl, ok := state.TableByName(rightTable); ok {
				if _, ok := tbl.ColumnByName(name); !ok {
					continue
				}
			}
		}
		table := pickUsingLeftTable(state, leftTables, name)
		if table == "" {
			continue
		}
		if leftTable == "" {
			leftTable = table
		}
		if table != leftTable {
			continue
		}
		leftKeys = append(leftKeys, name)
		rightKeys = append(rightKeys, name)
	}
	if leftTable == "" || len(leftKeys) == 0 {
		return JoinEdge{}, false, "using_no_match"
	}
	edge := JoinEdge{
		LeftTable:  leftTable,
		RightTable: rightTable,
		LeftKeys:   leftKeys,
		RightKeys:  rightKeys,
		JoinType:   joinType,
	}
	edge.LeftKey = leftKeys[0]
	edge.RightKey = rightKeys[0]
	return edge, true, ""
}

func leftTablesContain(tables []string, name string) bool {
	for _, table := range tables {
		if table == name {
			return true
		}
	}
	return false
}

func pickUsingLeftTable(state *schema.State, tables []string, col string) string {
	if len(tables) == 0 {
		return ""
	}
	if col == "" {
		return tables[len(tables)-1]
	}
	for i := len(tables) - 1; i >= 0; i-- {
		tbl, ok := state.TableByName(tables[i])
		if !ok {
			continue
		}
		if _, ok := tbl.ColumnByName(col); ok {
			return tables[i]
		}
	}
	return ""
}

func mapJoinType(jt generator.JoinType) JoinType {
	switch jt {
	case generator.JoinLeft:
		return JoinLeft
	case generator.JoinRight:
		return JoinRight
	case generator.JoinCross:
		return JoinCross
	default:
		return JoinInner
	}
}
