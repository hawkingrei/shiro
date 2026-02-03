package groundtruth

import (
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"

	"shiro/internal/schema"
)

var groundtruthParserPool = sync.Pool{
	New: func() any {
		return parser.New()
	},
}

// RefineJoinEdgesWithSQL re-extracts join keys using the SQL AST when keys are missing.
func RefineJoinEdgesWithSQL(sqlText string, state *schema.State, edges []JoinEdge, expected int) []JoinEdge {
	if expected <= 0 {
		return edges
	}
	originalMissing := joinEdgeMissingCount(edges, expected)
	if originalMissing == 0 {
		return edges
	}
	astEdges := JoinEdgesFromSQL(sqlText, state)
	if len(astEdges) != expected {
		return edges
	}
	if joinEdgeMissingCount(astEdges, expected) >= originalMissing {
		return edges
	}
	return astEdges
}

// JoinEdgesFromSQL parses SQL and extracts join edges using the AST.
func JoinEdgesFromSQL(sqlText string, state *schema.State) []JoinEdge {
	if strings.TrimSpace(sqlText) == "" {
		return nil
	}
	parser := groundtruthParserPool.Get().(*parser.Parser)
	defer groundtruthParserPool.Put(parser)
	stmt, err := parser.ParseOneStmt(sqlText, "", "")
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok || sel.From == nil || sel.From.TableRefs == nil {
		return nil
	}
	aliases := map[string]string{}
	edges, tables, ok := extractJoinEdgesFromResultSet(sel.From.TableRefs, state, aliases)
	if !ok || len(tables) == 0 {
		return nil
	}
	return edges
}

func joinEdgeMissingCount(edges []JoinEdge, expected int) int {
	if len(edges) != expected {
		return expected
	}
	missing := 0
	for _, edge := range edges {
		if len(edge.LeftKeyList()) == 0 || len(edge.RightKeyList()) == 0 {
			missing++
		}
	}
	return missing
}

func extractJoinEdgesFromResultSet(node ast.ResultSetNode, state *schema.State, aliases map[string]string) ([]JoinEdge, []string, bool) {
	switch v := node.(type) {
	case *ast.TableSource:
		name, ok := tableNameFromSource(v, aliases)
		if !ok {
			return nil, nil, false
		}
		return nil, []string{name}, true
	case *ast.Join:
		leftEdges, leftTables, ok := extractJoinEdgesFromResultSet(v.Left, state, aliases)
		if !ok {
			return nil, nil, false
		}
		rightEdges, rightTables, ok := extractJoinEdgesFromResultSet(v.Right, state, aliases)
		if !ok || len(rightEdges) > 0 || len(rightTables) != 1 {
			return nil, nil, false
		}
		rightTable := rightTables[0]
		edge := JoinEdge{
			LeftTable:  pickUsingLeftTable(state, leftTables, ""),
			RightTable: rightTable,
			JoinType:   mapJoinTypeFromAST(v.Tp),
		}
		if len(v.Using) > 0 {
			if leftTable, leftKeys, rightKeys, reason, ok := extractJoinKeyFromASTUsing(state, leftTables, rightTable, v.Using); ok {
				edge.LeftTable = leftTable
				edge.LeftKeys = leftKeys
				edge.RightKeys = rightKeys
				if len(leftKeys) > 0 {
					edge.LeftKey = leftKeys[0]
				}
				if len(rightKeys) > 0 {
					edge.RightKey = rightKeys[0]
				}
			} else if reason != "" {
				edge.KeyReason = reason
			}
		} else if v.On != nil && v.On.Expr != nil {
			if leftTable, rightTable, leftKeys, rightKeys, reason, ok := extractJoinKeysFromASTExpr(v.On.Expr, state, leftTables, rightTable, aliases); ok {
				edge.LeftTable = leftTable
				edge.RightTable = rightTable
				edge.LeftKeys = leftKeys
				edge.RightKeys = rightKeys
				if len(leftKeys) > 0 {
					edge.LeftKey = leftKeys[0]
				}
				if len(rightKeys) > 0 {
					edge.RightKey = rightKeys[0]
				}
			} else if reason != "" {
				edge.KeyReason = reason
			}
		}
		edges := append(leftEdges, edge)
		tables := append(append([]string{}, leftTables...), rightTable)
		return edges, tables, true
	default:
		return nil, nil, false
	}
}

func tableNameFromSource(source *ast.TableSource, aliases map[string]string) (string, bool) {
	if source == nil {
		return "", false
	}
	switch node := source.Source.(type) {
	case *ast.TableName:
		name := node.Name.O
		if source.AsName.O != "" {
			aliases[source.AsName.O] = name
		}
		return name, true
	default:
		return "", false
	}
}

func mapJoinTypeFromAST(tp ast.JoinType) JoinType {
	switch tp {
	case ast.LeftJoin:
		return JoinLeft
	case ast.RightJoin:
		return JoinRight
	case ast.CrossJoin:
		return JoinCross
	default:
		return JoinInner
	}
}

type astJoinKey struct {
	table string
	name  string
}

type astJoinCandidate struct {
	left  *ast.ColumnName
	right *ast.ColumnName
}

type astJoinKeyPair struct {
	left  astJoinKey
	right astJoinKey
}

func extractJoinKeysFromASTExpr(expr ast.ExprNode, state *schema.State, leftTables []string, joinTable string, aliases map[string]string) (leftTable string, rightTable string, leftKeys []string, rightKeys []string, reason string, ok bool) {
	groups := make(map[string][]astJoinKeyPair)
	candidates := collectJoinKeyCandidatesAST(expr)
	if len(candidates) == 0 {
		return "", "", nil, nil, "no_equal_candidates", false
	}
	for _, cand := range candidates {
		lcol, lok := resolveASTColumn(state, leftTables, joinTable, aliases, cand.left)
		rcol, rok := resolveASTColumn(state, leftTables, joinTable, aliases, cand.right)
		if !lok || !rok {
			continue
		}
		if joinTable != "" && lcol.table == joinTable && rcol.table != joinTable {
			lcol, rcol = rcol, lcol
		}
		if joinTable != "" && rcol.table != joinTable {
			continue
		}
		if joinTable != "" && lcol.table == joinTable {
			continue
		}
		if !leftTablesContain(leftTables, lcol.table) {
			continue
		}
		groups[lcol.table] = append(groups[lcol.table], astJoinKeyPair{left: lcol, right: rcol})
	}
	if len(groups) == 0 {
		return "", "", nil, nil, "unresolved_columns", false
	}
	var pickedTable string
	var picked []astJoinKeyPair
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
		leftKeys = append(leftKeys, cand.left.name)
		rightKeys = append(rightKeys, cand.right.name)
	}
	return pickedTable, joinTable, leftKeys, rightKeys, "", len(leftKeys) > 0
}

func collectJoinKeyCandidatesAST(expr ast.ExprNode) []astJoinCandidate {
	switch node := expr.(type) {
	case *ast.ParenthesesExpr:
		return collectJoinKeyCandidatesAST(node.Expr)
	case *ast.BinaryOperationExpr:
		switch node.Op {
		case opcode.LogicAnd:
			left := collectJoinKeyCandidatesAST(node.L)
			right := collectJoinKeyCandidatesAST(node.R)
			return append(left, right...)
		case opcode.EQ:
			leftExpr := unwrapASTColumnName(node.L)
			rightExpr := unwrapASTColumnName(node.R)
			if leftExpr == nil || rightExpr == nil {
				return nil
			}
			return []astJoinCandidate{{left: leftExpr.Name, right: rightExpr.Name}}
		default:
			return nil
		}
	default:
		return nil
	}
}

func unwrapASTColumnName(expr ast.ExprNode) *ast.ColumnNameExpr {
	switch node := expr.(type) {
	case *ast.ColumnNameExpr:
		return node
	case *ast.ParenthesesExpr:
		return unwrapASTColumnName(node.Expr)
	case *ast.UnaryOperationExpr:
		if node.Op == opcode.Plus {
			return unwrapASTColumnName(node.V)
		}
	case *ast.FuncCastExpr:
		return unwrapASTColumnName(node.Expr)
	}
	return nil
}

func resolveASTColumn(state *schema.State, leftTables []string, rightTable string, aliases map[string]string, name *ast.ColumnName) (astJoinKey, bool) {
	if name == nil || name.Name.O == "" {
		return astJoinKey{}, false
	}
	if name.Table.O != "" {
		table := resolveAlias(aliases, name.Table.O)
		if !hasColumn(state, table, name.Name.O) {
			return astJoinKey{}, false
		}
		return astJoinKey{table: table, name: name.Name.O}, true
	}
	candidates := make([]string, 0, len(leftTables)+1)
	candidates = append(candidates, leftTables...)
	if rightTable != "" {
		candidates = append(candidates, rightTable)
	}
	var resolved string
	for _, table := range candidates {
		if !hasColumn(state, table, name.Name.O) {
			continue
		}
		if resolved != "" && resolved != table {
			return astJoinKey{}, false
		}
		resolved = table
	}
	if resolved == "" {
		return astJoinKey{}, false
	}
	return astJoinKey{table: resolved, name: name.Name.O}, true
}

func extractJoinKeyFromASTUsing(state *schema.State, leftTables []string, rightTable string, using []*ast.ColumnName) (leftTable string, leftKeys []string, rightKeys []string, reason string, ok bool) {
	if rightTable == "" {
		return "", nil, nil, "using_no_match", false
	}
	for _, col := range using {
		if col == nil || col.Name.O == "" {
			continue
		}
		if !hasColumn(state, rightTable, col.Name.O) {
			continue
		}
		table := pickUsingLeftTable(state, leftTables, col.Name.O)
		if table == "" {
			continue
		}
		if leftTable == "" {
			leftTable = table
		}
		if table != leftTable {
			continue
		}
		leftKeys = append(leftKeys, col.Name.O)
		rightKeys = append(rightKeys, col.Name.O)
	}
	if leftTable == "" || len(leftKeys) == 0 {
		return "", nil, nil, "using_no_match", false
	}
	return leftTable, leftKeys, rightKeys, "", true
}

func resolveAlias(aliases map[string]string, table string) string {
	if aliases == nil {
		return table
	}
	if mapped, ok := aliases[table]; ok && mapped != "" {
		return mapped
	}
	return table
}

func hasColumn(state *schema.State, table string, col string) bool {
	if state == nil || table == "" || col == "" {
		return false
	}
	tbl, ok := state.TableByName(table)
	if !ok {
		return false
	}
	_, ok = tbl.ColumnByName(col)
	return ok
}
