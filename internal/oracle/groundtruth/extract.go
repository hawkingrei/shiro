package groundtruth

import (
	"shiro/internal/generator"
	"shiro/internal/schema"
)

// JoinEdgesFromQuery extracts join edges from a generated query.
// It supports USING and simple equality ON conditions.
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
			for _, name := range join.Using {
				leftTable := pickUsingLeftTable(state, leftTables, name)
				if leftTable == "" {
					continue
				}
				edges = append(edges, JoinEdge{
					LeftTable:  leftTable,
					RightTable: join.Table,
					LeftKey:    name,
					RightKey:   name,
					JoinType:   joinType,
				})
			}
			leftTables = append(leftTables, join.Table)
			continue
		}
		if join.On != nil {
			l, r, ok := extractColumnEquality(join.On)
			if ok {
				left := l
				right := r
				if l.Table == join.Table {
					left, right = r, l
				}
				edges = append(edges, JoinEdge{
					LeftTable:  left.Table,
					RightTable: right.Table,
					LeftKey:    left.Name,
					RightKey:   right.Name,
					JoinType:   joinType,
				})
			}
		}
		leftTables = append(leftTables, join.Table)
	}
	return edges
}

func extractColumnEquality(expr generator.Expr) (left generator.ColumnRef, right generator.ColumnRef, ok bool) {
	bin, ok := expr.(generator.BinaryExpr)
	if !ok || bin.Op != "=" {
		return generator.ColumnRef{}, generator.ColumnRef{}, false
	}
	leftExpr, okLeft := bin.Left.(generator.ColumnExpr)
	rightExpr, okRight := bin.Right.(generator.ColumnExpr)
	if !okLeft || !okRight {
		return generator.ColumnRef{}, generator.ColumnRef{}, false
	}
	if leftExpr.Ref.Table == "" || rightExpr.Ref.Table == "" {
		return generator.ColumnRef{}, generator.ColumnRef{}, false
	}
	return leftExpr.Ref, rightExpr.Ref, true
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
