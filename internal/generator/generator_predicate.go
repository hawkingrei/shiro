package generator

import (
	"shiro/internal/schema"
	"shiro/internal/util"
)

// GeneratePredicate builds a boolean predicate expression.
func (g *Generator) GeneratePredicate(tables []schema.Table, depth int, allowSubquery bool, subqDepth int) Expr {
	if g.disallowScalarSubq {
		allowSubquery = false
	}
	if allowSubquery && subqDepth > 0 && util.Chance(g.Rand, g.subqCount()*PredicateSubqueryScale) {
		g.subqueryAttempts++
		sub := g.GenerateSubquery(tables, subqDepth-1)
		if sub != nil {
			g.subqueryBuilt++
			if util.Chance(g.Rand, PredicateExistsProb) {
				existsSub := g.generateExistsSubquery(tables, subqDepth-1)
				if existsSub != nil {
					sub = existsSub
				}
				expr := Expr(ExistsExpr{Query: sub})
				if g.Config.Features.NotExists && util.Chance(g.Rand, g.Config.Weights.Features.NotExistsProb) {
					return UnaryExpr{Op: "NOT", Expr: expr}
				}
				return expr
			}
			leftExpr, leftType, _ := g.pickComparableExprPreferJoinGraph(tables)
			typedSub := g.generateInSubquery(tables, leftType, subqDepth-1)
			if typedSub == nil {
				left, ok := g.pickNumericExprPreferJoinGraph(tables)
				if !ok {
					left = g.generateScalarExpr(tables, 0, false, 0)
					if !g.isNumericExpr(left) {
						left = g.GenerateNumericExpr(tables)
					}
				}
				expr := Expr(InExpr{Left: left, List: []Expr{SubqueryExpr{Query: sub}}})
				if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
					return UnaryExpr{Op: "NOT", Expr: expr}
				}
				return expr
			}
			if g.Config.Features.QuantifiedSubqueries && util.Chance(g.Rand, QuantifiedSubqueryProb) {
				return CompareSubqueryExpr{
					Left:       leftExpr,
					Op:         g.pickQuantifiedComparison(),
					Quantifier: g.pickSubqueryQuantifier(),
					Query:      typedSub,
				}
			}
			expr := Expr(InExpr{Left: leftExpr, List: []Expr{SubqueryExpr{Query: typedSub}}})
			if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
				return UnaryExpr{Op: "NOT", Expr: expr}
			}
			return expr
		}
		g.subqueryFailed++
	}
	if depth <= 0 {
		left, right := g.generateComparablePair(tables, allowSubquery, subqDepth)
		return BinaryExpr{Left: left, Op: g.pickComparison(), Right: right}
	}
	if util.Chance(g.Rand, PredicateInListProb) {
		leftExpr, colType, _ := g.pickComparableExprPreferJoinGraph(tables)
		listSize := g.Rand.Intn(PredicateInListMax) + 1
		list := make([]Expr, 0, listSize)
		for i := 0; i < listSize; i++ {
			list = append(list, g.literalForColumn(schema.Column{Type: colType}))
		}
		expr := Expr(InExpr{Left: leftExpr, List: list})
		if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
			return UnaryExpr{Op: "NOT", Expr: expr}
		}
		return expr
	}
	choice := g.Rand.Intn(3)
	if choice == 0 {
		left, right := g.generateComparablePair(tables, allowSubquery, subqDepth)
		return BinaryExpr{Left: left, Op: g.pickComparison(), Right: right}
	}
	left := g.GeneratePredicate(tables, depth-1, allowSubquery, subqDepth)
	right := g.GeneratePredicate(tables, depth-1, allowSubquery, subqDepth)
	op := "AND"
	if util.Chance(g.Rand, PredicateOrProb) {
		op = "OR"
	}
	return BinaryExpr{Left: left, Op: op, Right: right}
}

// GenerateSimplePredicate builds a deterministic predicate composed of comparisons joined by AND.
func (g *Generator) GenerateSimplePredicate(tables []schema.Table, depth int) Expr {
	if depth <= 0 {
		left, right := g.generateComparablePair(tables, false, 0)
		return BinaryExpr{Left: left, Op: g.pickComparison(), Right: right}
	}
	left := g.GenerateSimplePredicate(tables, depth-1)
	right := g.GenerateSimplePredicate(tables, depth-1)
	return BinaryExpr{Left: left, Op: "AND", Right: right}
}

// GenerateSimplePredicateColumns builds an AND-only predicate with column comparisons only.
func (g *Generator) GenerateSimplePredicateColumns(tables []schema.Table, depth int) Expr {
	if depth <= 0 {
		return g.generateComparableColumnPredicate(tables)
	}
	left := g.GenerateSimplePredicateColumns(tables, depth-1)
	if left == nil {
		return nil
	}
	right := g.GenerateSimplePredicateColumns(tables, depth-1)
	if right == nil {
		return left
	}
	return BinaryExpr{Left: left, Op: "AND", Right: right}
}

// GenerateSimpleColumnLiteralPredicate builds a single column-to-literal comparison.
func (g *Generator) GenerateSimpleColumnLiteralPredicate(tables []schema.Table) Expr {
	if col, ok := g.pickComparableColumn(tables); ok {
		return BinaryExpr{
			Left:  ColumnExpr{Ref: col},
			Op:    g.pickComparison(),
			Right: g.literalForColumnRef(col),
		}
	}
	return nil
}

func (g *Generator) generateComparableColumnPredicate(tables []schema.Table) Expr {
	if leftCol, rightCol, ok := g.pickComparableColumnPair(tables); ok {
		return BinaryExpr{
			Left:  ColumnExpr{Ref: leftCol},
			Op:    g.pickComparison(),
			Right: ColumnExpr{Ref: rightCol},
		}
	}
	if col, ok := g.pickComparableColumn(tables); ok {
		return BinaryExpr{
			Left:  ColumnExpr{Ref: col},
			Op:    g.pickComparison(),
			Right: g.literalForColumnRef(col),
		}
	}
	return nil
}

// GenerateHavingPredicate builds a HAVING predicate from group-by expressions and aggregates.
func (g *Generator) GenerateHavingPredicate(groupBy []Expr, tables []schema.Table) Expr {
	candidates := make([]Expr, 0, len(groupBy)+2)
	candidates = append(candidates, groupBy...)
	candidates = append(candidates, FuncExpr{Name: "COUNT", Args: []Expr{LiteralExpr{Value: 1}}})
	sumArg := g.GenerateNumericExprNoDouble(tables)
	g.warnAggOnDouble("SUM", sumArg)
	candidates = append(candidates, FuncExpr{Name: "SUM", Args: []Expr{sumArg}})
	expr := candidates[g.Rand.Intn(len(candidates))]
	if colType, ok := g.exprType(expr); ok {
		return BinaryExpr{Left: expr, Op: g.pickComparison(), Right: g.literalForColumn(schema.Column{Type: colType})}
	}
	return BinaryExpr{Left: expr, Op: g.pickComparison(), Right: g.randomLiteralExpr()}
}

// GenerateScalarExpr builds a scalar expression with bounded depth.
func (g *Generator) GenerateScalarExpr(tables []schema.Table, depth int, allowSubquery bool) Expr {
	return g.generateScalarExpr(tables, depth, allowSubquery, g.maxSubqDepth)
}

func (g *Generator) falseExpr() Expr {
	return BinaryExpr{Left: LiteralExpr{Value: 1}, Op: "=", Right: LiteralExpr{Value: 0}}
}

func (g *Generator) pickQuantifiedComparison() string {
	ops := []string{"=", "!=", "<", "<=", ">", ">="}
	return ops[g.Rand.Intn(len(ops))]
}

func (g *Generator) pickSubqueryQuantifier() string {
	quantifiers := []string{"ANY", "SOME", "ALL"}
	return quantifiers[g.Rand.Intn(len(quantifiers))]
}
