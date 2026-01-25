package generator

import (
	"shiro/internal/schema"
	"shiro/internal/util"
)

// GeneratePredicate builds a boolean predicate expression.
func (g *Generator) GeneratePredicate(tables []schema.Table, depth int, allowSubquery bool, subqDepth int) Expr {
	if allowSubquery && subqDepth > 0 && util.Chance(g.Rand, g.subqCount()*PredicateSubqueryScale) {
		sub := g.GenerateSubquery(tables, subqDepth-1)
		if sub != nil {
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
			expr := Expr(InExpr{Left: leftExpr, List: []Expr{SubqueryExpr{Query: typedSub}}})
			if g.Config.Features.NotIn && util.Chance(g.Rand, g.Config.Weights.Features.NotInProb) {
				return UnaryExpr{Op: "NOT", Expr: expr}
			}
			return expr
		}
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
