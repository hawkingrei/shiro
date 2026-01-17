package generator

import "strings"

type AdaptiveWeights struct {
	JoinCount int
	SubqCount int
	AggProb   int
}

type QueryFeatures struct {
	JoinCount    int
	HasSubquery  bool
	HasAggregate bool
}

func AnalyzeQueryFeatures(query *SelectQuery) QueryFeatures {
	if query == nil {
		return QueryFeatures{}
	}
	features := QueryFeatures{
		JoinCount: len(query.From.Joins),
	}
	for _, item := range query.Items {
		if exprHasAggregate(item.Expr) {
			features.HasAggregate = true
		}
		if exprHasSubquery(item.Expr) {
			features.HasSubquery = true
		}
	}
	if query.Where != nil && exprHasSubquery(query.Where) {
		features.HasSubquery = true
	}
	if query.Having != nil && exprHasSubquery(query.Having) {
		features.HasSubquery = true
	}
	for _, expr := range query.GroupBy {
		if exprHasSubquery(expr) {
			features.HasSubquery = true
		}
	}
	for _, ob := range query.OrderBy {
		if exprHasSubquery(ob.Expr) {
			features.HasSubquery = true
		}
	}
	return features
}

func exprHasAggregate(expr Expr) bool {
	switch e := expr.(type) {
	case FuncExpr:
		if isAggregateFunc(e.Name) {
			return true
		}
		for _, arg := range e.Args {
			if exprHasAggregate(arg) {
				return true
			}
		}
		return false
	case UnaryExpr:
		return exprHasAggregate(e.Expr)
	case BinaryExpr:
		return exprHasAggregate(e.Left) || exprHasAggregate(e.Right)
	case CaseExpr:
		for _, w := range e.Whens {
			if exprHasAggregate(w.When) || exprHasAggregate(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return exprHasAggregate(e.Else)
		}
		return false
	case InExpr:
		if exprHasAggregate(e.Left) {
			return true
		}
		for _, item := range e.List {
			if exprHasAggregate(item) {
				return true
			}
		}
		return false
	case SubqueryExpr:
		return exprHasAggregateQuery(e.Query)
	case ExistsExpr:
		return exprHasAggregateQuery(e.Query)
	default:
		return false
	}
}

func exprHasAggregateQuery(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	for _, item := range query.Items {
		if exprHasAggregate(item.Expr) {
			return true
		}
	}
	return false
}

func exprHasSubquery(expr Expr) bool {
	switch e := expr.(type) {
	case SubqueryExpr:
		return true
	case ExistsExpr:
		return true
	case InExpr:
		for _, item := range e.List {
			if exprHasSubquery(item) {
				return true
			}
		}
		return exprHasSubquery(e.Left)
	case UnaryExpr:
		return exprHasSubquery(e.Expr)
	case BinaryExpr:
		return exprHasSubquery(e.Left) || exprHasSubquery(e.Right)
	case CaseExpr:
		for _, w := range e.Whens {
			if exprHasSubquery(w.When) || exprHasSubquery(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return exprHasSubquery(e.Else)
		}
		return false
	case FuncExpr:
		for _, arg := range e.Args {
			if exprHasSubquery(arg) {
				return true
			}
		}
		return false
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
