package generator

import "strings"

// AdaptiveWeights overrides feature weights dynamically.
type AdaptiveWeights struct {
	JoinCount       int
	SubqCount       int
	AggProb         int
	IndexPrefixProb int
	GroupByOrdProb  int
}

// QueryFeatures captures structural properties of a query.
type QueryFeatures struct {
	JoinCount              int
	JoinTypeSeq            string
	JoinGraphSig           string
	HasSubquery            bool
	HasInSubquery          bool
	HasNotInSubquery       bool
	HasAggregate           bool
	ViewCount              int
	PredicatePairsTotal    int64
	PredicatePairsJoin     int64
	SubqueryAllowed        bool
	SubqueryDisallowReason string
	SubqueryAttempts       int64
	SubqueryBuilt          int64
	SubqueryFailed         int64
}

// AnalyzeQueryFeatures summarizes a query for feature tracking.
func AnalyzeQueryFeatures(query *SelectQuery) QueryFeatures {
	if query == nil {
		return QueryFeatures{}
	}
	features := QueryFeatures{
		JoinCount:    len(query.From.Joins),
		JoinTypeSeq:  joinTypeSequence(query),
		JoinGraphSig: joinGraphSignature(query),
	}
	for _, item := range query.Items {
		if ExprHasAggregate(item.Expr) {
			features.HasAggregate = true
		}
		if exprHasSubquery(item.Expr) {
			features.HasSubquery = true
		}
		inSub, notInSub := exprHasInSubquery(item.Expr)
		features.HasInSubquery = features.HasInSubquery || inSub
		features.HasNotInSubquery = features.HasNotInSubquery || notInSub
	}
	if query.Where != nil && exprHasSubquery(query.Where) {
		features.HasSubquery = true
	}
	if query.Where != nil {
		inSub, notInSub := exprHasInSubquery(query.Where)
		features.HasInSubquery = features.HasInSubquery || inSub
		features.HasNotInSubquery = features.HasNotInSubquery || notInSub
	}
	if query.Having != nil && exprHasSubquery(query.Having) {
		features.HasSubquery = true
	}
	if query.Having != nil {
		inSub, notInSub := exprHasInSubquery(query.Having)
		features.HasInSubquery = features.HasInSubquery || inSub
		features.HasNotInSubquery = features.HasNotInSubquery || notInSub
	}
	for _, expr := range query.GroupBy {
		if exprHasSubquery(expr) {
			features.HasSubquery = true
		}
		inSub, notInSub := exprHasInSubquery(expr)
		features.HasInSubquery = features.HasInSubquery || inSub
		features.HasNotInSubquery = features.HasNotInSubquery || notInSub
	}
	for _, ob := range query.OrderBy {
		if exprHasSubquery(ob.Expr) {
			features.HasSubquery = true
		}
		inSub, notInSub := exprHasInSubquery(ob.Expr)
		features.HasInSubquery = features.HasInSubquery || inSub
		features.HasNotInSubquery = features.HasNotInSubquery || notInSub
	}
	return features
}

func joinTypeSequence(query *SelectQuery) string {
	if query == nil {
		return ""
	}
	if len(query.From.Joins) == 0 {
		return "base"
	}
	parts := make([]string, 0, len(query.From.Joins))
	for _, join := range query.From.Joins {
		parts = append(parts, string(join.Type))
	}
	return strings.Join(parts, "-")
}

func joinGraphSignature(query *SelectQuery) string {
	if query == nil {
		return ""
	}
	base := query.From.BaseTable
	if base == "" {
		base = "base"
	}
	if len(query.From.Joins) == 0 {
		return base
	}
	parts := make([]string, 0, len(query.From.Joins)+1)
	parts = append(parts, base)
	for _, join := range query.From.Joins {
		parts = append(parts, string(join.Type)+":"+join.Table)
	}
	return strings.Join(parts, "->")
}

// ExprHasAggregate reports whether the expression tree contains aggregates.
func ExprHasAggregate(expr Expr) bool {
	switch e := expr.(type) {
	case FuncExpr:
		if isAggregateFunc(e.Name) {
			return true
		}
		for _, arg := range e.Args {
			if ExprHasAggregate(arg) {
				return true
			}
		}
		return false
	case UnaryExpr:
		return ExprHasAggregate(e.Expr)
	case BinaryExpr:
		return ExprHasAggregate(e.Left) || ExprHasAggregate(e.Right)
	case CaseExpr:
		for _, w := range e.Whens {
			if ExprHasAggregate(w.When) || ExprHasAggregate(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return ExprHasAggregate(e.Else)
		}
		return false
	case InExpr:
		if ExprHasAggregate(e.Left) {
			return true
		}
		for _, item := range e.List {
			if ExprHasAggregate(item) {
				return true
			}
		}
		return false
	case GroupByOrdinalExpr:
		if e.Expr == nil {
			return false
		}
		return ExprHasAggregate(e.Expr)
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
		if ExprHasAggregate(item.Expr) {
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
	case GroupByOrdinalExpr:
		if e.Expr == nil {
			return false
		}
		return exprHasSubquery(e.Expr)
	default:
		return false
	}
}

func exprHasInSubquery(expr Expr) (hasInSubquery bool, hasNotInSubquery bool) {
	switch e := expr.(type) {
	case nil:
		return false, false
	case InExpr:
		hasSub := false
		for _, item := range e.List {
			if exprHasSubquery(item) {
				hasSub = true
				break
			}
		}
		return hasSub, false
	case UnaryExpr:
		if strings.EqualFold(e.Op, "NOT") {
			if inner, ok := e.Expr.(InExpr); ok {
				hasSub := false
				for _, item := range inner.List {
					if exprHasSubquery(item) {
						hasSub = true
						break
					}
				}
				if hasSub {
					return false, true
				}
			}
		}
		return exprHasInSubquery(e.Expr)
	case BinaryExpr:
		lin, lnot := exprHasInSubquery(e.Left)
		rin, rnot := exprHasInSubquery(e.Right)
		return lin || rin, lnot || rnot
	case FuncExpr:
		for _, arg := range e.Args {
			inSub, notInSub := exprHasInSubquery(arg)
			if inSub || notInSub {
				return inSub, notInSub
			}
		}
		return false, false
	case CaseExpr:
		for _, w := range e.Whens {
			inSub, notInSub := exprHasInSubquery(w.When)
			if inSub || notInSub {
				return inSub, notInSub
			}
			inSub, notInSub = exprHasInSubquery(w.Then)
			if inSub || notInSub {
				return inSub, notInSub
			}
		}
		if e.Else != nil {
			return exprHasInSubquery(e.Else)
		}
		return false, false
	case SubqueryExpr:
		return exprHasInSubqueryQuery(e.Query)
	case ExistsExpr:
		return exprHasInSubqueryQuery(e.Query)
	case WindowExpr:
		for _, arg := range e.Args {
			inSub, notInSub := exprHasInSubquery(arg)
			if inSub || notInSub {
				return inSub, notInSub
			}
		}
		for _, part := range e.PartitionBy {
			inSub, notInSub := exprHasInSubquery(part)
			if inSub || notInSub {
				return inSub, notInSub
			}
		}
		for _, ob := range e.OrderBy {
			inSub, notInSub := exprHasInSubquery(ob.Expr)
			if inSub || notInSub {
				return inSub, notInSub
			}
		}
		return false, false
	case GroupByOrdinalExpr:
		if e.Expr == nil {
			return false, false
		}
		return exprHasInSubquery(e.Expr)
	default:
		return false, false
	}
}

func exprHasInSubqueryQuery(query *SelectQuery) (hasInSubquery bool, hasNotInSubquery bool) {
	if query == nil {
		return false, false
	}
	for _, item := range query.Items {
		inSub, notInSub := exprHasInSubquery(item.Expr)
		if inSub || notInSub {
			return inSub, notInSub
		}
	}
	if query.Where != nil {
		inSub, notInSub := exprHasInSubquery(query.Where)
		if inSub || notInSub {
			return inSub, notInSub
		}
	}
	if query.Having != nil {
		inSub, notInSub := exprHasInSubquery(query.Having)
		if inSub || notInSub {
			return inSub, notInSub
		}
	}
	for _, expr := range query.GroupBy {
		inSub, notInSub := exprHasInSubquery(expr)
		if inSub || notInSub {
			return inSub, notInSub
		}
	}
	for _, ob := range query.OrderBy {
		inSub, notInSub := exprHasInSubquery(ob.Expr)
		if inSub || notInSub {
			return inSub, notInSub
		}
	}
	return false, false
}

func isAggregateFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}
