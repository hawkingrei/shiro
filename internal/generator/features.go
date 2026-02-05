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
	HasExistsSubquery      bool
	HasNotExistsSubquery   bool
	HasInList              bool
	HasNotInList           bool
	HasAggregate           bool
	HasWindow              bool
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
		if exprHasWindow(item.Expr) {
			features.HasWindow = true
		}
		if exprHasSubquery(item.Expr) {
			features.HasSubquery = true
		}
		inSub, notInSub := exprHasInSubquery(item.Expr)
		features.HasInSubquery = features.HasInSubquery || inSub
		features.HasNotInSubquery = features.HasNotInSubquery || notInSub
		existsSub, notExistsSub := exprHasExistsSubquery(item.Expr)
		features.HasExistsSubquery = features.HasExistsSubquery || existsSub
		features.HasNotExistsSubquery = features.HasNotExistsSubquery || notExistsSub
		inList, notInList := exprHasInList(item.Expr)
		features.HasInList = features.HasInList || inList
		features.HasNotInList = features.HasNotInList || notInList
	}
	if query.Where != nil {
		if exprHasWindow(query.Where) {
			features.HasWindow = true
		}
		if exprHasSubquery(query.Where) {
			features.HasSubquery = true
		}
		inSub, notInSub := exprHasInSubquery(query.Where)
		features.HasInSubquery = features.HasInSubquery || inSub
		features.HasNotInSubquery = features.HasNotInSubquery || notInSub
		existsSub, notExistsSub := exprHasExistsSubquery(query.Where)
		features.HasExistsSubquery = features.HasExistsSubquery || existsSub
		features.HasNotExistsSubquery = features.HasNotExistsSubquery || notExistsSub
		inList, notInList := exprHasInList(query.Where)
		features.HasInList = features.HasInList || inList
		features.HasNotInList = features.HasNotInList || notInList
	}
	if query.Having != nil {
		if exprHasWindow(query.Having) {
			features.HasWindow = true
		}
		if exprHasSubquery(query.Having) {
			features.HasSubquery = true
		}
		inSub, notInSub := exprHasInSubquery(query.Having)
		features.HasInSubquery = features.HasInSubquery || inSub
		features.HasNotInSubquery = features.HasNotInSubquery || notInSub
		existsSub, notExistsSub := exprHasExistsSubquery(query.Having)
		features.HasExistsSubquery = features.HasExistsSubquery || existsSub
		features.HasNotExistsSubquery = features.HasNotExistsSubquery || notExistsSub
		inList, notInList := exprHasInList(query.Having)
		features.HasInList = features.HasInList || inList
		features.HasNotInList = features.HasNotInList || notInList
	}
	for _, expr := range query.GroupBy {
		if exprHasWindow(expr) {
			features.HasWindow = true
		}
		if exprHasSubquery(expr) {
			features.HasSubquery = true
		}
		inSub, notInSub := exprHasInSubquery(expr)
		features.HasInSubquery = features.HasInSubquery || inSub
		features.HasNotInSubquery = features.HasNotInSubquery || notInSub
		existsSub, notExistsSub := exprHasExistsSubquery(expr)
		features.HasExistsSubquery = features.HasExistsSubquery || existsSub
		features.HasNotExistsSubquery = features.HasNotExistsSubquery || notExistsSub
		inList, notInList := exprHasInList(expr)
		features.HasInList = features.HasInList || inList
		features.HasNotInList = features.HasNotInList || notInList
	}
	for _, ob := range query.OrderBy {
		if exprHasWindow(ob.Expr) {
			features.HasWindow = true
		}
		if exprHasSubquery(ob.Expr) {
			features.HasSubquery = true
		}
		inSub, notInSub := exprHasInSubquery(ob.Expr)
		features.HasInSubquery = features.HasInSubquery || inSub
		features.HasNotInSubquery = features.HasNotInSubquery || notInSub
		existsSub, notExistsSub := exprHasExistsSubquery(ob.Expr)
		features.HasExistsSubquery = features.HasExistsSubquery || existsSub
		features.HasNotExistsSubquery = features.HasNotExistsSubquery || notExistsSub
		inList, notInList := exprHasInList(ob.Expr)
		features.HasInList = features.HasInList || inList
		features.HasNotInList = features.HasNotInList || notInList
	}
	for _, join := range query.From.Joins {
		if join.On != nil && exprHasWindow(join.On) {
			features.HasWindow = true
		}
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

func exprHasWindow(expr Expr) bool {
	switch e := expr.(type) {
	case WindowExpr:
		return true
	case SubqueryExpr:
		return exprHasWindowQuery(e.Query)
	case ExistsExpr:
		return exprHasWindowQuery(e.Query)
	case UnaryExpr:
		return exprHasWindow(e.Expr)
	case BinaryExpr:
		return exprHasWindow(e.Left) || exprHasWindow(e.Right)
	case CaseExpr:
		for _, w := range e.Whens {
			if exprHasWindow(w.When) || exprHasWindow(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return exprHasWindow(e.Else)
		}
		return false
	case InExpr:
		if exprHasWindow(e.Left) {
			return true
		}
		for _, item := range e.List {
			if exprHasWindow(item) {
				return true
			}
		}
		return false
	case FuncExpr:
		for _, arg := range e.Args {
			if exprHasWindow(arg) {
				return true
			}
		}
		return false
	case GroupByOrdinalExpr:
		if e.Expr == nil {
			return false
		}
		return exprHasWindow(e.Expr)
	default:
		return false
	}
}

func exprHasWindowQuery(query *SelectQuery) bool {
	if query == nil {
		return false
	}
	for _, item := range query.Items {
		if exprHasWindow(item.Expr) {
			return true
		}
	}
	if query.Where != nil && exprHasWindow(query.Where) {
		return true
	}
	if query.Having != nil && exprHasWindow(query.Having) {
		return true
	}
	for _, expr := range query.GroupBy {
		if exprHasWindow(expr) {
			return true
		}
	}
	for _, ob := range query.OrderBy {
		if exprHasWindow(ob.Expr) {
			return true
		}
	}
	for _, join := range query.From.Joins {
		if join.On != nil && exprHasWindow(join.On) {
			return true
		}
	}
	return false
}

func exprHasInSubquery(expr Expr) (hasInSubquery bool, hasNotInSubquery bool) {
	switch e := expr.(type) {
	case nil:
		return false, false
	case InExpr:
		return inExprListHasSubquery(e.List), false
	case UnaryExpr:
		if strings.EqualFold(e.Op, "NOT") {
			if inner, ok := e.Expr.(InExpr); ok {
				if inExprListHasSubquery(inner.List) {
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
		hasInSub := false
		hasNotInSub := false
		for _, arg := range e.Args {
			inSub, notInSub := exprHasInSubquery(arg)
			if inSub {
				hasInSub = true
			}
			if notInSub {
				hasNotInSub = true
			}
			if hasInSub && hasNotInSub {
				break
			}
		}
		return hasInSub, hasNotInSub
	case CaseExpr:
		hasInSub := false
		hasNotInSub := false
		for _, w := range e.Whens {
			inSub, notInSub := exprHasInSubquery(w.When)
			if inSub {
				hasInSub = true
			}
			if notInSub {
				hasNotInSub = true
			}
			inSub, notInSub = exprHasInSubquery(w.Then)
			if inSub {
				hasInSub = true
			}
			if notInSub {
				hasNotInSub = true
			}
			if hasInSub && hasNotInSub {
				return hasInSub, hasNotInSub
			}
		}
		if e.Else != nil {
			inSub, notInSub := exprHasInSubquery(e.Else)
			hasInSub = hasInSub || inSub
			hasNotInSub = hasNotInSub || notInSub
		}
		return hasInSub, hasNotInSub
	case SubqueryExpr:
		return exprHasInSubqueryQuery(e.Query)
	case ExistsExpr:
		return exprHasInSubqueryQuery(e.Query)
	case WindowExpr:
		hasInSub := false
		hasNotInSub := false
		for _, arg := range e.Args {
			inSub, notInSub := exprHasInSubquery(arg)
			if inSub {
				hasInSub = true
			}
			if notInSub {
				hasNotInSub = true
			}
			if hasInSub && hasNotInSub {
				return hasInSub, hasNotInSub
			}
		}
		for _, part := range e.PartitionBy {
			inSub, notInSub := exprHasInSubquery(part)
			if inSub {
				hasInSub = true
			}
			if notInSub {
				hasNotInSub = true
			}
			if hasInSub && hasNotInSub {
				return hasInSub, hasNotInSub
			}
		}
		for _, ob := range e.OrderBy {
			inSub, notInSub := exprHasInSubquery(ob.Expr)
			if inSub {
				hasInSub = true
			}
			if notInSub {
				hasNotInSub = true
			}
			if hasInSub && hasNotInSub {
				return hasInSub, hasNotInSub
			}
		}
		return hasInSub, hasNotInSub
	case GroupByOrdinalExpr:
		if e.Expr == nil {
			return false, false
		}
		return exprHasInSubquery(e.Expr)
	default:
		return false, false
	}
}

func inExprListHasSubquery(list []Expr) bool {
	for _, item := range list {
		if exprHasSubquery(item) {
			return true
		}
	}
	return false
}

func exprHasInList(expr Expr) (hasInList bool, hasNotInList bool) {
	switch e := expr.(type) {
	case nil:
		return false, false
	case InExpr:
		if inExprListHasSubquery(e.List) {
			return false, false
		}
		return true, false
	case UnaryExpr:
		if strings.EqualFold(e.Op, "NOT") {
			if inner, ok := e.Expr.(InExpr); ok {
				if inExprListHasSubquery(inner.List) {
					return false, false
				}
				return false, true
			}
		}
		return exprHasInList(e.Expr)
	case BinaryExpr:
		lin, lnot := exprHasInList(e.Left)
		rin, rnot := exprHasInList(e.Right)
		return lin || rin, lnot || rnot
	case FuncExpr:
		hasIn := false
		hasNotIn := false
		for _, arg := range e.Args {
			inList, notInList := exprHasInList(arg)
			if inList {
				hasIn = true
			}
			if notInList {
				hasNotIn = true
			}
			if hasIn && hasNotIn {
				break
			}
		}
		return hasIn, hasNotIn
	case CaseExpr:
		hasIn := false
		hasNotIn := false
		for _, w := range e.Whens {
			inList, notInList := exprHasInList(w.When)
			if inList {
				hasIn = true
			}
			if notInList {
				hasNotIn = true
			}
			inList, notInList = exprHasInList(w.Then)
			if inList {
				hasIn = true
			}
			if notInList {
				hasNotIn = true
			}
			if hasIn && hasNotIn {
				return hasIn, hasNotIn
			}
		}
		if e.Else != nil {
			inList, notInList := exprHasInList(e.Else)
			hasIn = hasIn || inList
			hasNotIn = hasNotIn || notInList
		}
		return hasIn, hasNotIn
	case SubqueryExpr:
		return exprHasInListQuery(e.Query)
	case ExistsExpr:
		return exprHasInListQuery(e.Query)
	case WindowExpr:
		hasIn := false
		hasNotIn := false
		for _, arg := range e.Args {
			inList, notInList := exprHasInList(arg)
			if inList {
				hasIn = true
			}
			if notInList {
				hasNotIn = true
			}
			if hasIn && hasNotIn {
				return hasIn, hasNotIn
			}
		}
		for _, part := range e.PartitionBy {
			inList, notInList := exprHasInList(part)
			if inList {
				hasIn = true
			}
			if notInList {
				hasNotIn = true
			}
			if hasIn && hasNotIn {
				return hasIn, hasNotIn
			}
		}
		for _, ob := range e.OrderBy {
			inList, notInList := exprHasInList(ob.Expr)
			if inList {
				hasIn = true
			}
			if notInList {
				hasNotIn = true
			}
			if hasIn && hasNotIn {
				return hasIn, hasNotIn
			}
		}
		return hasIn, hasNotIn
	case GroupByOrdinalExpr:
		if e.Expr == nil {
			return false, false
		}
		return exprHasInList(e.Expr)
	default:
		return false, false
	}
}

func exprHasInListQuery(query *SelectQuery) (hasInList bool, hasNotInList bool) {
	if query == nil {
		return false, false
	}
	hasIn := false
	hasNotIn := false
	for _, item := range query.Items {
		inList, notInList := exprHasInList(item.Expr)
		if inList {
			hasIn = true
		}
		if notInList {
			hasNotIn = true
		}
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	if query.Where != nil {
		inList, notInList := exprHasInList(query.Where)
		if inList {
			hasIn = true
		}
		if notInList {
			hasNotIn = true
		}
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	if query.Having != nil {
		inList, notInList := exprHasInList(query.Having)
		if inList {
			hasIn = true
		}
		if notInList {
			hasNotIn = true
		}
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	for _, expr := range query.GroupBy {
		inList, notInList := exprHasInList(expr)
		if inList {
			hasIn = true
		}
		if notInList {
			hasNotIn = true
		}
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	for _, ob := range query.OrderBy {
		inList, notInList := exprHasInList(ob.Expr)
		if inList {
			hasIn = true
		}
		if notInList {
			hasNotIn = true
		}
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	return hasIn, hasNotIn
}

func exprHasExistsSubquery(expr Expr) (hasExists bool, hasNotExists bool) {
	switch e := expr.(type) {
	case nil:
		return false, false
	case ExistsExpr:
		return true, false
	case UnaryExpr:
		if strings.EqualFold(e.Op, "NOT") {
			if _, ok := e.Expr.(ExistsExpr); ok {
				return false, true
			}
		}
		return exprHasExistsSubquery(e.Expr)
	case BinaryExpr:
		lex, lnot := exprHasExistsSubquery(e.Left)
		rex, rnot := exprHasExistsSubquery(e.Right)
		return lex || rex, lnot || rnot
	case FuncExpr:
		hasEx := false
		hasNotEx := false
		for _, arg := range e.Args {
			ex, notEx := exprHasExistsSubquery(arg)
			if ex {
				hasEx = true
			}
			if notEx {
				hasNotEx = true
			}
			if hasEx && hasNotEx {
				break
			}
		}
		return hasEx, hasNotEx
	case CaseExpr:
		hasEx := false
		hasNotEx := false
		for _, w := range e.Whens {
			ex, notEx := exprHasExistsSubquery(w.When)
			if ex {
				hasEx = true
			}
			if notEx {
				hasNotEx = true
			}
			ex, notEx = exprHasExistsSubquery(w.Then)
			if ex {
				hasEx = true
			}
			if notEx {
				hasNotEx = true
			}
			if hasEx && hasNotEx {
				return hasEx, hasNotEx
			}
		}
		if e.Else != nil {
			ex, notEx := exprHasExistsSubquery(e.Else)
			hasEx = hasEx || ex
			hasNotEx = hasNotEx || notEx
		}
		return hasEx, hasNotEx
	case SubqueryExpr:
		return exprHasExistsSubqueryQuery(e.Query)
	case WindowExpr:
		hasEx := false
		hasNotEx := false
		for _, arg := range e.Args {
			ex, notEx := exprHasExistsSubquery(arg)
			if ex {
				hasEx = true
			}
			if notEx {
				hasNotEx = true
			}
			if hasEx && hasNotEx {
				return hasEx, hasNotEx
			}
		}
		for _, part := range e.PartitionBy {
			ex, notEx := exprHasExistsSubquery(part)
			if ex {
				hasEx = true
			}
			if notEx {
				hasNotEx = true
			}
			if hasEx && hasNotEx {
				return hasEx, hasNotEx
			}
		}
		for _, ob := range e.OrderBy {
			ex, notEx := exprHasExistsSubquery(ob.Expr)
			if ex {
				hasEx = true
			}
			if notEx {
				hasNotEx = true
			}
			if hasEx && hasNotEx {
				return hasEx, hasNotEx
			}
		}
		return hasEx, hasNotEx
	case GroupByOrdinalExpr:
		if e.Expr == nil {
			return false, false
		}
		return exprHasExistsSubquery(e.Expr)
	default:
		return false, false
	}
}

// ExprHasExistsSubquery reports whether the expression contains EXISTS / NOT EXISTS.
func ExprHasExistsSubquery(expr Expr) (hasExists bool, hasNotExists bool) {
	if expr == nil {
		return false, false
	}
	return exprHasExistsSubquery(expr)
}

func exprHasExistsSubqueryQuery(query *SelectQuery) (hasExists bool, hasNotExists bool) {
	if query == nil {
		return false, false
	}
	hasEx := false
	hasNotEx := false
	for _, item := range query.Items {
		ex, notEx := exprHasExistsSubquery(item.Expr)
		if ex {
			hasEx = true
		}
		if notEx {
			hasNotEx = true
		}
		if hasEx && hasNotEx {
			return hasEx, hasNotEx
		}
	}
	if query.Where != nil {
		ex, notEx := exprHasExistsSubquery(query.Where)
		if ex {
			hasEx = true
		}
		if notEx {
			hasNotEx = true
		}
		if hasEx && hasNotEx {
			return hasEx, hasNotEx
		}
	}
	if query.Having != nil {
		ex, notEx := exprHasExistsSubquery(query.Having)
		if ex {
			hasEx = true
		}
		if notEx {
			hasNotEx = true
		}
		if hasEx && hasNotEx {
			return hasEx, hasNotEx
		}
	}
	for _, expr := range query.GroupBy {
		ex, notEx := exprHasExistsSubquery(expr)
		if ex {
			hasEx = true
		}
		if notEx {
			hasNotEx = true
		}
		if hasEx && hasNotEx {
			return hasEx, hasNotEx
		}
	}
	for _, ob := range query.OrderBy {
		ex, notEx := exprHasExistsSubquery(ob.Expr)
		if ex {
			hasEx = true
		}
		if notEx {
			hasNotEx = true
		}
		if hasEx && hasNotEx {
			return hasEx, hasNotEx
		}
	}
	return hasEx, hasNotEx
}

func exprHasInSubqueryQuery(query *SelectQuery) (hasInSubquery bool, hasNotInSubquery bool) {
	if query == nil {
		return false, false
	}
	hasIn := false
	hasNotIn := false
	for _, item := range query.Items {
		inSub, notInSub := exprHasInSubquery(item.Expr)
		if inSub {
			hasIn = true
		}
		if notInSub {
			hasNotIn = true
		}
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	if query.Where != nil {
		inSub, notInSub := exprHasInSubquery(query.Where)
		if inSub {
			hasIn = true
		}
		if notInSub {
			hasNotIn = true
		}
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	if query.Having != nil {
		inSub, notInSub := exprHasInSubquery(query.Having)
		if inSub {
			hasIn = true
		}
		if notInSub {
			hasNotIn = true
		}
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	for _, expr := range query.GroupBy {
		inSub, notInSub := exprHasInSubquery(expr)
		if inSub {
			hasIn = true
		}
		if notInSub {
			hasNotIn = true
		}
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	for _, ob := range query.OrderBy {
		inSub, notInSub := exprHasInSubquery(ob.Expr)
		if inSub {
			hasIn = true
		}
		if notInSub {
			hasNotIn = true
		}
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	return hasIn, hasNotIn
}

func isAggregateFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}
