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

// QueryAnalysis captures deterministic flags and structural properties for reuse.
type QueryAnalysis struct {
	Deterministic bool
	HasAggregate  bool
	HasWindow     bool
	HasSubquery   bool
	HasLimit      bool
	HasOrderBy    bool
	HasGroupBy    bool
	HasHaving     bool
	HasDistinct   bool
	HasCTE        bool
	HasSetOps     bool
	JoinCount     int
	JoinTypeSeq   string
	JoinGraphSig  string
}

// QueryFeatures captures structural properties of a query.
type QueryFeatures struct {
	JoinCount            int
	JoinTypeSeq          string
	JoinGraphSig         string
	HasSubquery          bool
	HasInSubquery        bool
	HasNotInSubquery     bool
	HasExistsSubquery    bool
	HasNotExistsSubquery bool
	HasInList            bool
	HasNotInList         bool
	HasAggregate         bool
	HasWindow            bool
	HasNaturalJoin       bool
	HasFullJoinEmulation bool
	// HasRecursiveCTE is true when the query owns a WITH RECURSIVE clause.
	HasRecursiveCTE        bool
	ViewCount              int
	PredicatePairsTotal    int64
	PredicatePairsJoin     int64
	SubqueryAllowed        bool
	SubqueryDisallowReason string
	SubqueryAttempts       int64
	SubqueryBuilt          int64
	SubqueryFailed         int64
}

// AnalyzeQuery summarizes a query for fast-path guards and shared checks.
func AnalyzeQuery(query *SelectQuery) QueryAnalysis {
	if query == nil {
		return QueryAnalysis{}
	}
	features := AnalyzeQueryFeatures(query)
	return AnalyzeQueryWithFeatures(query, features)
}

// AnalyzeQueryWithFeatures builds QueryAnalysis with precomputed features.
func AnalyzeQueryWithFeatures(query *SelectQuery, features QueryFeatures) QueryAnalysis {
	if query == nil {
		return QueryAnalysis{}
	}
	return QueryAnalysis{
		Deterministic: QueryDeterministic(query),
		HasAggregate:  features.HasAggregate,
		HasWindow:     features.HasWindow,
		HasSubquery:   features.HasSubquery,
		HasLimit:      query.Limit != nil,
		HasOrderBy:    len(query.OrderBy) > 0,
		HasGroupBy:    len(query.GroupBy) > 0,
		HasHaving:     query.Having != nil,
		HasDistinct:   query.Distinct,
		HasCTE:        len(query.With) > 0,
		HasSetOps:     len(query.SetOps) > 0,
		JoinCount:     features.JoinCount,
		JoinTypeSeq:   features.JoinTypeSeq,
		JoinGraphSig:  features.JoinGraphSig,
	}
}

func (g *Generator) setQueryAnalysis(query *SelectQuery) {
	if g == nil || query == nil {
		return
	}
	if query.Analysis == nil {
		analysis := AnalyzeQuery(query)
		query.Analysis = &analysis
		g.LastAnalysis = &analysis
		return
	}
	g.LastAnalysis = query.Analysis
}

func (g *Generator) setQueryAnalysisWithFeatures(query *SelectQuery, features QueryFeatures) {
	if g == nil || query == nil {
		return
	}
	analysis := AnalyzeQueryWithFeatures(query, features)
	query.Analysis = &analysis
	g.LastAnalysis = &analysis
}

// AnalyzeQueryFeatures summarizes a query for feature tracking.
func AnalyzeQueryFeatures(query *SelectQuery) QueryFeatures {
	if query == nil {
		return QueryFeatures{}
	}
	features := QueryFeatures{
		JoinCount:            len(query.From.Joins),
		JoinTypeSeq:          joinTypeSequence(query),
		JoinGraphSig:         joinGraphSignature(query),
		HasRecursiveCTE:      query.WithRecursive,
		HasFullJoinEmulation: query.FullJoinEmulation,
	}
	for _, op := range query.SetOps {
		if op.Query == nil {
			continue
		}
		mergeQueryFeatureFlags(&features, AnalyzeQueryFeatures(op.Query))
	}
	if query.From.BaseQuery != nil {
		features.HasSubquery = true
		mergeQueryFeatureFlags(&features, AnalyzeQueryFeatures(query.From.BaseQuery))
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
		if join.Natural {
			features.HasNaturalJoin = true
		}
		if join.TableQuery != nil {
			features.HasSubquery = true
			mergeQueryFeatureFlags(&features, AnalyzeQueryFeatures(join.TableQuery))
		}
		if join.On != nil && exprHasWindow(join.On) {
			features.HasWindow = true
		}
	}
	return features
}

func mergeQueryFeatureFlags(dst *QueryFeatures, src QueryFeatures) {
	dst.HasSubquery = dst.HasSubquery || src.HasSubquery
	dst.HasInSubquery = dst.HasInSubquery || src.HasInSubquery
	dst.HasNotInSubquery = dst.HasNotInSubquery || src.HasNotInSubquery
	dst.HasExistsSubquery = dst.HasExistsSubquery || src.HasExistsSubquery
	dst.HasNotExistsSubquery = dst.HasNotExistsSubquery || src.HasNotExistsSubquery
	dst.HasInList = dst.HasInList || src.HasInList
	dst.HasNotInList = dst.HasNotInList || src.HasNotInList
	dst.HasAggregate = dst.HasAggregate || src.HasAggregate
	dst.HasWindow = dst.HasWindow || src.HasWindow
	dst.HasNaturalJoin = dst.HasNaturalJoin || src.HasNaturalJoin
	dst.HasFullJoinEmulation = dst.HasFullJoinEmulation || src.HasFullJoinEmulation
	dst.HasRecursiveCTE = dst.HasRecursiveCTE || src.HasRecursiveCTE
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
		prefix := ""
		if join.Natural {
			prefix = "NATURAL "
		}
		parts = append(parts, prefix+string(join.Type))
	}
	return strings.Join(parts, "-")
}

func joinGraphSignature(query *SelectQuery) string {
	if query == nil {
		return ""
	}
	base := query.From.baseName()
	if base == "" {
		base = "base"
	}
	if len(query.From.Joins) == 0 {
		return base
	}
	parts := make([]string, 0, len(query.From.Joins)+1)
	parts = append(parts, base)
	for _, join := range query.From.Joins {
		prefix := ""
		if join.Natural {
			prefix = "NATURAL "
		}
		parts = append(parts, prefix+string(join.Type)+":"+join.tableName())
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
	case CompareSubqueryExpr:
		if ExprHasAggregate(e.Left) {
			return true
		}
		return exprHasAggregateQuery(e.Query)
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
	if query.Where != nil && ExprHasAggregate(query.Where) {
		return true
	}
	if query.Having != nil && ExprHasAggregate(query.Having) {
		return true
	}
	for _, expr := range query.GroupBy {
		if ExprHasAggregate(expr) {
			return true
		}
	}
	for _, ob := range query.OrderBy {
		if ExprHasAggregate(ob.Expr) {
			return true
		}
	}
	if query.From.BaseQuery != nil && exprHasAggregateQuery(query.From.BaseQuery) {
		return true
	}
	for _, join := range query.From.Joins {
		if join.TableQuery != nil && exprHasAggregateQuery(join.TableQuery) {
			return true
		}
		if join.On != nil && ExprHasAggregate(join.On) {
			return true
		}
	}
	for _, op := range query.SetOps {
		if op.Query != nil && exprHasAggregateQuery(op.Query) {
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
	case CompareSubqueryExpr:
		return true
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
	case CompareSubqueryExpr:
		if exprHasWindow(e.Left) {
			return true
		}
		return exprHasWindowQuery(e.Query)
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
		if join.TableQuery != nil && exprHasWindowQuery(join.TableQuery) {
			return true
		}
		if join.On != nil && exprHasWindow(join.On) {
			return true
		}
	}
	if query.From.BaseQuery != nil && exprHasWindowQuery(query.From.BaseQuery) {
		return true
	}
	for _, op := range query.SetOps {
		if op.Query != nil && exprHasWindowQuery(op.Query) {
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
	case CompareSubqueryExpr:
		inSub, notInSub := exprHasInSubquery(e.Left)
		qInSub, qNotInSub := exprHasInSubqueryQuery(e.Query)
		return inSub || qInSub, notInSub || qNotInSub
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
	case CompareSubqueryExpr:
		inList, notInList := exprHasInList(e.Left)
		qInList, qNotInList := exprHasInListQuery(e.Query)
		return inList || qInList, notInList || qNotInList
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
	for _, op := range query.SetOps {
		if op.Query == nil {
			continue
		}
		inList, notInList := exprHasInListQuery(op.Query)
		hasIn = hasIn || inList
		hasNotIn = hasNotIn || notInList
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
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
	if query.From.BaseQuery != nil {
		inList, notInList := exprHasInListQuery(query.From.BaseQuery)
		hasIn = hasIn || inList
		hasNotIn = hasNotIn || notInList
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	for _, join := range query.From.Joins {
		if join.TableQuery == nil {
			continue
		}
		inList, notInList := exprHasInListQuery(join.TableQuery)
		hasIn = hasIn || inList
		hasNotIn = hasNotIn || notInList
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
	case CompareSubqueryExpr:
		ex, notEx := exprHasExistsSubquery(e.Left)
		qEx, qNotEx := exprHasExistsSubqueryQuery(e.Query)
		return ex || qEx, notEx || qNotEx
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
	for _, op := range query.SetOps {
		if op.Query == nil {
			continue
		}
		ex, notEx := exprHasExistsSubqueryQuery(op.Query)
		hasEx = hasEx || ex
		hasNotEx = hasNotEx || notEx
		if hasEx && hasNotEx {
			return hasEx, hasNotEx
		}
	}
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
	if query.From.BaseQuery != nil {
		ex, notEx := exprHasExistsSubqueryQuery(query.From.BaseQuery)
		hasEx = hasEx || ex
		hasNotEx = hasNotEx || notEx
		if hasEx && hasNotEx {
			return hasEx, hasNotEx
		}
	}
	for _, join := range query.From.Joins {
		if join.TableQuery == nil {
			continue
		}
		ex, notEx := exprHasExistsSubqueryQuery(join.TableQuery)
		hasEx = hasEx || ex
		hasNotEx = hasNotEx || notEx
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
	for _, op := range query.SetOps {
		if op.Query == nil {
			continue
		}
		inSub, notInSub := exprHasInSubqueryQuery(op.Query)
		hasIn = hasIn || inSub
		hasNotIn = hasNotIn || notInSub
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
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
	if query.From.BaseQuery != nil {
		inSub, notInSub := exprHasInSubqueryQuery(query.From.BaseQuery)
		hasIn = hasIn || inSub
		hasNotIn = hasNotIn || notInSub
		if hasIn && hasNotIn {
			return hasIn, hasNotIn
		}
	}
	for _, join := range query.From.Joins {
		if join.TableQuery == nil {
			continue
		}
		inSub, notInSub := exprHasInSubqueryQuery(join.TableQuery)
		hasIn = hasIn || inSub
		hasNotIn = hasNotIn || notInSub
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
