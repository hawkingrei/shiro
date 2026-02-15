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
	JoinCount                     int
	JoinTypeSeq                   string
	JoinGraphSig                  string
	TemplateJoinPredicateStrategy string
	HasSetOperations              bool
	HasDerivedTables              bool
	HasQuantifiedSubqueries       bool
	HasSubquery                   bool
	HasInSubquery                 bool
	HasNotInSubquery              bool
	HasExistsSubquery             bool
	HasNotExistsSubquery          bool
	HasInList                     bool
	HasNotInList                  bool
	HasAggregate                  bool
	HasWindow                     bool
	HasWindowFrame                bool
	HasIntervalArith              bool
	HasNaturalJoin                bool
	HasFullJoinEmulation          bool
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
		JoinCount:                     len(query.From.Joins),
		JoinTypeSeq:                   joinTypeSequence(query),
		JoinGraphSig:                  joinGraphSignature(query),
		TemplateJoinPredicateStrategy: NormalizeTemplateJoinPredicateStrategy(query.TemplateJoinPredicateStrategy),
		HasSetOperations:              len(query.SetOps) > 0,
		HasDerivedTables:              query.From.BaseQuery != nil,
		HasRecursiveCTE:               query.WithRecursive,
		HasFullJoinEmulation:          query.FullJoinEmulation,
	}
	for _, cte := range query.With {
		observeSubqueryFeatures(&features, cte.Query, false)
	}
	if query.From.BaseQuery != nil {
		observeSubqueryFeatures(&features, query.From.BaseQuery, true)
	}
	for _, item := range query.Items {
		observeExprFeatures(&features, item.Expr)
	}
	observeExprFeatures(&features, query.Where)
	observeExprFeatures(&features, query.Having)
	for _, expr := range query.GroupBy {
		observeExprFeatures(&features, expr)
	}
	for _, ob := range query.OrderBy {
		observeExprFeatures(&features, ob.Expr)
	}
	for _, wd := range query.WindowDefs {
		if wd.Frame != nil {
			features.HasWindowFrame = true
		}
		for _, part := range wd.PartitionBy {
			observeExprFeatures(&features, part)
		}
		for _, ob := range wd.OrderBy {
			observeExprFeatures(&features, ob.Expr)
		}
	}
	for _, join := range query.From.Joins {
		if join.Natural {
			features.HasNaturalJoin = true
		}
		if join.TableQuery != nil {
			features.HasDerivedTables = true
			observeSubqueryFeatures(&features, join.TableQuery, true)
		}
		if join.On != nil {
			observeExprFeatures(&features, join.On)
		}
	}
	for _, op := range query.SetOps {
		observeSubqueryFeatures(&features, op.Query, false)
	}
	return features
}

func observeExprFeatures(features *QueryFeatures, expr Expr) {
	if features == nil || expr == nil {
		return
	}
	switch e := expr.(type) {
	case ColumnExpr, LiteralExpr, ParamExpr:
		return
	case GroupByOrdinalExpr:
		observeExprFeatures(features, e.Expr)
	case UnaryExpr:
		if strings.EqualFold(strings.TrimSpace(e.Op), "NOT") {
			switch inner := e.Expr.(type) {
			case ExistsExpr:
				features.HasNotExistsSubquery = true
				observeSubqueryFeatures(features, inner.Query, true)
				return
			case *ExistsExpr:
				features.HasNotExistsSubquery = true
				observeSubqueryFeatures(features, inner.Query, true)
				return
			case InExpr:
				observeInExprFeatures(features, inner, true)
				return
			case *InExpr:
				if inner != nil {
					observeInExprFeatures(features, *inner, true)
				}
				return
			}
		}
		observeExprFeatures(features, e.Expr)
	case BinaryExpr:
		observeExprFeatures(features, e.Left)
		observeExprFeatures(features, e.Right)
	case FuncExpr:
		if isAggregateFunc(e.Name) {
			features.HasAggregate = true
		}
		for _, arg := range e.Args {
			observeExprFeatures(features, arg)
		}
	case CaseExpr:
		for _, w := range e.Whens {
			observeExprFeatures(features, w.When)
			observeExprFeatures(features, w.Then)
		}
		observeExprFeatures(features, e.Else)
	case SubqueryExpr:
		observeSubqueryFeatures(features, e.Query, true)
	case *SubqueryExpr:
		if e != nil {
			observeSubqueryFeatures(features, e.Query, true)
		}
	case ExistsExpr:
		features.HasExistsSubquery = true
		observeSubqueryFeatures(features, e.Query, true)
	case *ExistsExpr:
		if e != nil {
			features.HasExistsSubquery = true
			observeSubqueryFeatures(features, e.Query, true)
		}
	case InExpr:
		observeInExprFeatures(features, e, false)
	case *InExpr:
		if e != nil {
			observeInExprFeatures(features, *e, false)
		}
	case CompareSubqueryExpr:
		features.HasQuantifiedSubqueries = true
		observeExprFeatures(features, e.Left)
		observeSubqueryFeatures(features, e.Query, true)
	case *CompareSubqueryExpr:
		if e != nil {
			features.HasQuantifiedSubqueries = true
			observeExprFeatures(features, e.Left)
			observeSubqueryFeatures(features, e.Query, true)
		}
	case WindowExpr:
		features.HasWindow = true
		if e.Frame != nil {
			features.HasWindowFrame = true
		}
		for _, arg := range e.Args {
			observeExprFeatures(features, arg)
		}
		for _, part := range e.PartitionBy {
			observeExprFeatures(features, part)
		}
		for _, ob := range e.OrderBy {
			observeExprFeatures(features, ob.Expr)
		}
	case *WindowExpr:
		if e != nil {
			features.HasWindow = true
			if e.Frame != nil {
				features.HasWindowFrame = true
			}
			for _, arg := range e.Args {
				observeExprFeatures(features, arg)
			}
			for _, part := range e.PartitionBy {
				observeExprFeatures(features, part)
			}
			for _, ob := range e.OrderBy {
				observeExprFeatures(features, ob.Expr)
			}
		}
	case IntervalExpr, *IntervalExpr:
		features.HasIntervalArith = true
	}
}

func observeSubqueryFeatures(features *QueryFeatures, query *SelectQuery, markSubquery bool) {
	if features == nil {
		return
	}
	if markSubquery {
		features.HasSubquery = true
	}
	if query == nil {
		return
	}
	mergeQueryFeatureFlags(features, AnalyzeQueryFeatures(query))
}

func observeInExprFeatures(features *QueryFeatures, expr InExpr, negated bool) {
	observeExprFeatures(features, expr.Left)
	hasSubquery := false
	for _, item := range expr.List {
		switch sub := item.(type) {
		case SubqueryExpr:
			hasSubquery = true
			observeSubqueryFeatures(features, sub.Query, true)
		case *SubqueryExpr:
			if sub != nil {
				hasSubquery = true
				observeSubqueryFeatures(features, sub.Query, true)
			}
		default:
			observeExprFeatures(features, item)
		}
	}
	if hasSubquery {
		if negated {
			features.HasNotInSubquery = true
		} else {
			features.HasInSubquery = true
		}
		return
	}
	if negated {
		features.HasNotInList = true
		return
	}
	features.HasInList = true
}

func mergeQueryFeatureFlags(dst *QueryFeatures, src QueryFeatures) {
	dst.HasSetOperations = dst.HasSetOperations || src.HasSetOperations
	dst.HasDerivedTables = dst.HasDerivedTables || src.HasDerivedTables
	dst.HasQuantifiedSubqueries = dst.HasQuantifiedSubqueries || src.HasQuantifiedSubqueries
	dst.HasSubquery = dst.HasSubquery || src.HasSubquery
	dst.HasInSubquery = dst.HasInSubquery || src.HasInSubquery
	dst.HasNotInSubquery = dst.HasNotInSubquery || src.HasNotInSubquery
	dst.HasExistsSubquery = dst.HasExistsSubquery || src.HasExistsSubquery
	dst.HasNotExistsSubquery = dst.HasNotExistsSubquery || src.HasNotExistsSubquery
	dst.HasInList = dst.HasInList || src.HasInList
	dst.HasNotInList = dst.HasNotInList || src.HasNotInList
	dst.HasAggregate = dst.HasAggregate || src.HasAggregate
	dst.HasWindow = dst.HasWindow || src.HasWindow
	dst.HasWindowFrame = dst.HasWindowFrame || src.HasWindowFrame
	dst.HasIntervalArith = dst.HasIntervalArith || src.HasIntervalArith
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

func isAggregateFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}
