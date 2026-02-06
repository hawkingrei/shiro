package generator

// SelectQueryBuilder builds SELECT queries under explicit constraints.
type SelectQueryBuilder struct {
	gen         *Generator
	constraints SelectQueryConstraints
}

// NewSelectQueryBuilder creates a builder bound to the generator.
func NewSelectQueryBuilder(gen *Generator) *SelectQueryBuilder {
	return &SelectQueryBuilder{gen: gen}
}

// WithConstraints replaces the builder constraints.
func (b *SelectQueryBuilder) WithConstraints(c SelectQueryConstraints) *SelectQueryBuilder {
	b.constraints = c
	return b
}

// RequireWhere ensures the query has a WHERE predicate.
func (b *SelectQueryBuilder) RequireWhere() *SelectQueryBuilder {
	b.constraints.RequireWhere = true
	return b
}

// PredicateMode sets the predicate generation mode.
func (b *SelectQueryBuilder) PredicateMode(mode PredicateMode) *SelectQueryBuilder {
	b.constraints.PredicateMode = mode
	return b
}

// PredicateGuard validates the generated WHERE predicate.
func (b *SelectQueryBuilder) PredicateGuard(guard func(Expr) bool) *SelectQueryBuilder {
	b.constraints.PredicateGuard = guard
	return b
}

// QueryGuard validates the generated query structure.
func (b *SelectQueryBuilder) QueryGuard(guard func(*SelectQuery) bool) *SelectQueryBuilder {
	b.constraints.QueryGuard = guard
	return b
}

// QueryGuardWithReason validates the generated query structure with a reason.
func (b *SelectQueryBuilder) QueryGuardWithReason(guard func(*SelectQuery) (bool, string)) *SelectQueryBuilder {
	b.constraints.QueryGuardReason = guard
	return b
}

// RequireDeterministic enforces deterministic expressions.
func (b *SelectQueryBuilder) RequireDeterministic() *SelectQueryBuilder {
	b.constraints.RequireDeterministic = true
	return b
}

// DisallowSubquery forbids subqueries in the generated query.
func (b *SelectQueryBuilder) DisallowSubquery() *SelectQueryBuilder {
	b.constraints.DisallowSubquery = true
	return b
}

// DisallowAggregate forbids aggregates in the generated query.
func (b *SelectQueryBuilder) DisallowAggregate() *SelectQueryBuilder {
	b.constraints.DisallowAggregate = true
	return b
}

// DisallowWindow forbids window functions in the generated query.
func (b *SelectQueryBuilder) DisallowWindow() *SelectQueryBuilder {
	b.constraints.DisallowWindow = true
	return b
}

// DisallowLimit forbids LIMIT in the generated query.
func (b *SelectQueryBuilder) DisallowLimit() *SelectQueryBuilder {
	b.constraints.DisallowLimit = true
	return b
}

// DisallowOrderBy forbids ORDER BY in the generated query.
func (b *SelectQueryBuilder) DisallowOrderBy() *SelectQueryBuilder {
	b.constraints.DisallowOrderBy = true
	return b
}

// DisallowDistinct forbids DISTINCT in the generated query.
func (b *SelectQueryBuilder) DisallowDistinct() *SelectQueryBuilder {
	b.constraints.DisallowDistinct = true
	return b
}

// DisallowGroupBy forbids GROUP BY in the generated query.
func (b *SelectQueryBuilder) DisallowGroupBy() *SelectQueryBuilder {
	b.constraints.DisallowGroupBy = true
	return b
}

// DisallowHaving forbids HAVING in the generated query.
func (b *SelectQueryBuilder) DisallowHaving() *SelectQueryBuilder {
	b.constraints.DisallowHaving = true
	return b
}

// DisallowCTE forbids WITH clauses in the generated query.
func (b *SelectQueryBuilder) DisallowCTE() *SelectQueryBuilder {
	b.constraints.DisallowCTE = true
	return b
}

// MaxJoinCount limits the maximum number of joins.
func (b *SelectQueryBuilder) MaxJoinCount(n int) *SelectQueryBuilder {
	b.constraints.MaxJoinCount = n
	b.constraints.MaxJoinCountSet = true
	return b
}

// MinJoinTables enforces a minimum number of tables in the query.
func (b *SelectQueryBuilder) MinJoinTables(n int) *SelectQueryBuilder {
	b.constraints.MinJoinTables = n
	b.constraints.MinJoinTablesSet = true
	return b
}

// MaxTries sets the maximum build attempts.
func (b *SelectQueryBuilder) MaxTries(n int) *SelectQueryBuilder {
	b.constraints.MaxTries = n
	return b
}

// Build generates a query that satisfies all constraints.
func (b *SelectQueryBuilder) Build() *SelectQuery {
	query, _, _ := b.BuildWithReason()
	return query
}

// BuildWithReason returns the query and the last failure reason if it cannot be built.
func (b *SelectQueryBuilder) BuildWithReason() (*SelectQuery, string, int) {
	if b == nil || b.gen == nil {
		return nil, "builder:nil", 0
	}
	c := b.constraints
	maxTries := c.MaxTries
	if maxTries <= 0 {
		maxTries = 5
	}

	origMode := b.gen.PredicateMode()
	origSubqueries := b.gen.Config.Features.Subqueries
	origDisallowScalar := b.gen.DisallowScalarSubquery()
	origDisallowConstraint := b.gen.subqueryConstraintDisallow
	origMinJoin := b.gen.MinJoinTables()
	if c.PredicateMode != PredicateModeDefault {
		b.gen.SetPredicateMode(c.PredicateMode)
	}
	if c.DisallowSubquery {
		b.gen.subqueryConstraintDisallow = true
		b.gen.Config.Features.Subqueries = false
		b.gen.SetDisallowScalarSubquery(true)
	}
	if c.MinJoinTablesSet {
		b.gen.SetMinJoinTables(c.MinJoinTables)
	}
	defer func() {
		b.gen.SetPredicateMode(origMode)
		b.gen.Config.Features.Subqueries = origSubqueries
		b.gen.SetDisallowScalarSubquery(origDisallowScalar)
		b.gen.subqueryConstraintDisallow = origDisallowConstraint
		if origMinJoin > 0 {
			b.gen.SetMinJoinTables(origMinJoin)
		} else {
			b.gen.ClearMinJoinTables()
		}
	}()

	lastReason := ""
	for i := 0; i < maxTries; i++ {
		query := b.gen.GenerateSelectQuery()
		if query == nil {
			lastReason = "constraint:empty_query"
			continue
		}
		if c.RequireWhere && query.Where == nil {
			if !b.attachPredicate(query, c) {
				lastReason = "constraint:no_where"
				continue
			}
		}
		if c.PredicateGuard != nil && query.Where != nil && !c.PredicateGuard(query.Where) {
			if !b.attachPredicate(query, c) || !c.PredicateGuard(query.Where) {
				lastReason = "constraint:predicate_guard"
				continue
			}
		}
		features := AnalyzeQueryFeatures(query)
		hasLimit := query.Limit != nil
		hasOrderBy := len(query.OrderBy) > 0
		hasDistinct := query.Distinct
		hasGroupBy := len(query.GroupBy) > 0
		hasHaving := query.Having != nil
		hasCTE := len(query.With) > 0
		if c.RequireDeterministic && !QueryDeterministic(query) {
			lastReason = "constraint:nondeterministic"
			continue
		}
		if c.DisallowLimit && hasLimit {
			lastReason = "constraint:limit"
			continue
		}
		if c.DisallowWindow && features.HasWindow {
			lastReason = "constraint:window"
			continue
		}
		if c.DisallowOrderBy && hasOrderBy {
			lastReason = "constraint:order_by"
			continue
		}
		if c.DisallowDistinct && hasDistinct {
			lastReason = "constraint:distinct"
			continue
		}
		if c.DisallowGroupBy && hasGroupBy {
			lastReason = "constraint:group_by"
			continue
		}
		if c.DisallowHaving && hasHaving {
			lastReason = "constraint:having"
			continue
		}
		if c.DisallowCTE && hasCTE {
			lastReason = "constraint:cte"
			continue
		}
		if c.DisallowSubquery && features.HasSubquery {
			lastReason = "constraint:subquery"
			continue
		}
		if c.DisallowAggregate && (features.HasAggregate || hasGroupBy || hasHaving) {
			lastReason = "constraint:aggregate"
			continue
		}
		if c.MaxJoinCountSet && features.JoinCount > c.MaxJoinCount {
			lastReason = "constraint:join_count"
			continue
		}
		if c.MinJoinTablesSet && features.JoinCount+1 < c.MinJoinTables {
			lastReason = "constraint:min_join_tables"
			continue
		}
		if c.QueryGuardReason != nil {
			if ok, reason := c.QueryGuardReason(query); !ok {
				if reason == "" {
					reason = "constraint:query_guard"
				}
				lastReason = reason
				continue
			}
		}
		if c.QueryGuard != nil && !c.QueryGuard(query) {
			lastReason = "constraint:query_guard"
			continue
		}
		attempts := i + 1
		b.gen.recordBuilderStats(attempts, "")
		b.gen.setQueryAnalysis(query)
		return query, "", attempts
	}
	b.gen.recordBuilderStats(maxTries, lastReason)
	return nil, lastReason, maxTries
}

func (b *SelectQueryBuilder) attachPredicate(query *SelectQuery, c SelectQueryConstraints) bool {
	if b == nil || b.gen == nil || query == nil {
		return false
	}
	tables := b.gen.TablesForQueryScope(query)
	if len(tables) == 0 {
		return false
	}
	var pred Expr
	switch c.PredicateMode {
	case PredicateModeSimple:
		pred = b.gen.GenerateSimplePredicate(tables, min(2, b.gen.maxDepth))
	case PredicateModeSimpleColumns:
		pred = b.gen.GenerateSimplePredicateColumns(tables, min(2, b.gen.maxDepth))
	case PredicateModeNone:
		return false
	default:
		allowSubquery := b.gen.Config.Features.Subqueries && !c.DisallowSubquery
		pred = b.gen.GeneratePredicate(tables, b.gen.maxDepth, allowSubquery, b.gen.maxSubqDepth)
	}
	if pred == nil {
		return false
	}
	if !b.gen.ValidateExprInQueryScope(pred, query) {
		return false
	}
	query.Where = pred
	return true
}

// QueryDeterministic reports whether a query only uses deterministic expressions.
func QueryDeterministic(query *SelectQuery) bool {
	if query == nil {
		return true
	}
	for _, item := range query.Items {
		if !item.Expr.Deterministic() {
			return false
		}
	}
	if query.Where != nil && !query.Where.Deterministic() {
		return false
	}
	if query.Having != nil && !query.Having.Deterministic() {
		return false
	}
	for _, expr := range query.GroupBy {
		if !expr.Deterministic() {
			return false
		}
	}
	for _, ob := range query.OrderBy {
		if !ob.Expr.Deterministic() {
			return false
		}
	}
	for _, join := range query.From.Joins {
		if join.On != nil && !join.On.Deterministic() {
			return false
		}
	}
	return true
}
