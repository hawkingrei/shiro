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

// MaxJoinCount limits the maximum number of joins.
func (b *SelectQueryBuilder) MaxJoinCount(n int) *SelectQueryBuilder {
	b.constraints.MaxJoinCount = n
	b.constraints.MaxJoinCountSet = true
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
	if c.PredicateMode != PredicateModeDefault {
		b.gen.SetPredicateMode(c.PredicateMode)
	}
	if c.DisallowSubquery {
		b.gen.Config.Features.Subqueries = false
		b.gen.SetDisallowScalarSubquery(true)
	}
	defer func() {
		b.gen.SetPredicateMode(origMode)
		b.gen.Config.Features.Subqueries = origSubqueries
		b.gen.SetDisallowScalarSubquery(origDisallowScalar)
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
		if c.RequireDeterministic && !QueryDeterministic(query) {
			lastReason = "constraint:nondeterministic"
			continue
		}
		if c.PredicateGuard != nil && query.Where != nil && !c.PredicateGuard(query.Where) {
			if !b.attachPredicate(query, c) || !c.PredicateGuard(query.Where) {
				lastReason = "constraint:predicate_guard"
				continue
			}
		}
		if c.DisallowSubquery && AnalyzeQueryFeatures(query).HasSubquery {
			lastReason = "constraint:subquery"
			continue
		}
		if c.DisallowAggregate && (AnalyzeQueryFeatures(query).HasAggregate || len(query.GroupBy) > 0 || query.Having != nil) {
			lastReason = "constraint:aggregate"
			continue
		}
		if c.MaxJoinCountSet && len(query.From.Joins) > c.MaxJoinCount {
			lastReason = "constraint:join_count"
			continue
		}
		if c.QueryGuard != nil && !c.QueryGuard(query) {
			lastReason = "constraint:query_guard"
			continue
		}
		return query, "", i + 1
	}
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
