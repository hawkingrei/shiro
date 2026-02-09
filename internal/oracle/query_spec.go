package oracle

import "shiro/internal/generator"

// QuerySpec describes generator constraints and predicate policies for an oracle.
type QuerySpec struct {
	Oracle              string
	Constraints         generator.SelectQueryConstraints
	PredicatePolicy     predicatePolicy
	PredicateGuard      bool
	MaxTries            int
	SkipReasonOverrides map[string]string
	Profile             *Profile
}

func buildQueryWithSpec(gen *generator.Generator, spec QuerySpec) (*generator.SelectQuery, map[string]any) {
	if spec.Profile != nil {
		applyProfileToSpec(&spec.Constraints, spec.Profile)
		if spec.Profile.PredicateMode != nil {
			spec.Constraints.PredicateMode = *spec.Profile.PredicateMode
		}
	}
	builder := generator.NewSelectQueryBuilder(gen).WithConstraints(spec.Constraints)
	if spec.MaxTries > 0 {
		builder.MaxTries(spec.MaxTries)
	}
	if spec.PredicateGuard {
		policy := spec.PredicatePolicy
		builder.PredicateGuard(func(expr generator.Expr) bool {
			return predicateMatches(expr, policy)
		})
	}
	query, reason, attempts := builder.BuildWithReason()
	if query == nil {
		skipReason := builderSkipReason(spec.Oracle, reason)
		if override, ok := spec.SkipReasonOverrides[reason]; ok {
			skipReason = override
		}
		return nil, map[string]any{
			"skip_reason":      skipReason,
			"builder_reason":   reason,
			"builder_attempts": attempts,
		}
	}
	return query, nil
}

func applyProfileToSpec(dst *generator.SelectQueryConstraints, profile *Profile) {
	if dst == nil || profile == nil {
		return
	}
	applyProfileFeatures(dst, profile.Features)
	if profile.AllowSubquery != nil && !*profile.AllowSubquery {
		dst.DisallowSubquery = true
	}
	if profile.MinJoinTables != nil {
		dst.MinJoinTables = *profile.MinJoinTables
		dst.MinJoinTablesSet = true
	}
}

func applyProfileFeatures(dst *generator.SelectQueryConstraints, overrides FeatureOverrides) {
	if dst == nil {
		return
	}
	if overrides.CTE != nil && !*overrides.CTE {
		dst.DisallowCTE = true
	}
	if overrides.SetOperations != nil && !*overrides.SetOperations {
		dst.DisallowSetOps = true
	}
	if overrides.Aggregates != nil && !*overrides.Aggregates {
		dst.DisallowAggregate = true
		dst.DisallowGroupBy = true
		dst.DisallowHaving = true
	}
	if overrides.GroupBy != nil && !*overrides.GroupBy {
		dst.DisallowGroupBy = true
	}
	if overrides.Having != nil && !*overrides.Having {
		dst.DisallowHaving = true
	}
	if overrides.Distinct != nil && !*overrides.Distinct {
		dst.DisallowDistinct = true
	}
	if overrides.OrderBy != nil && !*overrides.OrderBy {
		dst.DisallowOrderBy = true
	}
	if overrides.Limit != nil && !*overrides.Limit {
		dst.DisallowLimit = true
	}
	if overrides.WindowFuncs != nil && !*overrides.WindowFuncs {
		dst.DisallowWindow = true
	}
}
