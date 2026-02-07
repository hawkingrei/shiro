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
}

func buildQueryWithSpec(gen *generator.Generator, spec QuerySpec) (*generator.SelectQuery, map[string]any) {
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
