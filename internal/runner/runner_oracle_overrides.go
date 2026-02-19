package runner

import "shiro/internal/oracle"

func (r *Runner) applyOracleOverrides(name string) func() {
	origCfg := r.gen.Config
	origPred := r.gen.PredicateMode()
	origMinJoin := r.gen.MinJoinTables()
	origJoin, hadJoinOverride := r.gen.JoinTypeOverride()
	origDisallowScalar := r.gen.DisallowScalarSubquery()

	restore := func() {
		r.gen.Config = origCfg
		r.gen.SetPredicateMode(origPred)
		if hadJoinOverride {
			r.gen.SetJoinTypeOverride(origJoin)
		} else {
			r.gen.ClearJoinTypeOverride()
		}
		if origMinJoin > 0 {
			r.gen.SetMinJoinTables(origMinJoin)
		} else {
			r.gen.ClearMinJoinTables()
		}
		r.gen.SetDisallowScalarSubquery(origDisallowScalar)
	}

	profile := oracle.ProfileByName(name)
	if profile == nil {
		return restore
	}

	cfg := origCfg
	profile.Features.Apply(&cfg.Features)
	explicitNotExists := profile.Features.NotExists
	explicitNotIn := profile.Features.NotIn
	if profile.JoinOnPolicy != nil {
		cfg.Oracles.JoinOnPolicy = *profile.JoinOnPolicy
	}
	if profile.JoinUsingProbMin != nil && cfg.Oracles.JoinUsingProb < *profile.JoinUsingProbMin {
		cfg.Oracles.JoinUsingProb = *profile.JoinUsingProbMin
	}
	if profile.MaxJoinTables != nil && *profile.MaxJoinTables > 0 {
		cfg.MaxJoinTables = *profile.MaxJoinTables
	}
	if profile.AllowSubquery != nil {
		if *profile.AllowSubquery {
			cfg.Features.Subqueries = true
			cfg.Features.NotExists = true
			cfg.Features.NotIn = true
		} else {
			cfg.Features.Subqueries = false
			cfg.Features.NotExists = false
			cfg.Features.NotIn = false
		}
	}
	if explicitNotExists != nil {
		cfg.Features.NotExists = *explicitNotExists
	}
	if explicitNotIn != nil {
		cfg.Features.NotIn = *explicitNotIn
	}
	if r.isThroughputGuardActive() {
		cfg.Features.SetOperations = false
		cfg.Features.DerivedTables = false
		cfg.Features.QuantifiedSubqueries = false
		cfg.Features.WindowFuncs = false
		cfg.Features.WindowFrames = false
		if cfg.MaxJoinTables > 4 {
			cfg.MaxJoinTables = 4
		}
	}
	r.gen.Config = cfg

	if profile.PredicateMode != nil {
		r.gen.SetPredicateMode(*profile.PredicateMode)
	}
	if profile.JoinTypeOverride != nil {
		r.gen.SetJoinTypeOverride(*profile.JoinTypeOverride)
	}
	if profile.MinJoinTables != nil {
		r.gen.SetMinJoinTables(*profile.MinJoinTables)
	}
	if profile.DisallowScalarSubquery != nil {
		r.gen.SetDisallowScalarSubquery(*profile.DisallowScalarSubquery)
	}
	return restore
}
