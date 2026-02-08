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

	profile := oracle.OracleProfileByName(name)
	if profile == nil {
		return restore
	}

	cfg := origCfg
	profile.Features.Apply(&cfg.Features)
	if profile.JoinOnPolicy != nil {
		cfg.Oracles.JoinOnPolicy = *profile.JoinOnPolicy
	}
	if profile.JoinUsingProbMin != nil && cfg.Oracles.JoinUsingProb < *profile.JoinUsingProbMin {
		cfg.Oracles.JoinUsingProb = *profile.JoinUsingProbMin
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
