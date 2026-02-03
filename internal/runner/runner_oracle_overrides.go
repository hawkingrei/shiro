package runner

import "shiro/internal/generator"

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

	cfg := origCfg
	maxInt := func(a, b int) int {
		if a > b {
			return a
		}
		return b
	}
	allowSubquery := true
	switch name {
	case "GroundTruth":
		cfg.Features.CTE = false
		cfg.Features.Aggregates = false
		cfg.Features.GroupBy = false
		cfg.Features.Having = false
		cfg.Features.Distinct = false
		cfg.Features.OrderBy = false
		cfg.Features.Limit = false
		cfg.Features.WindowFuncs = false
		cfg.Oracles.JoinOnPolicy = "simple"
		cfg.Oracles.JoinUsingProb = maxInt(cfg.Oracles.JoinUsingProb, 80)
		r.gen.SetPredicateMode(generator.PredicateModeNone)
		r.gen.SetJoinTypeOverride(generator.JoinInner)
		r.gen.SetMinJoinTables(2)
	case "Impo":
		cfg.Features.CTE = false
		r.gen.SetDisallowScalarSubquery(true)
	case "NoREC":
		cfg.Features.CTE = false
		cfg.Features.Aggregates = false
		cfg.Features.GroupBy = false
		cfg.Features.Having = false
		cfg.Features.Distinct = false
		cfg.Features.OrderBy = false
		cfg.Features.Limit = false
		cfg.Features.WindowFuncs = false
		r.gen.SetPredicateMode(generator.PredicateModeSimple)
	case "TLP":
		cfg.Features.CTE = false
		cfg.Features.Aggregates = false
		cfg.Features.GroupBy = false
		cfg.Features.Having = false
		cfg.Features.Distinct = false
		cfg.Features.OrderBy = false
		cfg.Features.Limit = false
		cfg.Features.WindowFuncs = false
		cfg.Oracles.JoinOnPolicy = "complex"
		r.gen.SetPredicateMode(generator.PredicateModeSimpleColumns)
	case "DQP":
		cfg.Features.CTE = false
		cfg.Features.Aggregates = false
		cfg.Features.GroupBy = false
		cfg.Features.Having = false
		cfg.Features.Distinct = false
		cfg.Features.Limit = false
		cfg.Features.WindowFuncs = false
		r.gen.SetPredicateMode(generator.PredicateModeSimpleColumns)
		r.gen.SetMinJoinTables(2)
	default:
		return restore
	}

	if allowSubquery {
		cfg.Features.Subqueries = true
		cfg.Features.NotExists = true
		cfg.Features.NotIn = true
	}
	r.gen.Config = cfg
	return restore
}
