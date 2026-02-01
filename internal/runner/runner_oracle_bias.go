package runner

import "shiro/internal/generator"

func (r *Runner) applyOracleBias(oracleName string) func() {
	snapshot := r.adaptiveSnapshot()
	weights := generator.AdaptiveWeights{GroupByOrdProb: -1}
	if snapshot != nil {
		weights = *snapshot
	}
	applied := false
	switch oracleName {
	case "DQP":
		minJoin := max(r.cfg.Weights.Features.JoinCount, 3)
		weights.JoinCount = max(weights.JoinCount, minJoin)
		weights.SubqCount = 0
		weights.AggProb = 0
		applied = true
	case "NoREC":
		weights.SubqCount = 0
		weights.AggProb = 0
		applied = true
	case "GroundTruth":
		weights.SubqCount = 0
		weights.AggProb = 0
		applied = true
	}
	if !applied {
		return nil
	}
	r.setAdaptiveWeights(weights)
	if snapshot == nil {
		return r.clearAdaptiveWeights
	}
	return func() {
		r.setAdaptiveWeights(*snapshot)
	}
}
