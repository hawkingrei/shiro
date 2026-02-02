package runner

import "shiro/internal/generator"

func (r *Runner) applyOracleBias(oracleName string) func() {
	snapshot := r.adaptiveSnapshot()
	weights := generator.AdaptiveWeights{
		JoinCount:       -1,
		SubqCount:       -1,
		AggProb:         -1,
		IndexPrefixProb: -1,
		GroupByOrdProb:  -1,
	}
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
	// Apply a scoped override for the current oracle execution.
	// The returned closure restores the previous adaptive weights (or clears them).
	r.setAdaptiveWeights(weights)
	if snapshot == nil {
		return r.clearAdaptiveWeights
	}
	return func() {
		r.setAdaptiveWeights(*snapshot)
	}
}
