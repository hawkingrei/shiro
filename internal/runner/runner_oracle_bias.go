package runner

import "shiro/internal/generator"

func (r *Runner) applyOracleBias(oracleName string) bool {
	if r.adaptiveSnapshot() != nil {
		return false
	}
	weights := generator.AdaptiveWeights{}
	applied := false
	switch oracleName {
	case "DQP":
		weights.JoinCount = max(r.cfg.Weights.Features.JoinCount, 3)
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
		return false
	}
	r.setAdaptiveWeights(weights)
	return true
}
