package runner

import "shiro/internal/generator"

func (r *Runner) setAdaptiveWeights(weights generator.AdaptiveWeights) {
	r.genMu.Lock()
	defer r.genMu.Unlock()
	r.gen.SetAdaptiveWeights(weights)
}

func (r *Runner) clearAdaptiveWeights() {
	r.genMu.Lock()
	defer r.genMu.Unlock()
	r.gen.ClearAdaptiveWeights()
}

func (r *Runner) adaptiveSnapshot() *generator.AdaptiveWeights {
	r.genMu.Lock()
	defer r.genMu.Unlock()
	if r.gen.Adaptive == nil {
		return nil
	}
	weights := *r.gen.Adaptive
	return &weights
}

func (r *Runner) seedSnapshot() int64 {
	r.genMu.Lock()
	defer r.genMu.Unlock()
	return r.gen.Seed
}
