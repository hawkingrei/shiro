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

func (r *Runner) setTemplateWeights(weights generator.TemplateWeights) {
	r.genMu.Lock()
	defer r.genMu.Unlock()
	r.gen.SetTemplateWeights(weights)
}

func (r *Runner) clearTemplateWeights() {
	r.genMu.Lock()
	defer r.genMu.Unlock()
	r.gen.ClearTemplateWeights()
}

func (r *Runner) templateSnapshot() *generator.TemplateWeights {
	r.genMu.Lock()
	defer r.genMu.Unlock()
	if r.gen.Template == nil {
		return nil
	}
	weights := *r.gen.Template
	return &weights
}
