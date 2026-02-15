package runner

import (
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/schema"
)

func newTestRunnerForQPG(cfg config.Config) *Runner {
	state := &schema.State{}
	gen := generator.New(cfg, state, 1)
	return &Runner{
		cfg:      cfg,
		gen:      gen,
		state:    state,
		qpgState: newQPGState(cfg.QPG),
	}
}

func TestApplyQPGWeightsTriggersOverrideOnNoJoinThreshold(t *testing.T) {
	cfg := config.Config{
		MaxJoinTables: 8,
		Weights: config.Weights{
			Features: config.FeatureWeights{
				JoinCount:       2,
				SubqCount:       1,
				AggProb:         40,
				IndexPrefixProb: 20,
			},
		},
		QPG: config.QPGConfig{
			Enabled:                 true,
			NoJoinThreshold:         1,
			NoAggThreshold:          99,
			NoNewPlanThreshold:      99,
			NoNewOpSigThreshold:     99,
			NoNewShapeThreshold:     99,
			NoNewJoinTypeThreshold:  99,
			NoNewJoinOrderThreshold: 99,
			OverrideTTL:             2,
		},
	}
	r := newTestRunnerForQPG(cfg)
	r.qpgState.noJoin = 1

	applied := r.applyQPGWeights()
	if !applied {
		t.Fatalf("expected qpg override to be applied")
	}
	if r.qpgState.override == nil {
		t.Fatalf("expected qpg override to be created")
	}
	if r.qpgState.overrideTTL != 2 {
		t.Fatalf("unexpected qpg override ttl: %d", r.qpgState.overrideTTL)
	}
	if r.qpgState.override.JoinCount != 3 {
		t.Fatalf("unexpected qpg override join count: %d", r.qpgState.override.JoinCount)
	}
	adaptive := r.adaptiveSnapshot()
	if adaptive == nil {
		t.Fatalf("expected adaptive weights snapshot")
	}
	if adaptive.JoinCount != 3 {
		t.Fatalf("unexpected adaptive join count: %d", adaptive.JoinCount)
	}
}

func TestTickQPGClearsExpiredOverrides(t *testing.T) {
	cfg := config.Config{
		QPG: config.QPGConfig{
			Enabled: true,
		},
	}
	r := newTestRunnerForQPG(cfg)
	r.qpgState.override = &generator.AdaptiveWeights{JoinCount: 4}
	r.qpgState.overrideTTL = 1
	r.qpgState.templateOverride = &generator.TemplateWeights{EnabledProb: 60}
	r.qpgState.templateTTL = 1

	r.tickQPG()

	if r.qpgState.override != nil {
		t.Fatalf("expected qpg override cleared after ttl expires")
	}
	if r.qpgState.overrideTTL != 0 {
		t.Fatalf("expected qpg override ttl to reach zero, got %d", r.qpgState.overrideTTL)
	}
	if r.qpgState.templateOverride != nil {
		t.Fatalf("expected qpg template override cleared after ttl expires")
	}
	if r.qpgState.templateTTL != 0 {
		t.Fatalf("expected qpg template ttl to reach zero, got %d", r.qpgState.templateTTL)
	}
}
