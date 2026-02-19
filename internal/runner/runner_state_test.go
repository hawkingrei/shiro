package runner

import (
	"math/rand"
	"testing"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/tqs"
)

type stubWalker struct{}

func (stubWalker) WalkTables(_ *rand.Rand, _ int, _ float64) []string { return nil }
func (stubWalker) RecordPath(_ []string)                              {}

func TestApplyRuntimeTogglesClearsTQSStateWhenDisabled(t *testing.T) {
	gen := &generator.Generator{}
	gen.SetTQSWalker(stubWalker{})
	history := &tqs.History{}
	r := &Runner{
		cfg:            config.Config{TQS: config.TQSConfig{Enabled: true}},
		baseTQSEnabled: true,
		baseDQEWeight:  1,
		baseDSGEnabled: true,
		baseActions:    config.ActionWeights{},
		baseDMLWeights: config.DMLWeights{},
		gen:            gen,
		dbSeq:          1,
		tqsHistory:     history,
	}
	r.applyRuntimeToggles()
	if r.cfg.TQS.Enabled {
		t.Fatalf("expected TQS to be disabled in DQE round")
	}
	if r.tqsHistory != nil {
		t.Fatalf("expected TQS history to be cleared when TQS is disabled")
	}
	if r.gen.TQSWalker != nil {
		t.Fatalf("expected TQS walker to be detached when TQS is disabled")
	}
}

func TestApplyRuntimeTogglesKeepsTQSStateWhenEnabled(t *testing.T) {
	gen := &generator.Generator{}
	walker := stubWalker{}
	gen.SetTQSWalker(walker)
	history := &tqs.History{}
	r := &Runner{
		cfg:            config.Config{TQS: config.TQSConfig{Enabled: true}},
		baseTQSEnabled: true,
		baseDQEWeight:  1,
		baseDSGEnabled: true,
		baseActions:    config.ActionWeights{},
		baseDMLWeights: config.DMLWeights{},
		gen:            gen,
		dbSeq:          2,
		tqsHistory:     history,
	}
	r.applyRuntimeToggles()
	if !r.cfg.TQS.Enabled {
		t.Fatalf("expected TQS to stay enabled on non-DQE round")
	}
	if r.tqsHistory == nil {
		t.Fatalf("expected TQS history to stay attached")
	}
	if r.gen.TQSWalker == nil {
		t.Fatalf("expected TQS walker to stay attached")
	}
}
