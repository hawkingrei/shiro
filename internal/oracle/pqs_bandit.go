package oracle

import (
	"sync"

	"shiro/internal/generator"
	"shiro/internal/util"
)

type pqsPredicateArm int

const (
	pqsArmRectifyRandom pqsPredicateArm = iota
	pqsArmPivotSingle
	pqsArmPivotMulti
)

var pqsPredicateArmNames = []string{
	"rectify_random",
	"pivot_single",
	"pivot_multi",
}

type pqsBanditState struct {
	mu          sync.Mutex
	bandit      *util.Bandit
	window      int
	exploration float64
}

var globalPQSBandit = &pqsBanditState{}

func pqsPredicateArmName(arm pqsPredicateArm) string {
	idx := int(arm)
	if idx < 0 || idx >= len(pqsPredicateArmNames) {
		return "unknown"
	}
	return pqsPredicateArmNames[idx]
}

func pqsPickPredicateArm(gen *generator.Generator) (pqsPredicateArm, bool) {
	if gen == nil || !gen.Config.Adaptive.Enabled {
		return pqsArmRectifyRandom, false
	}
	bandit := globalPQSBandit.ensureBandit(gen)
	arm := bandit.Pick(gen.Rand, nil)
	return pqsPredicateArm(arm), true
}

func pqsUpdatePredicateArm(gen *generator.Generator, arm pqsPredicateArm, reward float64) {
	if gen == nil || !gen.Config.Adaptive.Enabled {
		return
	}
	bandit := globalPQSBandit.ensureBandit(gen)
	bandit.Update(int(arm), reward)
}

func (b *pqsBanditState) ensureBandit(gen *generator.Generator) *util.Bandit {
	window := gen.Config.Adaptive.WindowSize
	exploration := gen.Config.Adaptive.UCBExploration
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bandit == nil || b.window != window || b.exploration != exploration {
		b.window = window
		b.exploration = exploration
		b.bandit = util.NewBanditWithWindow(len(pqsPredicateArmNames), exploration, window)
	}
	return b.bandit
}

func pqsBanditReward(ok bool, err error, skipped bool) float64 {
	reward := 0.0
	if !ok {
		reward = 1.0
	}
	if err != nil && reward < 0.5 {
		reward = 0.5
	}
	if skipped {
		reward -= 0.2
	}
	if reward < 0 {
		reward = 0
	}
	return reward
}
