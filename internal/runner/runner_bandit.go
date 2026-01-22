package runner

import (
	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/util"
)

func (r *Runner) initBandits() {
	if !r.cfg.Adaptive.Enabled {
		return
	}
	if r.cfg.Adaptive.AdaptActions {
		r.actionBandit = util.NewBandit(3, r.cfg.Adaptive.UCBExploration)
		r.actionEnabled = []bool{
			r.cfg.Weights.Actions.DDL > 0,
			r.cfg.Weights.Actions.DML > 0,
			r.cfg.Weights.Actions.Query > 0,
		}
	}
	if r.cfg.Adaptive.AdaptOracles {
		r.oracleBandit = util.NewBandit(len(r.oracles), r.cfg.Adaptive.UCBExploration)
		r.oracleEnabled = []bool{
			r.cfg.Weights.Oracles.NoREC > 0,
			r.cfg.Weights.Oracles.TLP > 0,
			r.cfg.Weights.Oracles.DQP > 0,
			r.cfg.Weights.Oracles.CERT > 0,
			r.cfg.Weights.Oracles.CODDTest > 0,
			r.cfg.Weights.Oracles.DQE > 0,
		}
	}
	if r.cfg.Adaptive.AdaptDML {
		r.dmlBandit = util.NewBandit(3, r.cfg.Adaptive.UCBExploration)
		r.dmlEnabled = []bool{
			r.cfg.Weights.DML.Insert > 0,
			r.cfg.Weights.DML.Update > 0,
			r.cfg.Weights.DML.Delete > 0,
		}
	}
	if r.cfg.Adaptive.AdaptFeatures {
		r.featureBandit = newFeatureBandits(r.cfg)
	}
}

func (r *Runner) pickAction() int {
	if r.actionBandit != nil {
		return r.actionBandit.Pick(r.gen.Rand, r.actionEnabled)
	}
	return util.PickWeighted(r.gen.Rand, []int{r.cfg.Weights.Actions.DDL, r.cfg.Weights.Actions.DML, r.cfg.Weights.Actions.Query})
}

func (r *Runner) updateActionBandit(action int, reward float64) {
	if r.actionBandit != nil {
		r.actionBandit.Update(action, reward)
	}
}

func (r *Runner) pickOracle() int {
	if r.oracleBandit != nil {
		return r.oracleBandit.Pick(r.gen.Rand, r.oracleEnabled)
	}
	return util.PickWeighted(r.gen.Rand, []int{
		r.cfg.Weights.Oracles.NoREC,
		r.cfg.Weights.Oracles.TLP,
		r.cfg.Weights.Oracles.DQP,
		r.cfg.Weights.Oracles.CERT,
		r.cfg.Weights.Oracles.CODDTest,
		r.cfg.Weights.Oracles.DQE,
	})
}

func (r *Runner) updateOracleBandit(oracleIdx int, reward float64) {
	if r.oracleBandit != nil {
		r.oracleBandit.Update(oracleIdx, reward)
	}
}

func (r *Runner) pickDML() int {
	if r.dmlBandit != nil {
		return r.dmlBandit.Pick(r.gen.Rand, r.dmlEnabled)
	}
	return util.PickWeighted(r.gen.Rand, []int{r.cfg.Weights.DML.Insert, r.cfg.Weights.DML.Update, r.cfg.Weights.DML.Delete})
}

func (r *Runner) updateDMLBandit(choice int, reward float64) {
	if r.dmlBandit != nil {
		r.dmlBandit.Update(choice, reward)
	}
}

type featureBandits struct {
	joinBandit *util.Bandit
	subqBandit *util.Bandit
	aggBandit  *util.Bandit
	joinArms   []int
	subqArms   []int
	aggArms    []int
}

type featureArms struct {
	joinArm int
	subqArm int
	aggArm  int
}

func newFeatureBandits(cfg config.Config) *featureBandits {
	joinArms := makeArms(1, cfg.MaxJoinTables)
	subqMax := cfg.Weights.Features.SubqCount
	if subqMax < 1 {
		subqMax = 1
	}
	subqArms := makeArms(0, subqMax)
	aggArms := makeProbArms(cfg.Weights.Features.AggProb)
	return &featureBandits{
		joinBandit: util.NewBandit(len(joinArms), cfg.Adaptive.UCBExploration),
		subqBandit: util.NewBandit(len(subqArms), cfg.Adaptive.UCBExploration),
		aggBandit:  util.NewBandit(len(aggArms), cfg.Adaptive.UCBExploration),
		joinArms:   joinArms,
		subqArms:   subqArms,
		aggArms:    aggArms,
	}
}

func makeArms(minVal, maxVal int) []int {
	if maxVal < minVal {
		maxVal = minVal
	}
	arms := make([]int, 0, maxVal-minVal+1)
	for i := minVal; i <= maxVal; i++ {
		arms = append(arms, i)
	}
	return arms
}

func makeProbArms(base int) []int {
	if base < 0 {
		base = 0
	}
	arms := []int{0, base / 2, base, min(100, base*2)}
	return uniqueInts(arms)
}

func uniqueInts(values []int) []int {
	seen := map[int]struct{}{}
	out := make([]int, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func (r *Runner) prepareFeatureWeights() {
	if r.featureBandit == nil {
		return
	}
	r.lastFeatureArms.joinArm = r.featureBandit.joinBandit.Pick(r.gen.Rand, nil)
	r.lastFeatureArms.subqArm = r.featureBandit.subqBandit.Pick(r.gen.Rand, nil)
	r.lastFeatureArms.aggArm = r.featureBandit.aggBandit.Pick(r.gen.Rand, nil)
	r.setAdaptiveWeights(generator.AdaptiveWeights{
		JoinCount: r.featureBandit.joinArms[r.lastFeatureArms.joinArm],
		SubqCount: r.featureBandit.subqArms[r.lastFeatureArms.subqArm],
		AggProb:   r.featureBandit.aggArms[r.lastFeatureArms.aggArm],
	})
}

func (r *Runner) updateFeatureBandits(reward float64) {
	if r.featureBandit == nil {
		return
	}
	lastFeatures := r.gen.LastFeatures
	r.clearAdaptiveWeights()
	r.gen.LastFeatures = nil
	if lastFeatures == nil {
		return
	}
	r.featureBandit.joinBandit.Update(r.lastFeatureArms.joinArm, reward)
	r.featureBandit.subqBandit.Update(r.lastFeatureArms.subqArm, reward)
	r.featureBandit.aggBandit.Update(r.lastFeatureArms.aggArm, reward)
}
