package runner

import (
	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/util"
)

const certSampleRate = 1e-6

func (r *Runner) initBandits() {
	if !r.cfg.Adaptive.Enabled {
		return
	}
	if r.cfg.Adaptive.AdaptActions {
		r.actionBandit = util.NewBanditWithWindow(3, r.cfg.Adaptive.UCBExploration, r.cfg.Adaptive.WindowSize)
		r.actionEnabled = []bool{
			r.cfg.Weights.Actions.DDL > 0,
			r.cfg.Weights.Actions.DML > 0,
			r.cfg.Weights.Actions.Query > 0,
		}
	}
	if r.cfg.Adaptive.AdaptOracles {
		if len(r.nonCertOracleIdx) > 0 {
			r.oracleBandit = util.NewBanditWithWindow(len(r.nonCertOracleIdx), r.cfg.Adaptive.UCBExploration, r.cfg.Adaptive.WindowSize)
			r.refreshOracleEnabled()
		}
	}
	if r.cfg.Adaptive.AdaptDML {
		r.dmlBandit = util.NewBanditWithWindow(3, r.cfg.Adaptive.UCBExploration, r.cfg.Adaptive.WindowSize)
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

func (r *Runner) oracleWeightByName(name string) int {
	switch name {
	case "NoREC":
		return r.cfg.Weights.Oracles.NoREC
	case "TLP":
		return r.cfg.Weights.Oracles.TLP
	case "DQP":
		return r.cfg.Weights.Oracles.DQP
	case "CODDTest":
		return r.cfg.Weights.Oracles.CODDTest
	case "DQE":
		return r.cfg.Weights.Oracles.DQE
	case "Impo":
		return r.cfg.Weights.Oracles.Impo
	case "GroundTruth":
		return r.cfg.Weights.Oracles.GroundTruth
	default:
		return 0
	}
}

func (r *Runner) nonCertWeights() []int {
	if len(r.nonCertOracleIdx) == 0 {
		return nil
	}
	weights := make([]int, 0, len(r.nonCertOracleIdx))
	for _, idx := range r.nonCertOracleIdx {
		weights = append(weights, r.oracleWeightByName(r.oracles[idx].Name()))
	}
	return weights
}

func (r *Runner) refreshOracleEnabled() {
	weights := r.nonCertWeights()
	if r.oracleEnabled == nil || len(r.oracleEnabled) != len(weights) {
		r.oracleEnabled = make([]bool, len(weights))
	}
	for i, w := range weights {
		r.oracleEnabled[i] = w > 0
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
	r.statsMu.Lock()
	r.oraclePickTotal++
	r.statsMu.Unlock()
	if r.certOracleIdx >= 0 && r.gen.Rand.Float64() < certSampleRate {
		r.statsMu.Lock()
		r.certPickTotal++
		r.statsMu.Unlock()
		return r.certOracleIdx
	}
	idx := r.pickNonCertOracle()
	if idx == r.certOracleIdx {
		r.statsMu.Lock()
		r.certPickTotal++
		r.statsMu.Unlock()
	}
	return idx
}

func (r *Runner) pickNonCertOracle() int {
	if len(r.nonCertOracleIdx) == 0 {
		return r.certOracleIdx
	}
	if r.oracleBandit != nil {
		r.statsMu.Lock()
		defer r.statsMu.Unlock()
		choice := r.oracleBandit.Pick(r.gen.Rand, r.oracleEnabled)
		if choice < 0 || choice >= len(r.nonCertOracleIdx) {
			return r.nonCertOracleIdx[0]
		}
		return r.nonCertOracleIdx[choice]
	}
	weights := r.nonCertWeights()
	if len(weights) == 0 {
		return r.nonCertOracleIdx[0]
	}
	choice := util.PickWeighted(r.gen.Rand, weights)
	if choice < 0 || choice >= len(r.nonCertOracleIdx) {
		return r.nonCertOracleIdx[0]
	}
	return r.nonCertOracleIdx[choice]
}

func (r *Runner) updateOracleBandit(oracleIdx int, reward float64) {
	if r.oracleBandit != nil {
		if banditIdx, ok := r.oracleBanditIndex[oracleIdx]; ok {
			r.oracleBandit.Update(banditIdx, reward)
		}
	}
}

func (r *Runner) updateOracleBanditFromFunnel(delta map[string]oracleFunnel) {
	if r.oracleBandit == nil || len(delta) == 0 {
		return
	}
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	for i, o := range r.oracles {
		stat, ok := delta[o.Name()]
		if !ok || stat.Runs == 0 {
			continue
		}
		banditIdx, ok := r.oracleBanditIndex[i]
		if !ok {
			continue
		}
		reward := float64(stat.Mismatches + stat.Panics)
		if stat.Errors > 0 {
			reward += 0.5 * float64(stat.Errors)
		}
		skipRatio := float64(stat.Skips) / float64(stat.Runs)
		reward -= 0.2 * skipRatio
		if reward < 0 {
			reward = 0
		}
		rewardPerRun := reward / float64(stat.Runs)
		r.oracleBandit.UpdateBatch(banditIdx, rewardPerRun, int(stat.Runs))
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
	joinBandit        *util.Bandit
	subqBandit        *util.Bandit
	aggBandit         *util.Bandit
	indexPrefixBandit *util.Bandit
	joinArms          []int
	subqArms          []int
	aggArms           []int
	indexPrefixArms   []int
}

type featureArms struct {
	joinArm        int
	subqArm        int
	aggArm         int
	indexPrefixArm int
}

func newFeatureBandits(cfg config.Config) *featureBandits {
	joinArms := makeArms(1, cfg.MaxJoinTables)
	subqMax := cfg.Weights.Features.SubqCount
	if subqMax < 1 {
		subqMax = 1
	}
	subqArms := makeArms(0, subqMax)
	aggArms := makeProbArms(cfg.Weights.Features.AggProb)
	indexPrefixArms := makeProbArms(cfg.Weights.Features.IndexPrefixProb)
	return &featureBandits{
		joinBandit:        util.NewBanditWithWindow(len(joinArms), cfg.Adaptive.UCBExploration, cfg.Adaptive.WindowSize),
		subqBandit:        util.NewBanditWithWindow(len(subqArms), cfg.Adaptive.UCBExploration, cfg.Adaptive.WindowSize),
		aggBandit:         util.NewBanditWithWindow(len(aggArms), cfg.Adaptive.UCBExploration, cfg.Adaptive.WindowSize),
		indexPrefixBandit: util.NewBanditWithWindow(len(indexPrefixArms), cfg.Adaptive.UCBExploration, cfg.Adaptive.WindowSize),
		joinArms:          joinArms,
		subqArms:          subqArms,
		aggArms:           aggArms,
		indexPrefixArms:   indexPrefixArms,
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
	r.lastFeatureArms.indexPrefixArm = r.featureBandit.indexPrefixBandit.Pick(r.gen.Rand, nil)
	r.setAdaptiveWeights(generator.AdaptiveWeights{
		JoinCount:       r.featureBandit.joinArms[r.lastFeatureArms.joinArm],
		SubqCount:       r.featureBandit.subqArms[r.lastFeatureArms.subqArm],
		AggProb:         r.featureBandit.aggArms[r.lastFeatureArms.aggArm],
		IndexPrefixProb: r.featureBandit.indexPrefixArms[r.lastFeatureArms.indexPrefixArm],
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
	r.featureBandit.indexPrefixBandit.Update(r.lastFeatureArms.indexPrefixArm, reward)
}
