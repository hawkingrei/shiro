package runner

import (
	"fmt"

	"shiro/internal/generator"
)

// KQE-lite: join coverage feedback based on generator-side join signatures.
// It prefers deeper joins when join shape/type coverage stalls.

type kqeState struct {
	seenJoinTypeSeq  map[string]struct{}
	seenJoinGraphSig map[string]struct{}
	noNewTypeSeq     int
	noNewGraphSig    int
	override         *generator.AdaptiveWeights
	overrideTTL      int
	lastOverride     string
}

const (
	kqeNoNewTypeSeqThreshold  = 6
	kqeNoNewGraphSigThreshold = 6
	kqeOverrideTTL            = 5
	kqeJoinCountMin           = 3
	kqeJoinCountMax           = 7
)

func newKQELiteState() *kqeState {
	return &kqeState{
		seenJoinTypeSeq:  make(map[string]struct{}),
		seenJoinGraphSig: make(map[string]struct{}),
	}
}

func (s *kqeState) observe(features generator.QueryFeatures) {
	if features.JoinCount < 2 {
		return
	}
	if features.JoinTypeSeq != "" {
		if _, ok := s.seenJoinTypeSeq[features.JoinTypeSeq]; !ok {
			s.seenJoinTypeSeq[features.JoinTypeSeq] = struct{}{}
			s.noNewTypeSeq = 0
		} else {
			s.noNewTypeSeq++
		}
	}
	if features.JoinGraphSig != "" {
		if _, ok := s.seenJoinGraphSig[features.JoinGraphSig]; !ok {
			s.seenJoinGraphSig[features.JoinGraphSig] = struct{}{}
			s.noNewGraphSig = 0
		} else {
			s.noNewGraphSig++
		}
	}
}

func (r *Runner) observeKQELite(features *generator.QueryFeatures) {
	if r.kqeState == nil || features == nil {
		return
	}
	r.kqeMu.Lock()
	r.kqeState.observe(*features)
	r.kqeMu.Unlock()
}

func (r *Runner) applyKQELiteWeights() bool {
	if r.kqeState == nil {
		return false
	}
	base := generator.AdaptiveWeights{
		JoinCount: r.cfg.Weights.Features.JoinCount,
		SubqCount: r.cfg.Weights.Features.SubqCount,
		AggProb:   r.cfg.Weights.Features.AggProb,
	}
	if snapshot := r.adaptiveSnapshot(); snapshot != nil {
		base = *snapshot
	}
	r.kqeMu.Lock()
	if r.kqeState.overrideTTL <= 0 {
		maxJoin := min(kqeJoinCountMax, r.cfg.MaxJoinTables)
		if maxJoin >= kqeJoinCountMin &&
			(r.kqeState.noNewTypeSeq >= kqeNoNewTypeSeqThreshold ||
				r.kqeState.noNewGraphSig >= kqeNoNewGraphSigThreshold) {
			minJoin := kqeJoinCountMin
			if minJoin > maxJoin {
				minJoin = maxJoin
			}
			r.genMu.Lock()
			joinCount := minJoin + r.gen.Rand.Intn(maxJoin-minJoin+1)
			r.genMu.Unlock()
			r.kqeState.override = &generator.AdaptiveWeights{JoinCount: joinCount}
			r.kqeState.overrideTTL = kqeOverrideTTL
			if r.cfg.Logging.Verbose {
				sig := fmt.Sprintf("join=%d ttl=%d", joinCount, r.kqeState.overrideTTL)
				if sig != r.kqeState.lastOverride {
					r.kqeState.lastOverride = sig
				}
			}
		}
	}
	if r.kqeState.override == nil || r.kqeState.overrideTTL <= 0 {
		r.kqeMu.Unlock()
		return false
	}
	override := *r.kqeState.override
	r.kqeMu.Unlock()
	if override.JoinCount > 0 {
		base.JoinCount = override.JoinCount
	}
	r.setAdaptiveWeights(base)
	return true
}

func (r *Runner) tickKQELite() {
	if r.kqeState == nil {
		return
	}
	r.kqeMu.Lock()
	if r.kqeState.overrideTTL > 0 {
		r.kqeState.overrideTTL--
		if r.kqeState.overrideTTL == 0 {
			r.kqeState.override = nil
		}
	}
	r.kqeMu.Unlock()
}
