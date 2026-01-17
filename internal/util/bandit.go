package util

import (
	"math"
	"math/rand"
)

type Bandit struct {
	counts      []int
	rewards     []float64
	total       int
	exploration float64
}

func NewBandit(arms int, exploration float64) *Bandit {
	if exploration <= 0 {
		exploration = 1.5
	}
	return &Bandit{
		counts:      make([]int, arms),
		rewards:     make([]float64, arms),
		exploration: exploration,
	}
}

func (b *Bandit) Pick(r *rand.Rand, enabled []bool) int {
	bestIdx := -1
	bestScore := -1.0
	for i := range b.counts {
		if enabled != nil && i < len(enabled) && !enabled[i] {
			continue
		}
		if b.counts[i] == 0 {
			return i
		}
		avg := b.rewards[i] / float64(b.counts[i])
		score := avg + b.exploration*math.Sqrt(math.Log(float64(b.total))/float64(b.counts[i]))
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}
	if bestIdx >= 0 {
		return bestIdx
	}
	if len(b.counts) == 0 {
		return 0
	}
	return r.Intn(len(b.counts))
}

func (b *Bandit) Update(arm int, reward float64) {
	if arm < 0 || arm >= len(b.counts) {
		return
	}
	b.counts[arm]++
	b.rewards[arm] += reward
	b.total++
}
