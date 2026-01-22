// Package util provides shared helper utilities.
package util

import (
	"math"
	"math/rand"
	"sync"
)

// Bandit implements a simple UCB bandit.
type Bandit struct {
	mu          sync.Mutex
	counts      []int
	rewards     []float64
	total       int
	exploration float64
}

// NewBandit creates a bandit with the given number of arms.
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

// Pick selects an arm using UCB, respecting the enabled mask when provided.
func (b *Bandit) Pick(r *rand.Rand, enabled []bool) int {
	b.mu.Lock()
	defer b.mu.Unlock()
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

// Update records the reward for the chosen arm.
func (b *Bandit) Update(arm int, reward float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if arm < 0 || arm >= len(b.counts) {
		return
	}
	b.counts[arm]++
	b.rewards[arm] += reward
	b.total++
}

// Snapshot returns a copy of the bandit state.
func (b *Bandit) Snapshot() BanditSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BanditSnapshot{
		Counts:      append([]int{}, b.counts...),
		Rewards:     append([]float64{}, b.rewards...),
		Total:       b.total,
		Exploration: b.exploration,
	}
}

// BanditSnapshot captures the exported bandit state.
type BanditSnapshot struct {
	Counts      []int     `json:"counts"`
	Rewards     []float64 `json:"rewards"`
	Total       int       `json:"total"`
	Exploration float64   `json:"exploration"`
}
