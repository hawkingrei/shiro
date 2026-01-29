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
	window      int
	histArms    []int
	histRewards []float64
	histPos     int
	histSize    int
}

// NewBandit creates a bandit with the given number of arms.
func NewBandit(arms int, exploration float64) *Bandit {
	return NewBanditWithWindow(arms, exploration, 0)
}

// NewBanditWithWindow creates a bandit with a sliding window of updates.
func NewBanditWithWindow(arms int, exploration float64, window int) *Bandit {
	if exploration <= 0 {
		exploration = 1.5
	}
	if window < 0 {
		window = 0
	}
	b := &Bandit{
		counts:      make([]int, arms),
		rewards:     make([]float64, arms),
		exploration: exploration,
		window:      window,
	}
	if window > 0 {
		b.histArms = make([]int, window)
		b.histRewards = make([]float64, window)
	}
	return b
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
	b.updateOne(arm, reward)
}

func (b *Bandit) updateOne(arm int, reward float64) {
	if arm < 0 || arm >= len(b.counts) {
		return
	}
	if b.window > 0 {
		if b.histSize == b.window {
			oldArm := b.histArms[b.histPos]
			oldReward := b.histRewards[b.histPos]
			if oldArm >= 0 && oldArm < len(b.counts) {
				b.counts[oldArm]--
				b.rewards[oldArm] -= oldReward
				b.total--
			}
		} else {
			b.histSize++
		}
		b.histArms[b.histPos] = arm
		b.histRewards[b.histPos] = reward
		b.histPos++
		if b.histPos >= b.window {
			b.histPos = 0
		}
	}
	b.counts[arm]++
	b.rewards[arm] += reward
	b.total++
}

// UpdateBatch records rewards for multiple runs of the same arm.
func (b *Bandit) UpdateBatch(arm int, rewardPerRun float64, runs int) {
	if runs <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if arm < 0 || arm >= len(b.counts) {
		return
	}
	if b.window == 0 {
		b.counts[arm] += runs
		b.rewards[arm] += rewardPerRun * float64(runs)
		b.total += runs
		return
	}
	samples := runs
	if samples > b.window {
		samples = b.window
	}
	for i := 0; i < samples; i++ {
		b.updateOne(arm, rewardPerRun)
	}
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
		Window:      b.window,
	}
}

// BanditSnapshot captures the exported bandit state.
type BanditSnapshot struct {
	Counts      []int     `json:"counts"`
	Rewards     []float64 `json:"rewards"`
	Total       int       `json:"total"`
	Exploration float64   `json:"exploration"`
	Window      int       `json:"window"`
}
