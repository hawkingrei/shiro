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
	histCounts  []int
	histPos     int
	histSize    int
	histTotal   int
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
		b.histCounts = make([]int, window)
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
	b.updateBatchLocked(arm, reward, 1)
}

// UpdateBatch records rewards for multiple runs of the same arm.
func (b *Bandit) UpdateBatch(arm int, rewardPerRun float64, runs int) {
	if runs <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.updateBatchLocked(arm, rewardPerRun, runs)
}

func (b *Bandit) updateBatchLocked(arm int, rewardPerRun float64, runs int) {
	if runs <= 0 {
		return
	}
	if arm < 0 || arm >= len(b.counts) {
		return
	}
	rewardTotal := rewardPerRun * float64(runs)
	if b.window == 0 {
		b.counts[arm] += runs
		b.rewards[arm] += rewardTotal
		b.total += runs
		return
	}
	b.appendHistory(arm, runs, rewardTotal)
	b.trimHistory()
}

func (b *Bandit) appendHistory(arm int, runs int, rewardTotal float64) {
	if b.window <= 0 {
		return
	}
	if b.histSize == b.window {
		b.evictAt(b.histPos, b.histCounts[b.histPos])
		b.histPos = (b.histPos + 1) % b.window
		b.histSize--
	}
	tail := (b.histPos + b.histSize) % b.window
	b.histArms[tail] = arm
	b.histCounts[tail] = runs
	b.histRewards[tail] = rewardTotal
	b.histSize++
	b.counts[arm] += runs
	b.rewards[arm] += rewardTotal
	b.total += runs
	b.histTotal += runs
}

func (b *Bandit) trimHistory() {
	if b.window <= 0 {
		return
	}
	for b.histSize > 0 && b.histTotal > b.window {
		excess := b.histTotal - b.window
		idx := b.histPos
		entryCount := b.histCounts[idx]
		if entryCount <= 0 {
			b.histPos = (b.histPos + 1) % b.window
			b.histSize--
			continue
		}
		if entryCount <= excess {
			b.evictAt(idx, entryCount)
			b.histPos = (b.histPos + 1) % b.window
			b.histSize--
			continue
		}
		b.evictAt(idx, excess)
		break
	}
}

func (b *Bandit) evictAt(idx int, removeCount int) {
	if removeCount <= 0 {
		return
	}
	entryCount := b.histCounts[idx]
	if entryCount <= 0 {
		return
	}
	arm := b.histArms[idx]
	entryReward := b.histRewards[idx]
	if removeCount >= entryCount {
		if arm >= 0 && arm < len(b.counts) {
			b.counts[arm] -= entryCount
			b.rewards[arm] -= entryReward
			b.total -= entryCount
		}
		b.histTotal -= entryCount
		b.histCounts[idx] = 0
		b.histRewards[idx] = 0
		b.histArms[idx] = 0
		return
	}
	perRun := entryReward / float64(entryCount)
	if arm >= 0 && arm < len(b.counts) {
		b.counts[arm] -= removeCount
		b.rewards[arm] -= perRun * float64(removeCount)
		b.total -= removeCount
	}
	b.histCounts[idx] -= removeCount
	b.histRewards[idx] -= perRun * float64(removeCount)
	b.histTotal -= removeCount
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
