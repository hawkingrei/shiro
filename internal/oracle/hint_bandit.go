package oracle

import (
	"math"
	"math/rand"
	"sync"
)

type hintBandit struct {
	mu          sync.Mutex
	counts      map[string]int
	rewards     map[string]float64
	total       int
	exploration float64
	window      int
	histHints   []string
	histRewards []float64
	histPos     int
	histSize    int
}

var globalHintBandit = &hintBandit{
	counts:  make(map[string]int),
	rewards: make(map[string]float64),
}

func (b *hintBandit) ensureConfig(window int, exploration float64) {
	if exploration <= 0 {
		exploration = 1.5
	}
	if window < 0 {
		window = 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.window == window && b.exploration == exploration {
		return
	}
	b.window = window
	b.exploration = exploration
	b.counts = make(map[string]int)
	b.rewards = make(map[string]float64)
	b.total = 0
	b.histPos = 0
	b.histSize = 0
	if window > 0 {
		b.histHints = make([]string, window)
		b.histRewards = make([]float64, window)
	} else {
		b.histHints = nil
		b.histRewards = nil
	}
}

func pickHintBandit(r *rand.Rand, candidates []string, window int, exploration float64) string {
	if len(candidates) == 0 {
		return ""
	}
	globalHintBandit.ensureConfig(window, exploration)
	globalHintBandit.mu.Lock()
	defer globalHintBandit.mu.Unlock()

	zeroIdx := make([]int, 0, len(candidates))
	bestIdx := -1
	bestScore := -1.0
	for i, hint := range candidates {
		if hint == "" {
			continue
		}
		count := globalHintBandit.counts[hint]
		if count == 0 {
			zeroIdx = append(zeroIdx, i)
			continue
		}
		if globalHintBandit.total <= 0 {
			continue
		}
		avg := globalHintBandit.rewards[hint] / float64(count)
		score := avg + globalHintBandit.exploration*math.Sqrt(math.Log(float64(globalHintBandit.total))/float64(count))
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}
	if len(zeroIdx) > 0 {
		return candidates[zeroIdx[r.Intn(len(zeroIdx))]]
	}
	if bestIdx >= 0 {
		return candidates[bestIdx]
	}
	return candidates[r.Intn(len(candidates))]
}

func updateHintBandit(hint string, reward float64, window int, exploration float64) {
	if hint == "" {
		return
	}
	globalHintBandit.ensureConfig(window, exploration)
	globalHintBandit.mu.Lock()
	defer globalHintBandit.mu.Unlock()
	globalHintBandit.updateOneLocked(hint, reward)
}

func (b *hintBandit) updateOneLocked(hint string, reward float64) {
	if hint == "" {
		return
	}
	if b.window > 0 {
		if b.histSize == b.window {
			oldHint := b.histHints[b.histPos]
			oldReward := b.histRewards[b.histPos]
			if oldHint != "" {
				b.counts[oldHint]--
				b.rewards[oldHint] -= oldReward
				b.total--
			}
		} else {
			b.histSize++
		}
		b.histHints[b.histPos] = hint
		b.histRewards[b.histPos] = reward
		b.histPos++
		if b.histPos >= b.window {
			b.histPos = 0
		}
	}
	b.counts[hint]++
	b.rewards[hint] += reward
	b.total++
}
