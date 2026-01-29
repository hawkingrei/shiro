package oracle

import (
	"math"
	"math/rand"
	"sort"
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

func resetHintBandit() {
	globalHintBandit.mu.Lock()
	defer globalHintBandit.mu.Unlock()
	globalHintBandit.counts = make(map[string]int)
	globalHintBandit.rewards = make(map[string]float64)
	globalHintBandit.total = 0
	globalHintBandit.window = 0
	globalHintBandit.exploration = 0
	globalHintBandit.histPos = 0
	globalHintBandit.histSize = 0
	globalHintBandit.histHints = nil
	globalHintBandit.histRewards = nil
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

func pickHintsBandit(r *rand.Rand, candidates []string, limit int, window int, exploration float64) []string {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}
	if limit > len(candidates) {
		limit = len(candidates)
	}
	if r == nil {
		shuffled := append([]string{}, candidates...)
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		return shuffled[:limit]
	}
	globalHintBandit.ensureConfig(window, exploration)
	globalHintBandit.mu.Lock()
	defer globalHintBandit.mu.Unlock()

	unique := make([]string, 0, len(candidates))
	seenHint := make(map[string]struct{}, len(candidates))
	for _, hint := range candidates {
		if hint == "" {
			continue
		}
		if _, ok := seenHint[hint]; ok {
			continue
		}
		seenHint[hint] = struct{}{}
		unique = append(unique, hint)
	}
	if limit > len(unique) {
		limit = len(unique)
	}

	type scored struct {
		hint  string
		score float64
	}
	zeros := make([]string, 0, len(unique))
	scoredHints := make([]scored, 0, len(unique))
	for _, hint := range unique {
		count := globalHintBandit.counts[hint]
		if count == 0 {
			zeros = append(zeros, hint)
			continue
		}
		if globalHintBandit.total <= 0 {
			continue
		}
		avg := globalHintBandit.rewards[hint] / float64(count)
		score := avg + globalHintBandit.exploration*math.Sqrt(math.Log(float64(globalHintBandit.total))/float64(count))
		scoredHints = append(scoredHints, scored{hint: hint, score: score})
	}

	picked := make([]string, 0, limit)
	if len(zeros) > 0 {
		r.Shuffle(len(zeros), func(i, j int) { zeros[i], zeros[j] = zeros[j], zeros[i] })
		for i := 0; i < len(zeros) && len(picked) < limit; i++ {
			picked = append(picked, zeros[i])
		}
	}
	if len(picked) >= limit {
		return picked
	}
	sort.Slice(scoredHints, func(i, j int) bool { return scoredHints[i].score > scoredHints[j].score })
	for i := 0; i < len(scoredHints) && len(picked) < limit; i++ {
		picked = append(picked, scoredHints[i].hint)
	}
	return picked
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
