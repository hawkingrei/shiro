package oracle

import (
	"math/rand"
	"testing"
)

func TestPickHintsBanditDistinct(t *testing.T) {
	resetHintBandit()
	r := rand.New(rand.NewSource(1))
	hints := []string{"h1", "h2", "h3"}
	picked := pickHintsBandit(r, hints, 2, 10, 1.5)
	if len(picked) != 2 {
		t.Fatalf("expected 2 hints, got %d", len(picked))
	}
	if picked[0] == picked[1] {
		t.Fatalf("expected distinct hints, got %v", picked)
	}
}

func TestPickHintsBanditPrefersHigherScore(t *testing.T) {
	resetHintBandit()
	r := rand.New(rand.NewSource(2))
	window := 0
	exploration := 1.5
	for i := 0; i < 5; i++ {
		updateHintBandit("good", 1, window, exploration)
		updateHintBandit("bad", 0, window, exploration)
	}
	picked := pickHintsBandit(r, []string{"good", "bad"}, 1, window, exploration)
	if len(picked) != 1 {
		t.Fatalf("expected 1 hint, got %d", len(picked))
	}
	if picked[0] != "good" {
		t.Fatalf("expected good, got %s", picked[0])
	}
}

func TestHintBanditWindowEvicts(t *testing.T) {
	resetHintBandit()
	window := 2
	exploration := 1.5
	updateHintBandit("a", 1, window, exploration)
	updateHintBandit("a", 1, window, exploration)
	updateHintBandit("b", 1, window, exploration)
	globalHintBandit.mu.Lock()
	defer globalHintBandit.mu.Unlock()
	if globalHintBandit.total != 2 {
		t.Fatalf("expected total 2, got %d", globalHintBandit.total)
	}
	if globalHintBandit.counts["a"]+globalHintBandit.counts["b"] != 2 {
		t.Fatalf("expected window counts to sum to 2")
	}
}
