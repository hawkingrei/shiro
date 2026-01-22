// Package util provides shared helper utilities.
//revive:disable:var-naming // Package name follows project convention.
package util

import "math/rand"

// PickWeighted selects an index based on integer weights.
func PickWeighted(r *rand.Rand, weights []int) int {
	total := 0
	for _, w := range weights {
		if w > 0 {
			total += w
		}
	}
	if total == 0 {
		return r.Intn(len(weights))
	}
	roll := r.Intn(total)
	sum := 0
	for i, w := range weights {
		if w <= 0 {
			continue
		}
		sum += w
		if roll < sum {
			return i
		}
	}
	return len(weights) - 1
}

// Chance returns true with a given percent chance.
func Chance(r *rand.Rand, percent int) bool {
	if percent <= 0 {
		return false
	}
	if percent >= 100 {
		return true
	}
	return r.Intn(100) < percent
}
