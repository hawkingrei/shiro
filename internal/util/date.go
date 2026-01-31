package util

import "math/rand"

// RandIntRange returns a random int in [min, max].
func RandIntRange(r *rand.Rand, min int, max int) int {
	if max <= min {
		return min
	}
	return min + r.Intn(max-min+1)
}

// IsLeapYear reports whether year is a leap year.
func IsLeapYear(year int) bool {
	if year%400 == 0 {
		return true
	}
	if year%100 == 0 {
		return false
	}
	return year%4 == 0
}

// DaysInMonth returns the number of days for a given month in a year.
func DaysInMonth(year int, month int) int {
	switch month {
	case 2:
		if IsLeapYear(year) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	default:
		return 31
	}
}
