package util

import "math/rand"

// RandIntRange returns a random int in [minValue, maxValue].
func RandIntRange(r *rand.Rand, minValue int, maxValue int) int {
	if maxValue <= minValue {
		return minValue
	}
	return minValue + r.Intn(maxValue-minValue+1)
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
