package util

import "testing"

func TestIsLeapYear(t *testing.T) {
	cases := []struct {
		year int
		want bool
	}{
		{1900, false},
		{2000, true},
		{2023, false},
		{2024, true},
		{2025, false},
	}
	for _, c := range cases {
		if got := IsLeapYear(c.year); got != c.want {
			t.Fatalf("IsLeapYear(%d)=%v, want %v", c.year, got, c.want)
		}
	}
}

func TestDaysInMonth(t *testing.T) {
	if got := DaysInMonth(2023, 2); got != 28 {
		t.Fatalf("DaysInMonth(2023, 2)=%d, want 28", got)
	}
	if got := DaysInMonth(2024, 2); got != 29 {
		t.Fatalf("DaysInMonth(2024, 2)=%d, want 29", got)
	}
	if got := DaysInMonth(2024, 4); got != 30 {
		t.Fatalf("DaysInMonth(2024, 4)=%d, want 30", got)
	}
	if got := DaysInMonth(2024, 1); got != 31 {
		t.Fatalf("DaysInMonth(2024, 1)=%d, want 31", got)
	}
}
