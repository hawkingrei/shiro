package runner

import "testing"

func TestNormalizeSignatureValue(t *testing.T) {
	tests := []struct {
		raw   string
		scale int
		want  string
	}{
		{"1", 2, "1.00"},
		{"1.0", 2, "1.00"},
		{"1e0", 2, "1.00"},
		{"-1.2345", 2, "-1.23"},
		{"1.999", 0, "1.999"},
		{"scene", 2, "scene"},
		{"1e", 2, "1e"},
	}
	for _, tt := range tests {
		got := normalizeSignatureValue([]byte(tt.raw), tt.scale)
		if got != tt.want {
			t.Fatalf("normalizeSignatureValue(%q, %d)=%q, want %q", tt.raw, tt.scale, got, tt.want)
		}
	}
}

func TestLooksNumeric(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{"1", true},
		{"+1.2", true},
		{"-3e4", true},
		{"1e-3", true},
		{"e10", false},
		{"1e-", false},
		{"scene", false},
	}
	for _, tt := range cases {
		if got := looksNumeric(tt.raw); got != tt.want {
			t.Fatalf("looksNumeric(%q)=%v, want %v", tt.raw, got, tt.want)
		}
	}
}
