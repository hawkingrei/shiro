package oracle

import "testing"

func TestImpoSeedSkipReason(t *testing.T) {
	cases := []struct {
		name            string
		sawEmpty        bool
		guardrailReason string
		want            string
	}{
		{
			name:            "prefer guardrail reason",
			sawEmpty:        true,
			guardrailReason: "nondeterministic",
			want:            "seed_guardrail:nondeterministic",
		},
		{
			name:            "empty query only",
			sawEmpty:        true,
			guardrailReason: "",
			want:            "empty_query",
		},
		{
			name:            "fallback seed guardrail",
			sawEmpty:        false,
			guardrailReason: "",
			want:            "seed_guardrail",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := impoSeedSkipReason(tc.sawEmpty, tc.guardrailReason)
			if got != tc.want {
				t.Fatalf("impoSeedSkipReason(%v,%q)=%q want=%q", tc.sawEmpty, tc.guardrailReason, got, tc.want)
			}
		})
	}
}
