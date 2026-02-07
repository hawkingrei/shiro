package runner

import "testing"

func TestGroundTruthDSGMismatchReasonFromDetails(t *testing.T) {
	cases := []struct {
		name    string
		details map[string]any
		want    string
	}{
		{
			name: "direct detail",
			details: map[string]any{
				"groundtruth_dsg_mismatch_reason": "right_key",
			},
			want: "right_key",
		},
		{
			name: "skip reason underscore",
			details: map[string]any{
				"skip_reason": "groundtruth:dsg_key_mismatch_left_table",
			},
			want: "left_table",
		},
		{
			name: "skip reason colon",
			details: map[string]any{
				"skip_reason": "groundtruth:dsg_key_mismatch:right_key",
			},
			want: "right_key",
		},
		{
			name: "skip reason unknown",
			details: map[string]any{
				"skip_reason": "groundtruth:dsg_key_mismatch",
			},
			want: "unknown",
		},
		{
			name: "non dsg skip reason",
			details: map[string]any{
				"skip_reason": "groundtruth:key_missing",
			},
			want: "",
		},
		{
			name:    "nil details",
			details: nil,
			want:    "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := groundTruthDSGMismatchReasonFromDetails(tc.details)
			if got != tc.want {
				t.Fatalf("groundTruthDSGMismatchReasonFromDetails()=%q want=%q", got, tc.want)
			}
		})
	}
}
