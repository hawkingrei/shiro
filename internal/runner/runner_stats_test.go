package runner

import (
	"reflect"
	"testing"
)

func TestCollectGroundTruthDSGMismatchReasons(t *testing.T) {
	delta := map[string]oracleFunnel{
		"GroundTruth": {
			SkipReasons: map[string]int64{
				"groundtruth:dsg_key_mismatch_right_key": 2,
				"groundtruth:dsg_key_mismatch_left_key":  1,
				"groundtruth:dsg_key_mismatch":           3,
				"groundtruth:dsg_key_mismatch:right_key": 4,
				"groundtruth:key_missing":                5,
			},
		},
	}
	got := collectGroundTruthDSGMismatchReasons(delta)
	want := map[string]int64{
		"right_key": 6,
		"left_key":  1,
		"unknown":   3,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected mismatch reasons: got=%v want=%v", got, want)
	}
}

func TestCollectGroundTruthDSGMismatchReasonsEmpty(t *testing.T) {
	if got := collectGroundTruthDSGMismatchReasons(nil); got != nil {
		t.Fatalf("expected nil for empty funnel, got %v", got)
	}
	if got := collectGroundTruthDSGMismatchReasons(map[string]oracleFunnel{}); got != nil {
		t.Fatalf("expected nil for empty map, got %v", got)
	}
	if got := collectGroundTruthDSGMismatchReasons(map[string]oracleFunnel{
		"GroundTruth": {SkipReasons: map[string]int64{"groundtruth:key_missing": 1}},
	}); got != nil {
		t.Fatalf("expected nil for no dsg mismatch reasons, got %v", got)
	}
}
