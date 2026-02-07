package oracle

import "testing"

func TestSQLErrorReasonNil(t *testing.T) {
	reason, code := sqlErrorReason("norec", nil)
	if reason != "" {
		t.Fatalf("expected empty reason for nil error, got %q", reason)
	}
	if code != 0 {
		t.Fatalf("expected zero code for nil error, got %d", code)
	}
}
