package runner

import (
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestPlanCacheWarningReason(t *testing.T) {
	testCases := []struct {
		name    string
		warning string
		expect  string
	}{
		{
			name:    "prepared skip reason",
			warning: "Warning:1105:skip plan-cache: sub-queries are un-cacheable",
			expect:  "sub-queries are un-cacheable",
		},
		{
			name:    "non prepared skip reason",
			warning: "Warning:1105:skip non-prepared plan-cache: queries that have sub-queries are not supported",
			expect:  "queries that have sub-queries are not supported",
		},
		{
			name:    "plain warning message",
			warning: "Warning:1105:query has 'order by ?' is un-cacheable",
			expect:  "query has 'order by ?' is un-cacheable",
		},
		{
			name:    "empty warning message",
			warning: "Warning:1105:",
			expect:  "unknown",
		},
		{
			name:    "invalid warning format",
			warning: "invalid-warning",
			expect:  "invalid-warning",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := planCacheWarningReason(tc.warning)
			if got != tc.expect {
				t.Fatalf("warning reason mismatch: got=%q expect=%q", got, tc.expect)
			}
		})
	}
}

func TestObservePlanCacheWarnings(t *testing.T) {
	reasons := make(map[string]int)
	observePlanCacheWarnings(reasons, []string{
		"Warning:1105:skip plan-cache: sub-queries are un-cacheable",
		"Warning:1105:skip plan-cache: sub-queries are un-cacheable",
		"Warning:1105:query has 'order by ?' is un-cacheable",
	})
	if reasons["sub-queries are un-cacheable"] != 1 {
		t.Fatalf("unexpected count for subquery reason: %d", reasons["sub-queries are un-cacheable"])
	}
	if reasons["query has 'order by ?' is un-cacheable"] != 1 {
		t.Fatalf("unexpected count for order by reason: %d", reasons["query has 'order by ?' is un-cacheable"])
	}
	observePlanCacheWarnings(reasons, []string{
		"Warning:1105:skip plan-cache: sub-queries are un-cacheable",
	})
	if reasons["sub-queries are un-cacheable"] != 2 {
		t.Fatalf("unexpected count after second event: %d", reasons["sub-queries are un-cacheable"])
	}
}

func TestFormatPlanCacheWarningReasons(t *testing.T) {
	if got := formatPlanCacheWarningReasons(nil); got != "none" {
		t.Fatalf("unexpected nil map format: %q", got)
	}
	if got := formatPlanCacheWarningReasons(map[string]int{}); got != "none" {
		t.Fatalf("unexpected empty map format: %q", got)
	}
	got := formatPlanCacheWarningReasons(map[string]int{
		"b reason": 2,
		"a reason": 1,
	})
	if got != "a reason=1,b reason=2" {
		t.Fatalf("unexpected formatted reasons: %q", got)
	}
}

func TestPlanCacheMPPDisableStatements(t *testing.T) {
	got := planCacheMPPDisableStatements()
	want := []string{
		"SET SESSION tidb_allow_mpp=OFF",
		"SET SESSION tidb_enforce_mpp=OFF",
	}
	if len(got) != len(want) {
		t.Fatalf("unexpected statements count: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected statement at %d: got=%q want=%q", i, got[i], want[i])
		}
	}
}

func TestShouldIgnorePlanCacheMPPDisableError(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		expect bool
	}{
		{
			name:   "nil",
			err:    nil,
			expect: false,
		},
		{
			name:   "unknown system variable code",
			err:    &mysql.MySQLError{Number: 1193, Message: "Unknown system variable"},
			expect: true,
		},
		{
			name:   "unknown system variable text",
			err:    errors.New("Error 1105 (HY000): Unknown system variable 'tidb_enforce_mpp'"),
			expect: true,
		},
		{
			name:   "other mysql error",
			err:    &mysql.MySQLError{Number: 1064, Message: "syntax error"},
			expect: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldIgnorePlanCacheMPPDisableError(tc.err); got != tc.expect {
				t.Fatalf("unexpected result: got=%t expect=%t", got, tc.expect)
			}
		})
	}
}
