package oracle

import (
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestIsWhitelistedSQLError(t *testing.T) {
	cases := []struct {
		code     uint16
		expected bool
	}{
		{code: 1064, expected: true},
		{code: 1292, expected: true},
		{code: 1451, expected: true},
		{code: 1452, expected: true},
		{code: 1049, expected: false},
	}
	for _, tc := range cases {
		err := &mysql.MySQLError{Number: tc.code, Message: "boom"}
		_, ok := isWhitelistedSQLError(err)
		if ok != tc.expected {
			t.Fatalf("code %d expected=%v got=%v", tc.code, tc.expected, ok)
		}
	}
}
