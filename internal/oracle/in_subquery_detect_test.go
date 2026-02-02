package oracle

import "testing"

func TestDetectInSubquerySQL(t *testing.T) {
	cases := []struct {
		sql           string
		inSubquery    bool
		notInSubquery bool
	}{
		{sql: "SELECT 1 WHERE a IN (SELECT 1)", inSubquery: true},
		{sql: "SELECT 1 WHERE a IN ((SELECT 1))", inSubquery: true},
		{sql: "SELECT 1 WHERE a NOT IN (SELECT 1)", notInSubquery: true},
		{sql: "SELECT 1 WHERE a NOT IN ((SELECT 1))", notInSubquery: true},
		{sql: "SELECT 1 WHERE a IN (1,2,3)", inSubquery: false},
		{sql: "SELECT 1 WHERE a NOT IN (1,2,3)", notInSubquery: false},
	}
	for _, c := range cases {
		inSub, notInSub := DetectInSubquerySQL(c.sql)
		if inSub != c.inSubquery || notInSub != c.notInSubquery {
			t.Fatalf("DetectInSubquerySQL(%q) = in:%v notIn:%v", c.sql, inSub, notInSub)
		}
	}
}
