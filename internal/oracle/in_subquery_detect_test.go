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
		{sql: "SELECT 1 WHERE NOT (a IN (SELECT 1))", notInSubquery: true},
	}
	for _, c := range cases {
		inSub, notInSub := DetectInSubquerySQL(c.sql)
		if inSub != c.inSubquery || notInSub != c.notInSubquery {
			t.Fatalf("DetectInSubquerySQL(%q) = in:%v notIn:%v", c.sql, inSub, notInSub)
		}
	}
}

func TestDetectSubqueryFeaturesSQL(t *testing.T) {
	cases := []struct {
		sql           string
		inSubquery    bool
		notInSubquery bool
		inList        bool
		notInList     bool
		exists        bool
		notExists     bool
	}{
		{sql: "SELECT 1 WHERE a IN (SELECT 1)", inSubquery: true},
		{sql: "SELECT 1 WHERE a NOT IN (SELECT 1)", notInSubquery: true},
		{sql: "SELECT 1 WHERE a IN (1,2,3)", inList: true},
		{sql: "SELECT 1 WHERE a NOT IN (1,2,3)", notInList: true},
		{sql: "SELECT 1 WHERE EXISTS (SELECT 1)", exists: true},
		{sql: "SELECT 1 WHERE NOT EXISTS (SELECT 1)", notExists: true},
		{sql: "SELECT 1 WHERE a IN (SELECT 1) AND EXISTS (SELECT 1)", inSubquery: true, exists: true},
	}
	for _, c := range cases {
		features := DetectSubqueryFeaturesSQL(c.sql)
		if features.HasInSubquery != c.inSubquery ||
			features.HasNotInSubquery != c.notInSubquery ||
			features.HasInList != c.inList ||
			features.HasNotInList != c.notInList ||
			features.HasExistsSubquery != c.exists ||
			features.HasNotExists != c.notExists {
			t.Fatalf("DetectSubqueryFeaturesSQL(%q) = inSub:%v notInSub:%v inList:%v notInList:%v exists:%v notExists:%v",
				c.sql,
				features.HasInSubquery,
				features.HasNotInSubquery,
				features.HasInList,
				features.HasNotInList,
				features.HasExistsSubquery,
				features.HasNotExists,
			)
		}
	}
}

func TestShouldDetectSubqueryFeaturesSQL(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{sql: "SELECT 1", want: false},
		{sql: "select 1 where a in (select 1)", want: true},
		{sql: "SELECT 1 WHERE a IN (1,2,3)", want: true},
		{sql: "SELECT 1 WHERE a IN  (1,2,3)", want: true},
		{sql: "SELECT 1 WHERE a IN (SELECT 1)", want: true},
		{sql: "SELECT 1 WHERE a NOT IN  (SELECT 1)", want: true},
		{sql: "SELECT 1 WHERE NOT IN(1,2,3)", want: true},
		{sql: "SELECT 1 WHERE EXISTS (SELECT 1)", want: true},
		{sql: "SELECT 1 WHERE NOT  EXISTS (SELECT 1)", want: true},
		{sql: "SELECT 1 WHERE a IN\n(SELECT 1)", want: true},
		{sql: "INSERT INTO t VALUES (1)", want: false},
		{sql: "INSERT INTO t SELECT * FROM t2 WHERE a IN (SELECT 1)", want: true},
		{sql: "UPDATE t SET a = 1 WHERE b IN (SELECT 1)", want: true},
		{sql: "DELETE FROM t WHERE EXISTS (SELECT 1)", want: true},
		// Conservative matches: may return true for literals/comments/identifiers.
		{sql: "SELECT 'test IN query' FROM t", want: true},
		{sql: "SELECT 'EXISTS' FROM t", want: true},
		{sql: "SELECT 1 -- IN comment", want: true},
		{sql: "SELECT column_in FROM table_in", want: true},
	}
	for _, c := range cases {
		if got := ShouldDetectSubqueryFeaturesSQL(c.sql); got != c.want {
			t.Fatalf("ShouldDetectSubqueryFeaturesSQL(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}
