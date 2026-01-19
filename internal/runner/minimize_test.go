package runner

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func TestSelectCandidatesKeepCTEWhenReferenced(t *testing.T) {
	sql := "WITH cte_0 AS (SELECT 1 AS c0) SELECT c0 FROM cte_0"
	p := parser.New()
	node, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected select stmt")
	}
	candidates := selectCandidates(p, sel)
	for _, cand := range candidates {
		upper := strings.ToUpper(cand)
		if strings.Contains(upper, "CTE_0") && !strings.Contains(upper, "WITH") {
			t.Fatalf("cte removed while still referenced: %s", cand)
		}
	}
}
