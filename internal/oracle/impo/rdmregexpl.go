package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"

	"github.com/pkg/errors"
	"math/rand"
	"reflect"
)

// checkRdMRegExpL: return "": pass, otherwise error
func checkRdMRegExpL(in *ast.PatternRegexpExpr) string {
	if t, ok := (in.Expr).(*test_driver.ValueExpr); !ok {
		return "!(in.Expr).(*test_driver.ValueExpr)"
	} else {
		if _, o_k := t.GetValue().(string); !o_k {
			return "in.Expr: !test_driver.KindString"
		}
	}
	if t, ok := (in.Pattern).(*test_driver.ValueExpr); !ok {
		return "!(in.Pattern).(*test_driver.ValueExpr)"
	} else {
		if _, o_k := t.GetValue().(string); !o_k {
			return "in.Pattern: !test_driver.KindString"
		}
	}
	return ""
}

// addRdMRegExpL: RdMRegExpL, *ast.PatternRegexpExpr: '*' -> '+'|'?'
func (v *MutateVisitor) addRdMRegExpL(in *ast.PatternRegexpExpr, flag int) {
	if checkRdMRegExpL(in) == "" {
		v.addCandidate(RdMRegExpL, 0, in, flag)
	}
}

// doRdMRegExpL: RdMRegExpL, *ast.PatternRegexpExpr: '*' -> '+'|'?'
func doRdMRegExpL(rootNode ast.Node, in ast.Node, seed int64) ([]byte, error) {
	rander := rand.New(rand.NewSource(seed))
	switch in := in.(type) {
	case *ast.PatternRegexpExpr:
		// check
		ck := checkRdMRegExpL(in)
		if ck != "" {
			return nil, errors.New("[doRdMRegExpL]check error " + ck)
		}
		// mutate
		// '*' -> '+'|'?'
		oldPattern := in.Pattern
		newPattern := []byte(((oldPattern.(*test_driver.ValueExpr)).GetValue()).(string))
		for i, c := range newPattern {
			if (c == '*') && rander.Intn(2) == 0 {
				if rander.Intn(2) == 0 {
					newPattern[i] = '+'
				} else {
					newPattern[i] = '?'
				}
			}
		}
		in.Pattern = &test_driver.ValueExpr{
			Datum: test_driver.NewDatum(string(newPattern)),
		}
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doRdMRegExpL]restore error")
		}
		// recover
		in.Pattern = oldPattern
		return sql, nil
	case nil:
		return nil, errors.New("[doRdMRegExpL]type nil")
	default:
		return nil, errors.New("[doRdMRegExpL]type default " + reflect.TypeOf(in).String())
	}
}
