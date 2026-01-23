package impo

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pkg/errors"
	"math/rand"
	"reflect"
)

// checkRdMLikeU: return "": pass, otherwise error
func checkRdMLikeU(in *ast.PatternLikeOrIlikeExpr) string {
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
		if s, o_k := t.GetValue().(string); !o_k {
			return "in.Pattern: !test_driver.KindString"
		} else {
			o_k = false
			for _, c := range s {
				if c != '%' {
					o_k = true
					break
				}
			}
			if !o_k {
				return "in.Pattern: all '%'"
			}
		}
	}
	return ""
}

// addRdMLikeU: RdMLikeU, *ast.PatternLikeOrIlikeExpr: normal char -> '_'|'%',  '_' -> '%'
func (v *MutateVisitor) addRdMLikeU(in *ast.PatternLikeOrIlikeExpr, flag int) {
	if checkRdMLikeU(in) == "" {
		v.addCandidate(RdMLikeU, 1, in, flag)
	}
}

// doRdMLikeU: RdMLikeU, *ast.PatternLikeOrIlikeExpr: normal char -> '_'|'%',  '_' -> '%'
func doRdMLikeU(rootNode ast.Node, in ast.Node, seed int64) ([]byte, error) {
	rander := rand.New(rand.NewSource(seed))
	switch in.(type) {
	case *ast.PatternLikeOrIlikeExpr:
		like := in.(*ast.PatternLikeOrIlikeExpr)
		// check
		ck := checkRdMLikeU(like)
		if ck != "" {
			return nil, errors.New("[doRdMLikeU]check error " + ck)
		}
		// mutate
		// normal char -> '_'|'%',  '_' -> '%'
		oldPattern := like.Pattern
		newPattern := []byte(((oldPattern.(*test_driver.ValueExpr)).GetValue()).(string))
		for i, c := range newPattern {
			if c != '%' && rander.Intn(2) == 0 {
				newPattern[i] = '%'
			}
		}
		like.Pattern = &test_driver.ValueExpr{
			Datum: test_driver.NewDatum(string(newPattern)),
		}
		sql, err := restore(rootNode)
		if err != nil {
			return nil, errors.Wrap(err, "[doRdMLikeU]restore error")
		}
		// recover
		like.Pattern = oldPattern
		return sql, nil
	case nil:
		return nil, errors.New("[doRdMLikeU]type nil")
	default:
		return nil, errors.New("[doRdMLikeU]type default " + reflect.TypeOf(in).String())
	}
}
