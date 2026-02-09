package oracle

import (
	"strconv"
	"strings"

	"shiro/internal/generator"
	"shiro/internal/schema"
)

type pqsTruth int

const (
	pqsTruthUnknown pqsTruth = iota
	pqsTruthFalse
	pqsTruthTrue
	pqsTruthNull
)

type pqsValueKind int

const (
	pqsValueInvalid pqsValueKind = iota
	pqsValueNull
	pqsValueInt
	pqsValueFloat
	pqsValueString
	pqsValueBool
)

type pqsValue struct {
	Kind  pqsValueKind
	Int   int64
	Float float64
	Str   string
	Bool  bool
}

type pqsPredicateMeta struct {
	Original       string
	Rectified      string
	Reason         string
	Fallback       bool
	BanditEnabled  bool
	PredicateArm   int
	PredicateArmID string
}

func pqsBuildPredicate(gen *generator.Generator, pivot *pqsPivotRow) (generator.Expr, pqsPredicateMeta) {
	meta := pqsPredicateMeta{Fallback: true}
	if !pqsHasSafePredicateColumns(pivot) {
		meta.Reason = "predicate_no_safe_columns"
		return nil, meta
	}
	arm, banditEnabled := pqsPickPredicateArm(gen)
	meta.BanditEnabled = banditEnabled
	meta.PredicateArm = int(arm)
	meta.PredicateArmID = pqsPredicateArmName(arm)
	switch arm {
	case pqsArmPivotSingle:
		predicate := pqsPredicateForPivotWithRange(gen, pivot, 1, 1)
		if predicate == nil {
			meta.Reason = "strategy_pivot_single_empty"
			return nil, meta
		}
		meta.Original = buildExpr(predicate)
		meta.Rectified = meta.Original
		meta.Reason = "strategy_pivot_single"
		meta.Fallback = true
		return predicate, meta
	case pqsArmPivotMulti:
		predicate := pqsPredicateForPivotWithRange(gen, pivot, 2, pqsPredicateMaxCols)
		if predicate == nil {
			meta.Reason = "strategy_pivot_multi_empty"
			return nil, meta
		}
		meta.Original = buildExpr(predicate)
		meta.Rectified = meta.Original
		meta.Reason = "strategy_pivot_multi"
		meta.Fallback = true
		return predicate, meta
	default:
	}
	candidate := pqsRandomPredicate(gen, pivot)
	if candidate != nil {
		meta.Original = buildExpr(candidate)
		truth := pqsEvalExpr(candidate, pivot)
		rectified := pqsRectifyExpr(candidate, truth)
		switch truth {
		case pqsTruthTrue:
			meta.Reason = "rectify_true"
		case pqsTruthFalse:
			meta.Reason = "rectify_false"
		case pqsTruthNull:
			meta.Reason = "rectify_null"
		default:
			meta.Reason = "predicate_unsupported"
		}
		if rectified != nil && truth != pqsTruthUnknown {
			meta.Rectified = buildExpr(rectified)
			meta.Fallback = false
			return rectified, meta
		}
		if rectified == nil {
			meta.Reason = "rectify_failed"
		}
	}
	fallback := pqsPredicateForPivot(gen, pivot)
	if fallback == nil {
		if meta.Reason == "" {
			meta.Reason = "predicate_empty"
		}
		return nil, meta
	}
	if meta.Rectified == "" {
		meta.Rectified = buildExpr(fallback)
	}
	meta.Fallback = true
	return fallback, meta
}

func pqsRandomPredicate(gen *generator.Generator, pivot *pqsPivotRow) generator.Expr {
	if gen == nil || pivot == nil || len(pivot.Tables) == 0 {
		return nil
	}
	safeTables := pqsSafePredicateTables(pivot.Tables)
	if len(safeTables) == 0 {
		return nil
	}
	return gen.GenerateSimplePredicateColumns(safeTables, 2)
}

func pqsSafePredicateTables(tables []schema.Table) []schema.Table {
	if len(tables) == 0 {
		return nil
	}
	out := make([]schema.Table, 0, len(tables))
	for _, tbl := range tables {
		cols := make([]schema.Column, 0, len(tbl.Columns))
		for _, col := range tbl.Columns {
			if !pqsPredicateColumnAllowed(col) {
				continue
			}
			cols = append(cols, col)
		}
		if len(cols) == 0 {
			continue
		}
		copyTbl := tbl
		copyTbl.Columns = cols
		out = append(out, copyTbl)
	}
	return out
}

func pqsRectifyExpr(expr generator.Expr, truth pqsTruth) generator.Expr {
	switch truth {
	case pqsTruthTrue:
		return expr
	case pqsTruthFalse:
		return generator.UnaryExpr{Op: "NOT", Expr: expr}
	case pqsTruthNull:
		return generator.BinaryExpr{
			Left:  expr,
			Op:    "IS",
			Right: generator.LiteralExpr{Value: nil},
		}
	default:
		return nil
	}
}

func pqsEvalExpr(expr generator.Expr, pivot *pqsPivotRow) pqsTruth {
	switch e := expr.(type) {
	case generator.BinaryExpr:
		op := strings.ToUpper(strings.TrimSpace(e.Op))
		switch op {
		case "AND":
			return pqsAnd(pqsEvalExpr(e.Left, pivot), pqsEvalExpr(e.Right, pivot))
		case "OR":
			return pqsOr(pqsEvalExpr(e.Left, pivot), pqsEvalExpr(e.Right, pivot))
		case "IS", "IS NOT":
			left, ok := pqsEvalValue(e.Left, pivot)
			if !ok {
				return pqsTruthUnknown
			}
			right, ok := pqsEvalValue(e.Right, pivot)
			if !ok {
				return pqsTruthUnknown
			}
			return pqsEvalIs(left, right, op == "IS NOT")
		default:
			left, ok := pqsEvalValue(e.Left, pivot)
			if !ok {
				return pqsTruthUnknown
			}
			right, ok := pqsEvalValue(e.Right, pivot)
			if !ok {
				return pqsTruthUnknown
			}
			return pqsEvalCompare(left, right, op)
		}
	case generator.UnaryExpr:
		op := strings.ToUpper(strings.TrimSpace(e.Op))
		if op != "NOT" {
			return pqsTruthUnknown
		}
		return pqsNot(pqsEvalExpr(e.Expr, pivot))
	case generator.LiteralExpr:
		val, ok := pqsValueFromLiteral(e)
		if !ok {
			return pqsTruthUnknown
		}
		if val.Kind == pqsValueNull {
			return pqsTruthNull
		}
		if val.Kind == pqsValueBool {
			if val.Bool {
				return pqsTruthTrue
			}
			return pqsTruthFalse
		}
		return pqsTruthUnknown
	default:
		return pqsTruthUnknown
	}
}

func pqsEvalValue(expr generator.Expr, pivot *pqsPivotRow) (pqsValue, bool) {
	switch e := expr.(type) {
	case generator.ColumnExpr:
		return pqsValueFromPivot(e.Ref, pivot)
	case generator.LiteralExpr:
		return pqsValueFromLiteral(e)
	default:
		return pqsValue{}, false
	}
}

func pqsValueFromPivot(ref generator.ColumnRef, pivot *pqsPivotRow) (pqsValue, bool) {
	if ref.Table == "" || pivot == nil {
		return pqsValue{}, false
	}
	val, ok := pqsPivotValueFor(pivot, ref.Table, ref.Name)
	if !ok {
		return pqsValue{}, false
	}
	if val.Null {
		return pqsValue{Kind: pqsValueNull}, true
	}
	return pqsValueFromRaw(val.Column, val.Raw)
}

func pqsValueFromRaw(col schema.Column, raw string) (pqsValue, bool) {
	switch col.Type {
	case schema.TypeInt, schema.TypeBigInt:
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return pqsValue{Kind: pqsValueInt, Int: v}, true
		}
		return pqsValue{}, false
	case schema.TypeFloat, schema.TypeDouble:
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			return pqsValue{Kind: pqsValueFloat, Float: v}, true
		}
		return pqsValue{}, false
	case schema.TypeBool:
		lower := strings.ToLower(raw)
		if lower == "true" || lower == "false" {
			return pqsValue{Kind: pqsValueBool, Bool: lower == "true"}, true
		}
		if raw == "0" || raw == "1" {
			if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
				return pqsValue{Kind: pqsValueInt, Int: v}, true
			}
		}
		return pqsValue{}, false
	case schema.TypeDecimal, schema.TypeVarchar, schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return pqsValue{Kind: pqsValueString, Str: raw}, true
	default:
		return pqsValue{}, false
	}
}

func pqsValueFromLiteral(lit generator.LiteralExpr) (pqsValue, bool) {
	switch v := lit.Value.(type) {
	case nil:
		return pqsValue{Kind: pqsValueNull}, true
	case int:
		return pqsValue{Kind: pqsValueInt, Int: int64(v)}, true
	case int64:
		return pqsValue{Kind: pqsValueInt, Int: v}, true
	case float32:
		return pqsValue{Kind: pqsValueFloat, Float: float64(v)}, true
	case float64:
		return pqsValue{Kind: pqsValueFloat, Float: v}, true
	case bool:
		return pqsValue{Kind: pqsValueBool, Bool: v}, true
	case string:
		return pqsValue{Kind: pqsValueString, Str: v}, true
	default:
		return pqsValue{}, false
	}
}

func pqsEvalIs(left, right pqsValue, negated bool) pqsTruth {
	if right.Kind != pqsValueNull {
		return pqsTruthUnknown
	}
	match := left.Kind == pqsValueNull
	if negated {
		match = !match
	}
	if match {
		return pqsTruthTrue
	}
	return pqsTruthFalse
}

func pqsEvalCompare(left, right pqsValue, op string) pqsTruth {
	if left.Kind == pqsValueNull || right.Kind == pqsValueNull {
		return pqsTruthNull
	}
	switch op {
	case "=", "!=":
		return pqsEvalEqual(left, right, op == "!=")
	case "<", "<=", ">", ">=":
		return pqsEvalOrdered(left, right, op)
	default:
		return pqsTruthUnknown
	}
}

func pqsEvalEqual(left, right pqsValue, negated bool) pqsTruth {
	eq, ok := pqsCompareEqual(left, right)
	if !ok {
		return pqsTruthUnknown
	}
	if negated {
		eq = !eq
	}
	if eq {
		return pqsTruthTrue
	}
	return pqsTruthFalse
}

func pqsEvalOrdered(left, right pqsValue, op string) pqsTruth {
	lv, rv, ok := pqsCompareNumeric(left, right)
	if !ok {
		return pqsTruthUnknown
	}
	switch op {
	case "<":
		return pqsTruthBool(lv < rv)
	case "<=":
		return pqsTruthBool(lv <= rv)
	case ">":
		return pqsTruthBool(lv > rv)
	case ">=":
		return pqsTruthBool(lv >= rv)
	default:
		return pqsTruthUnknown
	}
}

func pqsCompareEqual(left, right pqsValue) (equal bool, ok bool) {
	if left.Kind == pqsValueString && right.Kind == pqsValueString {
		return left.Str == right.Str, true
	}
	if left.Kind == pqsValueBool && right.Kind == pqsValueBool {
		return left.Bool == right.Bool, true
	}
	lv, rv, ok := pqsCompareNumeric(left, right)
	if ok {
		return lv == rv, true
	}
	return false, false
}

func pqsCompareNumeric(left, right pqsValue) (leftVal float64, rightVal float64, ok bool) {
	lv, lok := pqsNumericValue(left)
	rv, rok := pqsNumericValue(right)
	if !lok || !rok {
		return 0, 0, false
	}
	return lv, rv, true
}

func pqsNumericValue(v pqsValue) (float64, bool) {
	switch v.Kind {
	case pqsValueInt:
		return float64(v.Int), true
	case pqsValueFloat:
		return v.Float, true
	case pqsValueBool:
		if v.Bool {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

func pqsAnd(left, right pqsTruth) pqsTruth {
	if left == pqsTruthFalse || right == pqsTruthFalse {
		return pqsTruthFalse
	}
	if left == pqsTruthUnknown || right == pqsTruthUnknown {
		return pqsTruthUnknown
	}
	if left == pqsTruthNull || right == pqsTruthNull {
		return pqsTruthNull
	}
	return pqsTruthTrue
}

func pqsOr(left, right pqsTruth) pqsTruth {
	if left == pqsTruthTrue || right == pqsTruthTrue {
		return pqsTruthTrue
	}
	if left == pqsTruthUnknown || right == pqsTruthUnknown {
		return pqsTruthUnknown
	}
	if left == pqsTruthNull || right == pqsTruthNull {
		return pqsTruthNull
	}
	return pqsTruthFalse
}

func pqsNot(val pqsTruth) pqsTruth {
	switch val {
	case pqsTruthTrue:
		return pqsTruthFalse
	case pqsTruthFalse:
		return pqsTruthTrue
	case pqsTruthNull:
		return pqsTruthNull
	default:
		return pqsTruthUnknown
	}
}

func pqsTruthBool(val bool) pqsTruth {
	if val {
		return pqsTruthTrue
	}
	return pqsTruthFalse
}
