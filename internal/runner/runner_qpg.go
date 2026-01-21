package runner

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/util"
)

func (r *Runner) observePlan(ctx context.Context, sqlText string) {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	if r.qpgState != nil && r.qpgState.shouldSkipExplain(sqlText) {
		return
	}
	explainSQL := "EXPLAIN " + sqlText
	if format := strings.TrimSpace(r.cfg.QPG.ExplainFormat); format != "" {
		explainSQL = fmt.Sprintf("EXPLAIN FORMAT='%s' %s", format, sqlText)
	}
	rows, err := r.exec.QueryContext(qctx, explainSQL)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "format") {
			rows, err = r.exec.QueryContext(qctx, "EXPLAIN "+sqlText)
		}
		if err != nil {
			return
		}
	}
	defer rows.Close()
	info, err := parsePlan(rows)
	if err != nil || info.signature == "" {
		return
	}
	r.observePlanInfo(ctx, info)
}

func (r *Runner) explainSignature(ctx context.Context, sqlText string) (string, string) {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	explainSQL := "EXPLAIN " + sqlText
	if format := strings.TrimSpace(r.cfg.QPG.ExplainFormat); format != "" {
		explainSQL = fmt.Sprintf("EXPLAIN FORMAT='%s' %s", format, sqlText)
	}
	rows, err := r.exec.QueryContext(qctx, explainSQL)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "format") {
			rows, err = r.exec.QueryContext(qctx, "EXPLAIN "+sqlText)
		}
		if err != nil {
			return "", ""
		}
	}
	defer rows.Close()
	info, err := parsePlan(rows)
	if err != nil {
		return "", ""
	}
	if info.signature == "" {
		return "", ""
	}
	return info.signature, info.version
}

func (r *Runner) observePlanForConnection(ctx context.Context, connID int64) {
	if !r.cfg.QPG.Enabled || r.qpgState == nil {
		return
	}
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	rows, err := r.exec.QueryContext(qctx, fmt.Sprintf("EXPLAIN FOR CONNECTION %d", connID))
	if err != nil {
		return
	}
	defer rows.Close()
	info, err := parsePlan(rows)
	if err != nil || info.signature == "" {
		return
	}
	r.observePlanInfo(ctx, info)
}

func (r *Runner) observePlanInfo(ctx context.Context, info planInfo) {
	if r.qpgState == nil {
		return
	}
	obs := r.qpgState.observe(info)
	if !obs.newPlan && r.cfg.QPG.MutationProb > 0 && util.Chance(r.gen.Rand, r.cfg.QPG.MutationProb) {
		r.qpgMutate(ctx)
	}
}

type planInfo struct {
	signature string
	shapeSig  string
	opSig     string
	operators []string
	joins     []string
	joinOrder string
	hasJoin   bool
	hasAgg    bool
	version   string
}

type qpgState struct {
	seenPlans          map[string]struct{}
	seenShapes         map[string]struct{}
	seenOps            map[string]struct{}
	seenJoins          map[string]struct{}
	seenJoinOrder      map[string]struct{}
	seenOpSig          map[string]struct{}
	seenSQL            map[string]int64
	noNewPlan          int
	noNewOp            int
	noJoin             int
	noAgg              int
	noNewJoinType      int
	noNewShape         int
	noNewOpSig         int
	noNewJoinOrder     int
	override           *generator.AdaptiveWeights
	overrideTTL        int
	lastOverride       string
	lastOverrideLogged string
	seenSQLTTL         int64
	seenSQLMax         int
	seenSQLSweep       int64
}

type qpgObservation struct {
	newPlan     bool
	newOp       bool
	newJoinType bool
}

func newQPGState(cfg config.QPGConfig) *qpgState {
	ttl := cfg.SeenSQLTTLSeconds
	if ttl <= 0 {
		ttl = 60
	}
	maxEntries := cfg.SeenSQLMax
	if maxEntries <= 0 {
		maxEntries = 4096
	}
	sweep := cfg.SeenSQLSweepSeconds
	if sweep <= 0 {
		sweep = 300
	}
	return &qpgState{
		seenPlans:     make(map[string]struct{}),
		seenShapes:    make(map[string]struct{}),
		seenOps:       make(map[string]struct{}),
		seenJoins:     make(map[string]struct{}),
		seenJoinOrder: make(map[string]struct{}),
		seenOpSig:     make(map[string]struct{}),
		seenSQL:       make(map[string]int64),
		seenSQLTTL:    int64(ttl),
		seenSQLMax:    maxEntries,
		seenSQLSweep:  int64(sweep),
	}
}

func parsePlan(rows *sql.Rows) (planInfo, error) {
	cols, err := rows.Columns()
	if err != nil {
		return planInfo{}, err
	}
	idIdx := 0
	for i, col := range cols {
		if strings.EqualFold(col, "id") {
			idIdx = i
			break
		}
	}
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var b strings.Builder
	var shape strings.Builder
	var opSig strings.Builder
	var ops []string
	var joins []string
	hasJoin := false
	hasAgg := false
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return planInfo{}, err
		}
		if len(cols) == 1 {
			text := string(values[0])
			if isJSONText(text) {
				return parsePlanJSON(text), nil
			}
		}
		normalizePlanRow(values)
		for i, v := range values {
			if i > 0 {
				b.WriteByte('|')
			}
			b.Write(v)
		}
		b.WriteByte('\n')
		id := ""
		if idIdx >= 0 && idIdx < len(values) {
			id = string(values[idIdx])
		}
		depth, op := parsePlanNode(id)
		if op != "" {
			ops = append(ops, op)
			shape.WriteString(fmt.Sprintf("%d:%s;", depth, op))
			opSig.WriteString(op)
			opSig.WriteByte(';')
			if strings.Contains(strings.ToLower(op), "join") {
				hasJoin = true
				joins = append(joins, fmt.Sprintf("%s@%d", joinTypeFromOp(op), depth))
			}
			if strings.Contains(strings.ToLower(op), "agg") {
				hasAgg = true
			}
		}
	}
	sum := sha1.Sum([]byte(b.String()))
	version := "plain"
	return planInfo{
		signature: hex.EncodeToString(sum[:]),
		shapeSig:  shape.String(),
		opSig:     opSig.String(),
		operators: ops,
		joins:     joins,
		joinOrder: strings.Join(joins, "->"),
		hasJoin:   hasJoin,
		hasAgg:    hasAgg,
		version:   version,
	}, nil
}

func parsePlanNode(id string) (int, string) {
	if id == "" {
		return 0, ""
	}
	prefix, rest := splitPlanPrefix(id)
	if rest == "" {
		return 0, ""
	}
	op := rest
	for i, r := range rest {
		if r == '_' || r == ' ' || r == '(' {
			op = rest[:i]
			break
		}
	}
	spaceCount := 0
	barCount := 0
	for _, r := range prefix {
		if r == ' ' {
			spaceCount++
		} else if r == '│' || r == '|' {
			barCount++
		}
	}
	depth := barCount + spaceCount/2
	return depth, op
}

func splitPlanPrefix(id string) (string, string) {
	for i, r := range id {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return id[:i], id[i:]
		}
	}
	return id, ""
}

func (s *qpgState) observe(info planInfo) qpgObservation {
	obs := qpgObservation{}
	if info.signature == "" {
		return obs
	}
	if _, ok := s.seenPlans[info.signature]; !ok {
		s.seenPlans[info.signature] = struct{}{}
		obs.newPlan = true
		s.noNewPlan = 0
	} else {
		s.noNewPlan++
	}
	if info.shapeSig != "" {
		if _, ok := s.seenShapes[info.shapeSig]; !ok {
			s.seenShapes[info.shapeSig] = struct{}{}
			s.noNewShape = 0
		} else {
			s.noNewShape++
		}
	}
	for _, op := range info.operators {
		if _, ok := s.seenOps[op]; !ok {
			s.seenOps[op] = struct{}{}
			obs.newOp = true
		}
	}
	if info.opSig != "" {
		if _, ok := s.seenOpSig[info.opSig]; !ok {
			s.seenOpSig[info.opSig] = struct{}{}
			s.noNewOpSig = 0
		} else {
			s.noNewOpSig++
		}
	}
	if obs.newOp {
		s.noNewOp = 0
	} else {
		s.noNewOp++
	}
	for _, joinType := range info.joins {
		if joinType == "" {
			continue
		}
		if _, ok := s.seenJoins[joinType]; !ok {
			s.seenJoins[joinType] = struct{}{}
			obs.newJoinType = true
		}
	}
	if obs.newJoinType {
		s.noNewJoinType = 0
	} else if info.hasJoin {
		s.noNewJoinType++
	}
	if info.joinOrder != "" {
		if _, ok := s.seenJoinOrder[info.joinOrder]; !ok {
			s.seenJoinOrder[info.joinOrder] = struct{}{}
			s.noNewJoinOrder = 0
		} else if info.hasJoin {
			s.noNewJoinOrder++
		}
	}
	if info.hasJoin {
		s.noJoin = 0
	} else {
		s.noJoin++
	}
	if info.hasAgg {
		s.noAgg = 0
	} else {
		s.noAgg++
	}
	return obs
}

func (s *qpgState) stats() (int, int, int, int) {
	return len(s.seenPlans), len(s.seenShapes), len(s.seenOps), len(s.seenJoins)
}

func (r *Runner) applyQPGWeights() bool {
	if !r.cfg.QPG.Enabled || r.qpgState == nil {
		return false
	}
	if r.qpgState.overrideTTL <= 0 {
		setOverride := false
		if r.qpgState.noJoin >= 3 {
			joinCount := max(r.cfg.Weights.Features.JoinCount, 3)
			r.qpgState.override = &generator.AdaptiveWeights{JoinCount: min(joinCount, r.cfg.MaxJoinTables)}
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noAgg >= 3 {
			agg := max(r.cfg.Weights.Features.AggProb, 60)
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.AggProb = agg
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noNewPlan >= 5 {
			subq := max(r.cfg.Weights.Features.SubqCount, 3)
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.SubqCount = subq
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noNewOpSig >= 4 {
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.SubqCount = max(r.cfg.Weights.Features.SubqCount, 3)
			override.AggProb = max(r.cfg.Weights.Features.AggProb, 60)
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noNewShape >= 4 {
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.JoinCount = max(r.cfg.Weights.Features.JoinCount, 3)
			override.SubqCount = max(r.cfg.Weights.Features.SubqCount, 3)
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noNewJoinType >= 3 {
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.JoinCount = max(r.cfg.Weights.Features.JoinCount, 3)
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if r.qpgState.noNewJoinOrder >= 3 {
			override := r.qpgState.override
			if override == nil {
				override = &generator.AdaptiveWeights{}
			}
			override.JoinCount = max(r.cfg.Weights.Features.JoinCount, 4)
			override.SubqCount = max(r.cfg.Weights.Features.SubqCount, 3)
			r.qpgState.override = override
			r.qpgState.overrideTTL = 5
			setOverride = true
		}
		if setOverride && r.cfg.Logging.Verbose && r.qpgState.override != nil {
			sig := fmt.Sprintf("%d/%d/%d/%d", r.qpgState.override.JoinCount, r.qpgState.override.SubqCount, r.qpgState.override.AggProb, r.qpgState.overrideTTL)
			if sig != r.qpgState.lastOverride {
				r.qpgState.lastOverride = sig
			}
		}
	}
	if r.qpgState.override == nil || r.qpgState.overrideTTL <= 0 {
		return false
	}
	base := generator.AdaptiveWeights{
		JoinCount: r.cfg.Weights.Features.JoinCount,
		SubqCount: r.cfg.Weights.Features.SubqCount,
		AggProb:   r.cfg.Weights.Features.AggProb,
	}
	if r.gen.Adaptive != nil {
		base = *r.gen.Adaptive
	}
	override := r.qpgState.override
	if override.JoinCount > 0 {
		base.JoinCount = override.JoinCount
	}
	if override.SubqCount > 0 {
		base.SubqCount = override.SubqCount
	}
	if override.AggProb > 0 {
		base.AggProb = override.AggProb
	}
	r.gen.SetAdaptiveWeights(base)
	return true
}

func (r *Runner) tickQPG() {
	if r.qpgState == nil || r.qpgState.overrideTTL <= 0 {
		return
	}
	r.qpgState.overrideTTL--
	if r.qpgState.overrideTTL == 0 {
		r.qpgState.override = nil
	}
}

func (r *Runner) qpgMutate(ctx context.Context) {
	if len(r.state.Tables) == 0 {
		return
	}
	if r.cfg.Features.Indexes && util.Chance(r.gen.Rand, 50) {
		tableIdx := r.gen.Rand.Intn(len(r.state.Tables))
		tablePtr := &r.state.Tables[tableIdx]
		sql, ok := r.gen.CreateIndexSQL(tablePtr)
		if ok {
			_ = r.execSQL(ctx, sql)
		}
		return
	}
	tbl := r.state.Tables[r.gen.Rand.Intn(len(r.state.Tables))]
	_ = r.execSQL(ctx, fmt.Sprintf("ANALYZE TABLE %s", tbl.Name))
}

func (s *qpgState) shouldSkipExplain(sqlText string) bool {
	if sqlText == "" {
		return true
	}
	key := sha1.Sum([]byte(sqlText))
	hash := hex.EncodeToString(key[:])
	now := time.Now().Unix()
	if last, ok := s.seenSQL[hash]; ok && now-last < s.seenSQLTTL {
		return true
	}
	s.seenSQL[hash] = now
	if len(s.seenSQL) > s.seenSQLMax {
		for k, v := range s.seenSQL {
			if now-v > s.seenSQLSweep {
				delete(s.seenSQL, k)
			}
		}
	}
	return false
}

func joinTypeFromOp(op string) string {
	lower := strings.ToLower(op)
	switch {
	case strings.Contains(lower, "indexhashjoin"):
		return "IndexHashJoin"
	case strings.Contains(lower, "indexjoin"):
		return "IndexJoin"
	case strings.Contains(lower, "mergejoin"):
		return "MergeJoin"
	case strings.Contains(lower, "hashjoin"):
		return "HashJoin"
	case strings.Contains(lower, "join"):
		return "Join"
	default:
		return ""
	}
}

func normalizePlanRow(values []sql.RawBytes) {
	for i, v := range values {
		if len(v) == 0 {
			continue
		}
		normalized := normalizePlanValue(string(v))
		if normalized != string(v) {
			values[i] = []byte(normalized)
		}
	}
}

func normalizePlanValue(value string) string {
	if value == "" {
		return value
	}
	normalized := regexp.MustCompile(`t\\d+`).ReplaceAllString(value, "tN")
	normalized = regexp.MustCompile(`c\\d+`).ReplaceAllString(normalized, "cN")
	normalized = regexp.MustCompile(`idx_\\w+`).ReplaceAllString(normalized, "idx_N")
	normalized = regexp.MustCompile(`\\b\\d+\\b`).ReplaceAllString(normalized, "N")
	return normalized
}

func normalizeOp(op string) string {
	if op == "" {
		return ""
	}
	out := op
	for i, r := range op {
		if r == '_' || r == ' ' || r == '(' {
			out = op[:i]
			break
		}
	}
	return out
}

func isJSONText(text string) bool {
	trimmed := strings.TrimSpace(text)
	return strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[")
}

var jsonIDPattern = regexp.MustCompile(`"id"\s*:\s*"([^"]+)"`)
var jsonOpPattern = regexp.MustCompile(`"operator"\s*:\s*"([^"]+)"`)

func parsePlanJSON(text string) planInfo {
	trimmed := strings.TrimSpace(text)
	ops := make([]string, 0, 16)
	joins := make([]string, 0, 8)
	hasJoin := false
	hasAgg := false
	var shape strings.Builder
	var opSig strings.Builder
	matches := jsonIDPattern.FindAllStringSubmatch(trimmed, -1)
	if len(matches) == 0 {
		matches = jsonOpPattern.FindAllStringSubmatch(trimmed, -1)
	}
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		token := match[1]
		_, op := parsePlanNode(token)
		if op == "" {
			op = token
		}
		op = normalizeOp(op)
		if op == "" {
			continue
		}
		ops = append(ops, op)
		shape.WriteString("0:")
		shape.WriteString(op)
		shape.WriteString(";")
		opSig.WriteString(op)
		opSig.WriteByte(';')
		if strings.Contains(strings.ToLower(op), "join") {
			hasJoin = true
			joins = append(joins, joinTypeFromOp(op))
		}
		if strings.Contains(strings.ToLower(op), "agg") {
			hasAgg = true
		}
	}
	sum := sha1.Sum([]byte(trimmed))
	return planInfo{
		signature: hex.EncodeToString(sum[:]),
		shapeSig:  shape.String(),
		opSig:     opSig.String(),
		operators: ops,
		joins:     joins,
		joinOrder: strings.Join(joins, "->"),
		hasJoin:   hasJoin,
		hasAgg:    hasAgg,
		version:   "json",
	}
}
