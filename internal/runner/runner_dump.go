package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"shiro/internal/config"
	"shiro/internal/generator"
	"shiro/internal/util"
)

type dynamicDump struct {
	Version        int                        `json:"version"`
	Timestamp      string                     `json:"timestamp"`
	Seed           int64                      `json:"seed"`
	FeatureWeights config.FeatureWeights      `json:"feature_weights"`
	Adaptive       *generator.AdaptiveWeights `json:"adaptive_weights,omitempty"`
	Template       *generator.TemplateWeights `json:"template_weights,omitempty"`
	Bandits        *banditDump                `json:"bandits,omitempty"`
	QPG            *qpgDump                   `json:"qpg,omitempty"`
	Impo           *impoDump                  `json:"impo,omitempty"`
}

type banditDump struct {
	Action        *util.BanditSnapshot `json:"action,omitempty"`
	Oracle        *util.BanditSnapshot `json:"oracle,omitempty"`
	OracleArms    []string             `json:"oracle_arms,omitempty"`
	OracleEnabled []bool               `json:"oracle_enabled,omitempty"`
	OracleWeights []int                `json:"oracle_weights,omitempty"`
	DML           *util.BanditSnapshot `json:"dml,omitempty"`
	Feature       *featureDump         `json:"feature,omitempty"`
}

type featureDump struct {
	JoinArms        []int               `json:"join_arms"`
	SubqArms        []int               `json:"subq_arms"`
	AggArms         []int               `json:"agg_arms"`
	IndexPrefixArms []int               `json:"index_prefix_arms"`
	GroupByOrdArms  []int               `json:"group_by_ord_arms"`
	Join            util.BanditSnapshot `json:"join"`
	Subq            util.BanditSnapshot `json:"subq"`
	Agg             util.BanditSnapshot `json:"agg"`
	IndexPrefix     util.BanditSnapshot `json:"index_prefix"`
	GroupByOrd      util.BanditSnapshot `json:"group_by_ord"`
	LastArms        featureArms         `json:"last_arms"`
}

type qpgDump struct {
	SeenPlans      int                        `json:"seen_plans"`
	SeenShapes     int                        `json:"seen_shapes"`
	SeenOps        int                        `json:"seen_ops"`
	SeenJoins      int                        `json:"seen_joins"`
	SeenJoinOrders int                        `json:"seen_join_orders"`
	SeenOpSigs     int                        `json:"seen_op_sigs"`
	SeenSQL        int                        `json:"seen_sql"`
	NoNewPlan      int                        `json:"no_new_plan"`
	NoNewOp        int                        `json:"no_new_op"`
	NoNewJoinType  int                        `json:"no_new_join_type"`
	NoNewShape     int                        `json:"no_new_shape"`
	NoNewOpSig     int                        `json:"no_new_op_sig"`
	NoNewJoinOrder int                        `json:"no_new_join_order"`
	NoJoin         int                        `json:"no_join"`
	NoAgg          int                        `json:"no_agg"`
	Override       *generator.AdaptiveWeights `json:"override,omitempty"`
	OverrideTTL    int                        `json:"override_ttl"`
	LastOverride   string                     `json:"last_override"`
}

type impoDump struct {
	Total     int64 `json:"total"`
	Skipped   int64 `json:"skipped"`
	Truncated int64 `json:"truncated"`
}

func (r *Runner) dumpDynamicState() {
	dump := dynamicDump{
		Version:        1,
		Timestamp:      time.Now().Format(time.RFC3339),
		Seed:           r.seedSnapshot(),
		FeatureWeights: r.cfg.Weights.Features,
		Adaptive:       r.adaptiveSnapshot(),
		Template:       r.templateSnapshot(),
		Bandits:        r.snapshotBandits(),
		QPG:            r.snapshotQPG(),
		Impo:           r.snapshotImpo(),
	}
	data, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return
	}
	wd, err := os.Getwd()
	if err != nil {
		wd = "."
	}
	path := filepath.Join(wd, "dynamic_state.json")
	_ = os.WriteFile(path, data, 0o644)
}

func (r *Runner) snapshotBandits() *banditDump {
	if r.actionBandit == nil && r.oracleBandit == nil && r.dmlBandit == nil && r.featureBandit == nil {
		return nil
	}
	out := &banditDump{}
	if r.actionBandit != nil {
		s := r.actionBandit.Snapshot()
		out.Action = &s
	}
	if r.oracleBandit != nil {
		s := r.oracleBandit.Snapshot()
		out.Oracle = &s
		out.OracleWeights = append([]int{}, r.nonCertWeights()...)
		out.OracleArms = make([]string, 0, len(r.nonCertOracleIdx))
		for _, idx := range r.nonCertOracleIdx {
			out.OracleArms = append(out.OracleArms, r.oracles[idx].Name())
		}
		if r.oracleEnabled != nil {
			out.OracleEnabled = append([]bool{}, r.oracleEnabled...)
		}
	}
	if r.dmlBandit != nil {
		s := r.dmlBandit.Snapshot()
		out.DML = &s
	}
	if r.featureBandit != nil {
		out.Feature = &featureDump{
			JoinArms:        append([]int{}, r.featureBandit.joinArms...),
			SubqArms:        append([]int{}, r.featureBandit.subqArms...),
			AggArms:         append([]int{}, r.featureBandit.aggArms...),
			IndexPrefixArms: append([]int{}, r.featureBandit.indexPrefixArms...),
			GroupByOrdArms:  append([]int{}, r.featureBandit.groupByOrdArms...),
			Join:            r.featureBandit.joinBandit.Snapshot(),
			Subq:            r.featureBandit.subqBandit.Snapshot(),
			Agg:             r.featureBandit.aggBandit.Snapshot(),
			IndexPrefix:     r.featureBandit.indexPrefixBandit.Snapshot(),
			GroupByOrd:      r.featureBandit.groupByOrdBandit.Snapshot(),
			LastArms:        r.lastFeatureArms,
		}
	}
	return out
}

func (r *Runner) snapshotQPG() *qpgDump {
	if !r.cfg.QPG.Enabled || r.qpgState == nil {
		return nil
	}
	r.qpgMu.Lock()
	defer r.qpgMu.Unlock()
	plans, shapes, ops, joins, joinOrders, opSigs, seenSQL := r.qpgState.stats()
	var override *generator.AdaptiveWeights
	if r.qpgState.override != nil {
		overrideCopy := *r.qpgState.override
		override = &overrideCopy
	}
	return &qpgDump{
		SeenPlans:      plans,
		SeenShapes:     shapes,
		SeenOps:        ops,
		SeenJoins:      joins,
		SeenJoinOrders: joinOrders,
		SeenOpSigs:     opSigs,
		SeenSQL:        seenSQL,
		NoNewPlan:      r.qpgState.noNewPlan,
		NoNewOp:        r.qpgState.noNewOp,
		NoNewJoinType:  r.qpgState.noNewJoinType,
		NoNewShape:     r.qpgState.noNewShape,
		NoNewOpSig:     r.qpgState.noNewOpSig,
		NoNewJoinOrder: r.qpgState.noNewJoinOrder,
		NoJoin:         r.qpgState.noJoin,
		NoAgg:          r.qpgState.noAgg,
		Override:       override,
		OverrideTTL:    r.qpgState.overrideTTL,
		LastOverride:   r.qpgState.lastOverride,
	}
}

func (r *Runner) snapshotImpo() *impoDump {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	if r.impoTotal == 0 && r.impoSkips == 0 && r.impoTrunc == 0 {
		return nil
	}
	return &impoDump{
		Total:     r.impoTotal,
		Skipped:   r.impoSkips,
		Truncated: r.impoTrunc,
	}
}
