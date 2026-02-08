package runner

import (
	"shiro/internal/config"
	"shiro/internal/generator"
)

type featureOverrides struct {
	CTE               *bool
	Views             *bool
	DerivedTables     *bool
	SetOperations     *bool
	NaturalJoins      *bool
	FullJoinEmulation *bool
	Aggregates        *bool
	GroupBy           *bool
	Having            *bool
	Distinct          *bool
	OrderBy           *bool
	Limit             *bool
	WindowFuncs       *bool
	Subqueries        *bool
	NotExists         *bool
	NotIn             *bool
}

func (o featureOverrides) apply(dst *config.Features) {
	if dst == nil {
		return
	}
	if o.CTE != nil {
		dst.CTE = *o.CTE
	}
	if o.Views != nil {
		dst.Views = *o.Views
	}
	if o.DerivedTables != nil {
		dst.DerivedTables = *o.DerivedTables
	}
	if o.SetOperations != nil {
		dst.SetOperations = *o.SetOperations
	}
	if o.NaturalJoins != nil {
		dst.NaturalJoins = *o.NaturalJoins
	}
	if o.FullJoinEmulation != nil {
		dst.FullJoinEmulation = *o.FullJoinEmulation
	}
	if o.Aggregates != nil {
		dst.Aggregates = *o.Aggregates
	}
	if o.GroupBy != nil {
		dst.GroupBy = *o.GroupBy
	}
	if o.Having != nil {
		dst.Having = *o.Having
	}
	if o.Distinct != nil {
		dst.Distinct = *o.Distinct
	}
	if o.OrderBy != nil {
		dst.OrderBy = *o.OrderBy
	}
	if o.Limit != nil {
		dst.Limit = *o.Limit
	}
	if o.WindowFuncs != nil {
		dst.WindowFuncs = *o.WindowFuncs
	}
	if o.Subqueries != nil {
		dst.Subqueries = *o.Subqueries
	}
	if o.NotExists != nil {
		dst.NotExists = *o.NotExists
	}
	if o.NotIn != nil {
		dst.NotIn = *o.NotIn
	}
}

type oracleProfile struct {
	Features               featureOverrides
	AllowSubquery          *bool
	PredicateMode          *generator.PredicateMode
	JoinTypeOverride       *generator.JoinType
	MinJoinTables          *int
	DisallowScalarSubquery *bool
	JoinOnPolicy           *string
	JoinUsingProbMin       *int
}

func boolPtr(v bool) *bool {
	return &v
}

func intPtr(v int) *int {
	return &v
}

func stringPtr(v string) *string {
	return &v
}

func predicateModePtr(v generator.PredicateMode) *generator.PredicateMode {
	return &v
}

func joinTypePtr(v generator.JoinType) *generator.JoinType {
	return &v
}

var oracleProfiles = map[string]oracleProfile{
	"GroundTruth": {
		Features: featureOverrides{
			CTE:               boolPtr(false),
			Views:             boolPtr(false),
			DerivedTables:     boolPtr(false),
			SetOperations:     boolPtr(false),
			NaturalJoins:      boolPtr(false),
			FullJoinEmulation: boolPtr(false),
			Aggregates:        boolPtr(false),
			GroupBy:           boolPtr(false),
			Having:            boolPtr(false),
			Distinct:          boolPtr(false),
			OrderBy:           boolPtr(false),
			Limit:             boolPtr(false),
			WindowFuncs:       boolPtr(false),
			Subqueries:        boolPtr(false),
			NotExists:         boolPtr(false),
			NotIn:             boolPtr(false),
		},
		AllowSubquery:          boolPtr(false),
		PredicateMode:          predicateModePtr(generator.PredicateModeNone),
		JoinTypeOverride:       joinTypePtr(generator.JoinInner),
		MinJoinTables:          intPtr(2),
		DisallowScalarSubquery: boolPtr(true),
		JoinOnPolicy:           stringPtr("simple"),
		JoinUsingProbMin:       intPtr(100),
	},
	"CODDTest": {
		Features: featureOverrides{
			CTE:               boolPtr(false),
			Views:             boolPtr(false),
			DerivedTables:     boolPtr(false),
			SetOperations:     boolPtr(false),
			NaturalJoins:      boolPtr(false),
			FullJoinEmulation: boolPtr(false),
			Aggregates:        boolPtr(false),
			GroupBy:           boolPtr(false),
			Having:            boolPtr(false),
			Distinct:          boolPtr(false),
			OrderBy:           boolPtr(false),
			Limit:             boolPtr(false),
			WindowFuncs:       boolPtr(false),
			Subqueries:        boolPtr(false),
			NotExists:         boolPtr(false),
			NotIn:             boolPtr(false),
		},
		AllowSubquery:          boolPtr(false),
		PredicateMode:          predicateModePtr(generator.PredicateModeSimpleColumns),
		MinJoinTables:          intPtr(1),
		DisallowScalarSubquery: boolPtr(true),
	},
	"Impo": {
		Features: featureOverrides{
			CTE: boolPtr(false),
		},
		AllowSubquery:          boolPtr(true),
		DisallowScalarSubquery: boolPtr(true),
	},
	"NoREC": {
		Features: featureOverrides{
			CTE:         boolPtr(false),
			Aggregates:  boolPtr(false),
			GroupBy:     boolPtr(false),
			Having:      boolPtr(false),
			Distinct:    boolPtr(false),
			OrderBy:     boolPtr(false),
			Limit:       boolPtr(false),
			WindowFuncs: boolPtr(false),
		},
		AllowSubquery: boolPtr(true),
		PredicateMode: predicateModePtr(generator.PredicateModeSimple),
	},
	"TLP": {
		Features: featureOverrides{
			CTE:         boolPtr(false),
			Aggregates:  boolPtr(false),
			GroupBy:     boolPtr(false),
			Having:      boolPtr(false),
			Distinct:    boolPtr(false),
			OrderBy:     boolPtr(false),
			Limit:       boolPtr(false),
			WindowFuncs: boolPtr(false),
		},
		AllowSubquery: boolPtr(true),
		PredicateMode: predicateModePtr(generator.PredicateModeSimpleColumns),
		JoinOnPolicy:  stringPtr("complex"),
	},
	"DQP": {
		Features: featureOverrides{
			CTE:         boolPtr(false),
			Aggregates:  boolPtr(false),
			GroupBy:     boolPtr(false),
			Having:      boolPtr(false),
			Distinct:    boolPtr(false),
			Limit:       boolPtr(false),
			WindowFuncs: boolPtr(false),
		},
		AllowSubquery: boolPtr(true),
		PredicateMode: predicateModePtr(generator.PredicateModeSimpleColumns),
		MinJoinTables: intPtr(2),
	},
}
