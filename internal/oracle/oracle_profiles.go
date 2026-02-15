package oracle

import (
	"shiro/internal/config"
	"shiro/internal/generator"
)

// FeatureOverrides describes per-oracle feature flags to toggle capabilities.
type FeatureOverrides struct {
	CTE                  *bool
	Views                *bool
	DerivedTables        *bool
	SetOperations        *bool
	NaturalJoins         *bool
	FullJoinEmulation    *bool
	Aggregates           *bool
	GroupBy              *bool
	Having               *bool
	Distinct             *bool
	OrderBy              *bool
	Limit                *bool
	WindowFuncs          *bool
	Subqueries           *bool
	QuantifiedSubqueries *bool
	NotExists            *bool
	NotIn                *bool
}

// Apply copies overrides onto the target feature set.
func (o FeatureOverrides) Apply(dst *config.Features) {
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
	if o.QuantifiedSubqueries != nil {
		dst.QuantifiedSubqueries = *o.QuantifiedSubqueries
	}
	if o.NotExists != nil {
		dst.NotExists = *o.NotExists
	}
	if o.NotIn != nil {
		dst.NotIn = *o.NotIn
	}
}

// Profile captures per-oracle capability and generator overrides.
type Profile struct {
	Features               FeatureOverrides
	AllowSubquery          *bool
	PredicateMode          *generator.PredicateMode
	JoinTypeOverride       *generator.JoinType
	MinJoinTables          *int
	DisallowScalarSubquery *bool
	JoinOnPolicy           *string
	JoinUsingProbMin       *int
}

// BoolPtr returns a pointer to a bool literal.
func BoolPtr(v bool) *bool {
	return &v
}

// IntPtr returns a pointer to an int literal.
func IntPtr(v int) *int {
	return &v
}

// StringPtr returns a pointer to a string literal.
func StringPtr(v string) *string {
	return &v
}

// PredicateModePtr returns a pointer to a predicate mode literal.
func PredicateModePtr(v generator.PredicateMode) *generator.PredicateMode {
	return &v
}

// JoinTypePtr returns a pointer to a join type literal.
func JoinTypePtr(v generator.JoinType) *generator.JoinType {
	return &v
}

// Profiles defines the default per-oracle capability profiles.
var Profiles = map[string]Profile{
	"GroundTruth": {
		Features: FeatureOverrides{
			CTE:               BoolPtr(false),
			Views:             BoolPtr(false),
			DerivedTables:     BoolPtr(false),
			SetOperations:     BoolPtr(false),
			NaturalJoins:      BoolPtr(false),
			FullJoinEmulation: BoolPtr(false),
			Aggregates:        BoolPtr(false),
			GroupBy:           BoolPtr(false),
			Having:            BoolPtr(false),
			Distinct:          BoolPtr(false),
			OrderBy:           BoolPtr(false),
			Limit:             BoolPtr(false),
			WindowFuncs:       BoolPtr(false),
			Subqueries:        BoolPtr(false),
			NotExists:         BoolPtr(false),
			NotIn:             BoolPtr(false),
		},
		AllowSubquery:          BoolPtr(false),
		PredicateMode:          PredicateModePtr(generator.PredicateModeNone),
		JoinTypeOverride:       JoinTypePtr(generator.JoinInner),
		MinJoinTables:          IntPtr(2),
		DisallowScalarSubquery: BoolPtr(true),
		JoinOnPolicy:           StringPtr("simple"),
		JoinUsingProbMin:       IntPtr(100),
	},
	"CODDTest": {
		Features: FeatureOverrides{
			CTE:               BoolPtr(false),
			Views:             BoolPtr(false),
			DerivedTables:     BoolPtr(false),
			SetOperations:     BoolPtr(false),
			NaturalJoins:      BoolPtr(false),
			FullJoinEmulation: BoolPtr(false),
			Aggregates:        BoolPtr(false),
			GroupBy:           BoolPtr(false),
			Having:            BoolPtr(false),
			Distinct:          BoolPtr(false),
			OrderBy:           BoolPtr(false),
			Limit:             BoolPtr(false),
			WindowFuncs:       BoolPtr(false),
			Subqueries:        BoolPtr(false),
			NotExists:         BoolPtr(false),
			NotIn:             BoolPtr(false),
		},
		AllowSubquery:          BoolPtr(false),
		PredicateMode:          PredicateModePtr(generator.PredicateModeSimpleColumns),
		MinJoinTables:          IntPtr(1),
		DisallowScalarSubquery: BoolPtr(true),
	},
	"Impo": {
		Features: FeatureOverrides{
			CTE: BoolPtr(false),
		},
		AllowSubquery:          BoolPtr(true),
		DisallowScalarSubquery: BoolPtr(true),
	},
	"NoREC": {
		Features: FeatureOverrides{
			CTE:           BoolPtr(false),
			Aggregates:    BoolPtr(false),
			GroupBy:       BoolPtr(false),
			Having:        BoolPtr(false),
			Distinct:      BoolPtr(false),
			OrderBy:       BoolPtr(false),
			Limit:         BoolPtr(false),
			SetOperations: BoolPtr(false),
			WindowFuncs:   BoolPtr(false),
		},
		AllowSubquery: BoolPtr(true),
		PredicateMode: PredicateModePtr(generator.PredicateModeSimple),
	},
	"TLP": {
		Features: FeatureOverrides{
			CTE:         BoolPtr(false),
			Aggregates:  BoolPtr(false),
			GroupBy:     BoolPtr(false),
			Having:      BoolPtr(false),
			Distinct:    BoolPtr(false),
			OrderBy:     BoolPtr(false),
			Limit:       BoolPtr(false),
			WindowFuncs: BoolPtr(false),
		},
		AllowSubquery: BoolPtr(true),
		PredicateMode: PredicateModePtr(generator.PredicateModeSimpleColumns),
		JoinOnPolicy:  StringPtr("complex"),
	},
	"DQP": {
		Features: FeatureOverrides{
			CTE:         BoolPtr(false),
			Aggregates:  BoolPtr(false),
			GroupBy:     BoolPtr(false),
			Having:      BoolPtr(false),
			Distinct:    BoolPtr(false),
			Limit:       BoolPtr(false),
			WindowFuncs: BoolPtr(false),
		},
		AllowSubquery: BoolPtr(true),
		PredicateMode: PredicateModePtr(generator.PredicateModeSimpleColumns),
		MinJoinTables: IntPtr(2),
	},
	"PQS": {
		Features: FeatureOverrides{
			CTE:                  BoolPtr(false),
			Views:                BoolPtr(false),
			DerivedTables:        BoolPtr(true),
			SetOperations:        BoolPtr(false),
			NaturalJoins:         BoolPtr(false),
			FullJoinEmulation:    BoolPtr(false),
			Aggregates:           BoolPtr(false),
			GroupBy:              BoolPtr(false),
			Having:               BoolPtr(false),
			Distinct:             BoolPtr(false),
			OrderBy:              BoolPtr(false),
			Limit:                BoolPtr(false),
			WindowFuncs:          BoolPtr(false),
			Subqueries:           BoolPtr(true),
			QuantifiedSubqueries: BoolPtr(true),
			NotExists:            BoolPtr(false),
			NotIn:                BoolPtr(false),
		},
		AllowSubquery: BoolPtr(true),
		PredicateMode: PredicateModePtr(generator.PredicateModeNone),
	},
	"CERT": {
		PredicateMode: PredicateModePtr(generator.PredicateModeSimple),
	},
	"EET": {
		AllowSubquery: BoolPtr(true),
	},
}

// ProfileByName returns a profile by oracle name when available.
func ProfileByName(name string) *Profile {
	profile, ok := Profiles[name]
	if !ok {
		return nil
	}
	return &profile
}
