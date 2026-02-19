package generator

import (
	"strings"

	"shiro/internal/schema"
	"shiro/internal/util"
)

// (constants moved to constants.go)

type templateSpec struct {
	weight int
	build  func() *SelectQuery
}

const (
	templateJoinPredicateStrategyJoinOnly   = "join_only"
	templateJoinPredicateStrategyJoinFilter = "join_filter"
)

// TemplateWeights configures how often and which templates are used when generating queries.
// All values are non-negative integers. Higher values increase the relative likelihood that
// a template (or template family) is selected compared to others.
type TemplateWeights struct {
	// EnabledProb is the percentage (0-100) chance that the generator will attempt to use
	// any template for a query. A value of 0 disables templates entirely.
	EnabledProb int
	// JoinReorder is the relative weight for selecting join-reorder templates.
	JoinReorder int
	// AggPushdown is the relative weight for selecting aggregation-pushdown templates.
	AggPushdown int
	// SemiAnti is the relative weight for selecting semi/anti-join templates.
	SemiAnti int
}

// generateTemplateQuery attempts to construct a SELECT query using one of the
// available query templates for the provided baseTables. It returns the first
// successfully built template query, or nil if template generation is disabled,
// no templates are applicable, or all template builders fail to produce a query.
func (g *Generator) generateTemplateQuery(baseTables []schema.Table) *SelectQuery {
	weights := g.templateWeights()
	if !g.shouldTryTemplates(weights) {
		return nil
	}
	templates := g.availableTemplates(baseTables, weights)
	if len(templates) == 0 {
		return nil
	}
	weightList := make([]int, 0, len(templates))
	for _, t := range templates {
		weightList = append(weightList, t.weight)
	}
	for i := 0; i < len(templates); i++ {
		pick := g.pickTemplate(weightList)
		if weightList[pick] == 0 {
			continue
		}
		if query := templates[pick].build(); query != nil {
			return query
		}
		templates[pick].weight = 0
		weightList[pick] = 0
	}
	return nil
}

// DefaultTemplateWeights returns the baseline template sampling weights.
func DefaultTemplateWeights() TemplateWeights {
	return TemplateWeights{
		EnabledProb: templateEnabledProb,
		JoinReorder: templateJoinReorderWeight,
		AggPushdown: templateAggPushdownWeight,
		SemiAnti:    templateSemiAntiWeight,
	}
}

func (g *Generator) templateWeights() TemplateWeights {
	if g.Template != nil {
		return *g.Template
	}
	return DefaultTemplateWeights()
}

func (g *Generator) shouldTryTemplates(weights TemplateWeights) bool {
	if weights.EnabledProb <= 0 {
		return false
	}
	return util.Chance(g.Rand, weights.EnabledProb)
}

func (g *Generator) availableTemplates(baseTables []schema.Table, weights TemplateWeights) []templateSpec {
	templates := make([]templateSpec, 0, 3)
	if g.Config.Features.Joins && len(g.State.Tables) >= 3 && weights.JoinReorder > 0 {
		templates = append(templates, templateSpec{weight: weights.JoinReorder, build: func() *SelectQuery {
			return g.templateJoinReorder(baseTables)
		}})
	}
	if g.Config.Features.Aggregates && g.Config.Features.GroupBy && weights.AggPushdown > 0 {
		templates = append(templates, templateSpec{weight: weights.AggPushdown, build: func() *SelectQuery {
			return g.templateAggPushdown(baseTables)
		}})
	}
	if g.Config.Features.Subqueries && weights.SemiAnti > 0 {
		templates = append(templates, templateSpec{weight: weights.SemiAnti, build: func() *SelectQuery {
			return g.templateSemiAnti(baseTables)
		}})
	}
	return templates
}

func (g *Generator) pickTemplate(weights []int) int {
	return util.PickWeighted(g.Rand, weights)
}

func (g *Generator) templateJoinReorder(baseTables []schema.Table) *SelectQuery {
	tables := g.pickTemplateTables(baseTables, 3)
	if len(tables) < 3 {
		return nil
	}
	tables = g.maybeShuffleTemplateTables(tables)
	query := &SelectQuery{}
	g.applyTemplateFrom(query, tables)
	g.applyTemplateSelect(query, tables)
	g.applyTemplateJoinPredicate(query, tables)
	return query
}

func (g *Generator) templateAggPushdown(baseTables []schema.Table) *SelectQuery {
	tables := g.pickTemplateTables(baseTables, 1)
	if len(tables) == 0 {
		return nil
	}
	query := &SelectQuery{}
	g.applyTemplateFrom(query, tables)
	if !g.applyTemplateGroupBy(query, tables) {
		return nil
	}
	g.applyTemplateAggSelect(query, tables)
	g.applyTemplatePredicateNoSubquery(query, tables)
	g.applyTemplateHaving(query, tables)
	return query
}

func (g *Generator) templateSemiAnti(baseTables []schema.Table) *SelectQuery {
	tables := g.pickTemplateTables(baseTables, 1)
	if len(tables) == 0 {
		return nil
	}
	sub := g.GenerateSubquery(tables, g.maxSubqDepth-1)
	if sub == nil {
		return nil
	}
	query := &SelectQuery{}
	g.applyTemplateFrom(query, tables)
	g.applyTemplateSelect(query, tables)
	g.applyTemplateSemiAntiPredicate(query, tables, sub)
	return query
}

func (g *Generator) pickTemplateTables(baseTables []schema.Table, minCount int) []schema.Table {
	if len(baseTables) >= minCount {
		return append([]schema.Table{}, baseTables...)
	}
	if len(g.State.Tables) < minCount {
		return nil
	}
	for i := 0; i < templateTablePickRetries; i++ {
		candidate := g.pickTables()
		if len(candidate) >= minCount {
			return candidate
		}
	}
	idxs := g.Rand.Perm(len(g.State.Tables))[:minCount]
	tables := make([]schema.Table, 0, minCount)
	for _, idx := range idxs {
		tables = append(tables, g.State.Tables[idx])
	}
	return tables
}

func (g *Generator) maybeShuffleTemplateTables(tables []schema.Table) []schema.Table {
	return g.maybeShuffleTables(tables)
}

func (g *Generator) templatePredicate(tables []schema.Table) Expr {
	switch g.predicateMode {
	case PredicateModeNone:
		return nil
	case PredicateModeSimple:
		return g.GenerateSimplePredicate(tables, g.maxDepth)
	case PredicateModeSimpleColumns:
		return g.GenerateSimplePredicateColumns(tables, g.maxDepth)
	default:
		allowSubquery := g.Config.Features.Subqueries && !g.disallowScalarSubq
		return g.GeneratePredicate(tables, g.maxDepth, allowSubquery, g.maxSubqDepth)
	}
}

func (g *Generator) templatePredicateNoSubquery(tables []schema.Table) Expr {
	switch g.predicateMode {
	case PredicateModeNone:
		return nil
	case PredicateModeSimple:
		return g.GenerateSimplePredicate(tables, g.maxDepth-1)
	case PredicateModeSimpleColumns:
		return g.GenerateSimplePredicateColumns(tables, g.maxDepth-1)
	default:
		return g.GeneratePredicate(tables, g.maxDepth-1, false, g.maxSubqDepth)
	}
}

func (g *Generator) templateSemiAntiPredicate(tables []schema.Table, sub *SelectQuery) Expr {
	pred := Expr(ExistsExpr{Query: sub})
	if g.Config.Features.NotExists && util.Chance(g.Rand, g.Config.Weights.Features.NotExistsProb) {
		pred = UnaryExpr{Op: "NOT", Expr: pred}
	}
	if util.Chance(g.Rand, templateSemiAntiExtraFilterProb) {
		extra := g.templatePredicateNoSubquery(tables)
		pred = BinaryExpr{Left: pred, Op: "AND", Right: extra}
	}
	return pred
}

func (g *Generator) applyTemplateFrom(query *SelectQuery, tables []schema.Table) {
	derived := g.buildDerivedTableMap(tables)
	query.From = g.buildFromClause(tables, derived)
}

func (g *Generator) applyTemplateSelect(query *SelectQuery, tables []schema.Table) {
	query.Items = g.GenerateSelectList(tables)
}

func (g *Generator) applyTemplatePredicateNoSubquery(query *SelectQuery, tables []schema.Table) {
	query.Where = g.templatePredicateNoSubquery(tables)
}

func (g *Generator) applyTemplateSemiAntiPredicate(query *SelectQuery, tables []schema.Table, sub *SelectQuery) {
	query.Where = g.templateSemiAntiPredicate(tables, sub)
}

func (g *Generator) applyTemplateJoinPredicate(query *SelectQuery, tables []schema.Table) {
	where, strategy := g.templateJoinPredicate(tables)
	query.Where = where
	query.TemplateJoinPredicateStrategy = strategy
}

func (g *Generator) applyTemplateGroupBy(query *SelectQuery, tables []schema.Table) bool {
	query.GroupBy = g.GenerateGroupBy(tables)
	g.applyGroupByExtension(query)
	return len(query.GroupBy) > 0
}

func (g *Generator) applyTemplateAggSelect(query *SelectQuery, tables []schema.Table) {
	query.Items = g.GenerateAggregateSelectList(tables, query.GroupBy)
	if query.GroupByWithRollup || query.GroupByWithCube || len(query.GroupByGroupingSets) > 0 {
		g.maybeAppendGroupingSelectItem(query)
	}
}

func (g *Generator) applyTemplateHaving(query *SelectQuery, tables []schema.Table) {
	if !g.Config.Features.Having || len(query.GroupBy) == 0 {
		return
	}
	if !util.Chance(g.Rand, g.Config.Weights.Features.HavingProb) {
		return
	}
	query.Having = g.GenerateHavingPredicate(query.GroupBy, tables)
}

func (g *Generator) templateJoinPredicate(tables []schema.Table) (Expr, string) {
	switch g.pickTemplateJoinPredicateStrategy() {
	case templateJoinPredicateStrategyJoinOnly:
		return nil, templateJoinPredicateStrategyJoinOnly
	default:
		return g.templatePredicate(tables), templateJoinPredicateStrategyJoinFilter
	}
}

func (g *Generator) pickTemplateJoinPredicateStrategy() string {
	joinOnlyWeight, joinFilterWeight := g.templateJoinPredicateWeights()
	pick := util.PickWeighted(g.Rand, []int{joinOnlyWeight, joinFilterWeight})
	if pick == 0 {
		return templateJoinPredicateStrategyJoinOnly
	}
	return templateJoinPredicateStrategyJoinFilter
}

func (g *Generator) templateJoinPredicateWeights() (joinOnlyWeight int, joinFilterWeight int) {
	joinOnlyWeight = g.Config.Weights.Features.TemplateJoinOnlyWeight
	joinFilterWeight = g.Config.Weights.Features.TemplateJoinFilterWeight
	// Config loading already normalizes template weights. Keep only a
	// lightweight fallback for callers that construct configs programmatically.
	if joinOnlyWeight == 0 && joinFilterWeight == 0 {
		joinOnlyWeight = templateJoinOnlyWeightDefault
		joinFilterWeight = templateJoinFilterWeightDefault
	}
	return joinOnlyWeight, joinFilterWeight
}

// NormalizeTemplateJoinPredicateStrategy canonicalizes template strategy labels.
func NormalizeTemplateJoinPredicateStrategy(strategy string) string {
	switch strings.TrimSpace(strings.ToLower(strategy)) {
	case templateJoinPredicateStrategyJoinOnly:
		return templateJoinPredicateStrategyJoinOnly
	case templateJoinPredicateStrategyJoinFilter:
		return templateJoinPredicateStrategyJoinFilter
	default:
		return ""
	}
}
