package generator

import (
	"shiro/internal/schema"
	"shiro/internal/util"
)

const templateEnabledProb = 55
const templateJoinReorderWeight = 4
const templateAggPushdownWeight = 3
const templateSemiAntiWeight = 3

type templateSpec struct {
	weight int
	build  func() *SelectQuery
}

// TemplateWeights tunes template selection.
type TemplateWeights struct {
	EnabledProb int
	JoinReorder int
	AggPushdown int
	SemiAnti    int
}

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
		if templates[pick].weight == 0 {
			continue
		}
		if query := templates[pick].build(); query != nil {
			return query
		}
		templates[pick].weight = 0
	}
	return nil
}

func DefaultTemplateWeights() TemplateWeights {
	return defaultTemplateWeights()
}

func defaultTemplateWeights() TemplateWeights {
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
	return defaultTemplateWeights()
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
	for i := 0; i < 4; i++ {
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
	return g.GeneratePredicate(tables, g.maxDepth, g.Config.Features.Subqueries, g.maxSubqDepth)
}

func (g *Generator) templatePredicateNoSubquery(tables []schema.Table) Expr {
	return g.GeneratePredicate(tables, g.maxDepth-1, false, g.maxSubqDepth)
}

func (g *Generator) templateSemiAntiPredicate(tables []schema.Table, sub *SelectQuery) Expr {
	pred := Expr(ExistsExpr{Query: sub})
	if g.Config.Features.NotExists && util.Chance(g.Rand, g.Config.Weights.Features.NotExistsProb) {
		pred = UnaryExpr{Op: "NOT", Expr: pred}
	}
	if util.Chance(g.Rand, 60) {
		extra := g.templatePredicateNoSubquery(tables)
		pred = BinaryExpr{Left: pred, Op: "AND", Right: extra}
	}
	return pred
}

func (g *Generator) applyTemplateFrom(query *SelectQuery, tables []schema.Table) {
	query.From = g.buildFromClause(tables)
}

func (g *Generator) applyTemplateSelect(query *SelectQuery, tables []schema.Table) {
	query.Items = g.GenerateSelectList(tables)
}

func (g *Generator) applyTemplatePredicate(query *SelectQuery, tables []schema.Table) {
	query.Where = g.templatePredicate(tables)
}

func (g *Generator) applyTemplatePredicateNoSubquery(query *SelectQuery, tables []schema.Table) {
	query.Where = g.templatePredicateNoSubquery(tables)
}

func (g *Generator) applyTemplateSemiAntiPredicate(query *SelectQuery, tables []schema.Table, sub *SelectQuery) {
	query.Where = g.templateSemiAntiPredicate(tables, sub)
}

func (g *Generator) applyTemplateJoinPredicate(query *SelectQuery, tables []schema.Table) {
	query.Where = g.templateJoinPredicate(tables)
}

func (g *Generator) applyTemplateGroupBy(query *SelectQuery, tables []schema.Table) bool {
	query.GroupBy = g.GenerateGroupBy(tables)
	return len(query.GroupBy) > 0
}

func (g *Generator) applyTemplateAggSelect(query *SelectQuery, tables []schema.Table) {
	query.Items = g.GenerateAggregateSelectList(tables, true)
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

func (g *Generator) templateJoinPredicate(tables []schema.Table) Expr {
	if g.shouldUseJoinOnlyPredicate() {
		return nil
	}
	return g.templatePredicate(tables)
}

// TODO: Split join-only vs join+filter into distinct strategies with richer controls.
func (g *Generator) shouldUseJoinOnlyPredicate() bool {
	return util.Chance(g.Rand, 40)
}
