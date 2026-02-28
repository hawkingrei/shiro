package oracle

import "shiro/internal/generator"

func queryTableFactorCountWithCTE(query *generator.SelectQuery) int {
	if query == nil {
		return 0
	}
	return queryTableFactorCountFromClause(query.From) + queryTableFactorCountCTEs(query.With)
}

func queryTableFactorCountFromClause(from generator.FromClause) int {
	count := 0
	if from.BaseQuery != nil || from.BaseTable != "" {
		count++
	}
	count += len(from.Joins)
	return count
}

func queryTableFactorCountCTEs(ctes []generator.CTE) int {
	total := 0
	for _, cte := range ctes {
		total++ // Count the CTE name itself as one table factor.
		total += queryTableFactorCountRecursive(cte.Query)
	}
	return total
}

func queryTableFactorCountRecursive(query *generator.SelectQuery) int {
	if query == nil {
		return 0
	}
	total := queryTableFactorCountFromClause(query.From)
	total += queryTableFactorCountCTEs(query.With)
	if query.From.BaseQuery != nil {
		total += queryTableFactorCountRecursive(query.From.BaseQuery)
	}
	for _, join := range query.From.Joins {
		if join.TableQuery != nil {
			total += queryTableFactorCountRecursive(join.TableQuery)
		}
	}
	for _, op := range query.SetOps {
		total += queryTableFactorCountRecursive(op.Query)
	}
	return total
}
