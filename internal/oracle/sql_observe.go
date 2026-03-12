package oracle

import (
	"strings"

	"shiro/internal/db"
	"shiro/internal/generator"
)

func sqlSubqueryFeaturesFromQuery(query *generator.SelectQuery) db.SQLSubqueryFeatures {
	if query == nil {
		return db.SQLSubqueryFeatures{}
	}
	return sqlSubqueryFeaturesFromQueryFeatures(generator.AnalyzeQueryFeatures(query))
}

func sqlSubqueryFeaturesFromQueryFeatures(features generator.QueryFeatures) db.SQLSubqueryFeatures {
	return db.SQLSubqueryFeatures{
		HasInSubquery:     features.HasInSubquery,
		HasNotInSubquery:  features.HasNotInSubquery,
		HasInList:         features.HasInList,
		HasNotInList:      features.HasNotInList,
		HasExistsSubquery: features.HasExistsSubquery,
		HasNotExists:      features.HasNotExistsSubquery,
	}
}

func mergeSQLSubqueryFeatures(features ...db.SQLSubqueryFeatures) db.SQLSubqueryFeatures {
	merged := db.SQLSubqueryFeatures{}
	for _, feature := range features {
		merged.HasInSubquery = merged.HasInSubquery || feature.HasInSubquery
		merged.HasNotInSubquery = merged.HasNotInSubquery || feature.HasNotInSubquery
		merged.HasInList = merged.HasInList || feature.HasInList
		merged.HasNotInList = merged.HasNotInList || feature.HasNotInList
		merged.HasExistsSubquery = merged.HasExistsSubquery || feature.HasExistsSubquery
		merged.HasNotExists = merged.HasNotExists || feature.HasNotExists
	}
	return merged
}

func recordObservedExecSQL(exec *db.DB, sql string, features db.SQLSubqueryFeatures) {
	if exec == nil {
		return
	}
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return
	}
	exec.RegisterObservedSQLFeatures(trimmed, features)
}

func recordObservedResultSQL(observed map[string]db.SQLSubqueryFeatures, sql string, features db.SQLSubqueryFeatures) map[string]db.SQLSubqueryFeatures {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return observed
	}
	if observed == nil {
		observed = make(map[string]db.SQLSubqueryFeatures)
	}
	observed[trimmed] = features
	return observed
}
