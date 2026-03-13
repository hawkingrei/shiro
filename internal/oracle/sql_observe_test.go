package oracle

import (
	"testing"

	"shiro/internal/db"
)

func TestRecordObservedResultSQLsStoresAllTrimmedKeys(t *testing.T) {
	features := db.SQLSubqueryFeatures{HasExistsSubquery: true}
	observed := recordObservedResultSQLs(nil, features, " SELECT 1 ", "", "\nSELECT 2\n")

	if len(observed) != 2 {
		t.Fatalf("expected 2 stored SQL keys, got %d", len(observed))
	}
	if got := observed["SELECT 1"]; got != features {
		t.Fatalf("unexpected features for SELECT 1: %+v", got)
	}
	if got := observed["SELECT 2"]; got != features {
		t.Fatalf("unexpected features for SELECT 2: %+v", got)
	}
}
