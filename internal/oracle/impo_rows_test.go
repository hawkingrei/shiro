package oracle

import "testing"

func TestCompareRowSets(t *testing.T) {
	base := rowSet{columns: 2, rows: []string{"a\x1f1", "b\x1f2", "b\x1f2"}}
	other := rowSet{columns: 2, rows: []string{"a\x1f1", "b\x1f2", "b\x1f2", "c\x1f3"}}
	if got, _ := compareRowSets(base, other); got != -1 {
		t.Fatalf("expected base subset, got %d", got)
	}
	if got, _ := compareRowSets(other, base); got != 1 {
		t.Fatalf("expected base contains other, got %d", got)
	}
	if got, _ := compareRowSets(base, base); got != 0 {
		t.Fatalf("expected equal, got %d", got)
	}
	if got, _ := compareRowSets(rowSet{columns: 1}, rowSet{columns: 2}); got != 2 {
		t.Fatalf("expected incomparable columns, got %d", got)
	}
	if got, _ := compareRowSets(rowSet{}, rowSet{}); got != 0 {
		t.Fatalf("expected empty equal, got %d", got)
	}
	if got, _ := compareRowSets(rowSet{}, base); got != -1 {
		t.Fatalf("expected empty subset, got %d", got)
	}
	if got, _ := compareRowSets(base, rowSet{}); got != 1 {
		t.Fatalf("expected base contains empty, got %d", got)
	}
}

func TestImplicationOK(t *testing.T) {
	if !implicationOK(true, -1) {
		t.Fatalf("expected upper implication for subset")
	}
	if !implicationOK(true, 0) {
		t.Fatalf("expected upper implication for equal")
	}
	if implicationOK(true, 1) {
		t.Fatalf("unexpected upper implication for superset")
	}
	if !implicationOK(false, 1) {
		t.Fatalf("expected lower implication for superset")
	}
	if !implicationOK(false, 0) {
		t.Fatalf("expected lower implication for equal")
	}
	if implicationOK(false, -1) {
		t.Fatalf("unexpected lower implication for subset")
	}
}
