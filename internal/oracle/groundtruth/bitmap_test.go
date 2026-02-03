package groundtruth

import "testing"

func TestBitmapSetAndOps(t *testing.T) {
	bm := NewBitmap(128)
	bm.Set(1)
	bm.Set(64)
	bm.Set(127)

	if !bm.Has(1) || !bm.Has(64) || !bm.Has(127) {
		t.Fatalf("expected bits to be set")
	}

	bm2 := NewBitmap(128)
	bm2.Set(64)
	bm2.Set(3)

	inter := bm.And(bm2)
	if !inter.Has(64) || inter.Has(1) || inter.Has(127) {
		t.Fatalf("unexpected intersection result")
	}

	diff := bm.Sub(bm2)
	if diff.Has(64) || !diff.Has(1) || !diff.Has(127) {
		t.Fatalf("unexpected difference result")
	}
}

func TestEqualityJoin(t *testing.T) {
	truth := NewSchemaTruth(10)
	truth.AddTable("t1")
	truth.AddTable("t2")

	truth.AddColumnValue("t1", "a", "int", "1", 1)
	truth.AddColumnValue("t1", "a", "int", "2", 2)
	truth.AddColumnValue("t2", "b", "int", "2", 2)
	truth.AddColumnValue("t2", "b", "int", "3", 3)

	exec := JoinTruthExecutor{Truth: truth}
	rows := exec.equalityJoin("t1", []string{"a"}, "t2", []string{"b"})
	if !rows.Has(2) || rows.Has(1) || rows.Has(3) {
		t.Fatalf("unexpected join rows")
	}
}
