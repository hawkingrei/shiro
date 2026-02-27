package runner

import (
	"testing"

	"shiro/internal/schema"
)

func TestShouldApplyTiFlashReplica(t *testing.T) {
	base := &schema.Table{Name: "t1"}
	view := &schema.Table{Name: "v1", IsView: true}

	if !shouldApplyTiFlashReplica(base, 1, false) {
		t.Fatalf("expected base table with positive replicas to be eligible")
	}
	if shouldApplyTiFlashReplica(base, 0, false) {
		t.Fatalf("did not expect zero replicas to be eligible")
	}
	if shouldApplyTiFlashReplica(view, 1, false) {
		t.Fatalf("did not expect view to be eligible")
	}
	if shouldApplyTiFlashReplica(nil, 1, false) {
		t.Fatalf("did not expect nil table to be eligible")
	}
	if shouldApplyTiFlashReplica(base, 1, true) {
		t.Fatalf("did not expect disable_mpp=true to be eligible")
	}
}

func TestTiFlashReplicaSQL(t *testing.T) {
	got := tiFlashReplicaSQL("t1", 1)
	want := "ALTER TABLE t1 SET TIFLASH REPLICA 1"
	if got != want {
		t.Fatalf("unexpected tiflash sql: got %q want %q", got, want)
	}
}

func TestTiFlashReplicaPendingSQL(t *testing.T) {
	got := tiFlashReplicaPendingSQL()
	want := "SELECT COUNT(*) FROM information_schema.tiflash_replica WHERE AVAILABLE = 0"
	if got != want {
		t.Fatalf("unexpected tiflash pending sql: got %q want %q", got, want)
	}
}
