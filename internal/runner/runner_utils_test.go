package runner

import (
	"testing"

	"shiro/internal/schema"
)

func TestShouldApplyTiFlashReplica(t *testing.T) {
	base := &schema.Table{Name: "t1"}
	view := &schema.Table{Name: "v1", IsView: true}

	if !shouldApplyTiFlashReplica(base, 1) {
		t.Fatalf("expected base table with positive replicas to be eligible")
	}
	if shouldApplyTiFlashReplica(base, 0) {
		t.Fatalf("did not expect zero replicas to be eligible")
	}
	if shouldApplyTiFlashReplica(view, 1) {
		t.Fatalf("did not expect view to be eligible")
	}
	if shouldApplyTiFlashReplica(nil, 1) {
		t.Fatalf("did not expect nil table to be eligible")
	}
}

func TestTiFlashReplicaSQL(t *testing.T) {
	got := tiFlashReplicaSQL("t1", 1)
	want := "ALTER TABLE t1 SET TIFLASH REPLICA 1"
	if got != want {
		t.Fatalf("unexpected tiflash sql: got %q want %q", got, want)
	}
}

func TestTiFlashReplicaReadySQL(t *testing.T) {
	got := tiFlashReplicaReadySQL("t1")
	want := "SELECT IFNULL(MAX(CASE WHEN AVAILABLE = 1 THEN 1 ELSE 0 END), 0) FROM information_schema.TIFLASH_REPLICA WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 't1'"
	if got != want {
		t.Fatalf("unexpected tiflash ready sql: got %q want %q", got, want)
	}
}
