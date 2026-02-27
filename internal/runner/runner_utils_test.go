package runner

import (
	"testing"

	"shiro/internal/schema"
)

func TestShouldApplyTiFlashReplica(t *testing.T) {
	base := &schema.Table{Name: "t1"}
	view := &schema.Table{Name: "v1", IsView: true}

	if !shouldApplyTiFlashReplica(base, 1, false, false) {
		t.Fatalf("expected base table with positive replicas to be eligible")
	}
	if shouldApplyTiFlashReplica(base, 0, false, false) {
		t.Fatalf("did not expect zero replicas to be eligible")
	}
	if shouldApplyTiFlashReplica(view, 1, false, false) {
		t.Fatalf("did not expect view to be eligible")
	}
	if shouldApplyTiFlashReplica(nil, 1, false, false) {
		t.Fatalf("did not expect nil table to be eligible")
	}
	if shouldApplyTiFlashReplica(base, 1, true, false) {
		t.Fatalf("did not expect disable_mpp=true to be eligible")
	}
	if shouldApplyTiFlashReplica(base, 1, false, true) {
		t.Fatalf("did not expect plan_cache_only=true to be eligible")
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

func TestForeignKeyCompatibilitySQL(t *testing.T) {
	fk := &schema.ForeignKey{
		Name:      "fk_43",
		Table:     "t4",
		Column:    "id",
		RefTable:  "t0",
		RefColumn: "id",
	}
	got := foreignKeyCompatibilitySQL(fk)
	want := "SELECT 1 FROM t4 c LEFT JOIN t0 p ON c.id <=> p.id WHERE c.id IS NOT NULL AND p.id IS NULL LIMIT 1"
	if got != want {
		t.Fatalf("unexpected fk compatibility sql: got %q want %q", got, want)
	}
}

func TestForeignKeyCompatibilitySQLEmpty(t *testing.T) {
	if foreignKeyCompatibilitySQL(nil) != "" {
		t.Fatalf("expected empty sql for nil fk")
	}
	empty := &schema.ForeignKey{}
	if foreignKeyCompatibilitySQL(empty) != "" {
		t.Fatalf("expected empty sql for empty fk")
	}
}
