package runner

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"shiro/internal/schema"
)

func foreignKeyCompatibilitySQL(fk *schema.ForeignKey) string {
	if fk == nil {
		return ""
	}
	table := strings.TrimSpace(fk.Table)
	column := strings.TrimSpace(fk.Column)
	refTable := strings.TrimSpace(fk.RefTable)
	refColumn := strings.TrimSpace(fk.RefColumn)
	if table == "" || column == "" || refTable == "" || refColumn == "" {
		return ""
	}
	return fmt.Sprintf(
		"SELECT 1 FROM %s c LEFT JOIN %s p ON c.%s <=> p.%s WHERE c.%s IS NOT NULL AND p.%s IS NULL LIMIT 1",
		table, refTable, column, refColumn, column, refColumn,
	)
}

// isForeignKeyDataCompatible reports whether existing child rows satisfy fk.
// Incompatible rows are detected by probing for any child value that cannot
// find a matching parent value.
func (r *Runner) isForeignKeyDataCompatible(ctx context.Context, fk *schema.ForeignKey) (bool, error) {
	if r == nil || r.exec == nil {
		return false, nil
	}
	sqlText := foreignKeyCompatibilitySQL(fk)
	if sqlText == "" {
		return false, nil
	}
	var marker int
	err := r.exec.QueryRowContext(ctx, sqlText).Scan(&marker)
	if err == sql.ErrNoRows {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}
