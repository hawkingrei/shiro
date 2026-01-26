package repro

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"shiro/internal/config"
	"shiro/internal/db"
	"shiro/internal/util"
)

// Options configures a reproduction run.
type Options struct {
	CaseDir  string
	DSN      string
	Database string
	UseMin   bool
}

// Run executes the reproduction flow for a case directory.
func Run(ctx context.Context, opts Options) error {
	if opts.CaseDir == "" {
		return fmt.Errorf("case_dir is required")
	}
	if opts.DSN == "" {
		return fmt.Errorf("dsn is required")
	}
	if opts.Database == "" {
		opts.Database = "shiro_repro"
	}
	if err := db.EnsureDatabase(ctx, opts.DSN, opts.Database); err != nil {
		return err
	}
	dsn := config.UpdateDatabaseInDSN(opts.DSN, opts.Database)
	exec, err := db.Open(dsn)
	if err != nil {
		return err
	}
	defer util.CloseWithErr(exec, "repro db")

	fmt.Printf("database=%s dsn=%s\n", opts.Database, dsn)
	printVersion(ctx, exec)

	schemaPath := filepath.Join(opts.CaseDir, "schema.sql")
	if err := execSQLFile(ctx, exec, schemaPath); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	insertsPath := filepath.Join(opts.CaseDir, "inserts.sql")
	if err := execSQLFile(ctx, exec, insertsPath); err != nil {
		return fmt.Errorf("inserts: %w", err)
	}
	casePath, label := pickCaseSQL(opts.CaseDir, opts.UseMin)
	if err := execSQLFile(ctx, exec, casePath); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	return nil
}

func pickCaseSQL(caseDir string, useMin bool) (path string, label string) {
	if useMin {
		minPath := filepath.Join(caseDir, "min", "repro.sql")
		if fileExists(minPath) {
			return minPath, "min_repro"
		}
	}
	return filepath.Join(caseDir, "case.sql"), "case"
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func execSQLFile(ctx context.Context, exec *db.DB, path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	statements := splitSQL(string(content))
	if len(statements) == 0 {
		return nil
	}
	fmt.Printf("exec_file=%s statements=%d\n", path, len(statements))
	for idx, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := exec.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("stmt=%d err=%v sql=%s", idx+1, err, strings.TrimSpace(stmt))
		}
	}
	return nil
}

func printVersion(ctx context.Context, exec *db.DB) {
	row := exec.QueryRowContext(ctx, "SELECT tidb_version()")
	var v string
	if err := row.Scan(&v); err == nil && strings.TrimSpace(v) != "" {
		fmt.Printf("tidb_version=%s\n", strings.ReplaceAll(v, "\n", " "))
		return
	}
	row = exec.QueryRowContext(ctx, "SELECT VERSION()")
	if err := row.Scan(&v); err == nil && strings.TrimSpace(v) != "" {
		fmt.Printf("version=%s\n", v)
	}
}
