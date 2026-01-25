package db

import (
	"context"
	"fmt"

	"shiro/internal/config"
	"shiro/internal/util"
)

// EnsureDatabase creates the database if it does not exist.
func EnsureDatabase(ctx context.Context, dsn string, dbName string) error {
	if dbName == "" {
		return nil
	}
	exec, err := Open(config.AdminDSN(dsn))
	if err != nil {
		return err
	}
	defer util.CloseWithErr(exec, "db exec")
	_, err = exec.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	return err
}
