package runner

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (r *Runner) execOnConn(ctx context.Context, conn *sql.Conn, sql string) error {
	if err := r.validator.Validate(sql); err != nil {
		return err
	}
	_, err := conn.ExecContext(ctx, sql)
	return err
}

func (r *Runner) prepareConn(ctx context.Context, conn *sql.Conn, dbName string) error {
	if dbName == "" {
		return nil
	}
	return r.execOnConn(ctx, conn, fmt.Sprintf("USE %s", dbName))
}

func (r *Runner) execSQL(ctx context.Context, sql string) error {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	conn, err := r.exec.Conn(qctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := r.prepareConn(qctx, conn, r.cfg.Database); err != nil {
		return err
	}
	_, err = conn.ExecContext(qctx, sql)
	if err == nil {
		r.recordInsert(sql)
		return nil
	}
	return err
}

func (r *Runner) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, time.Duration(r.cfg.StatementTimeoutMs)*time.Millisecond)
}
