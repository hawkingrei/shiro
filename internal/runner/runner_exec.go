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

func (r *Runner) execSQL(ctx context.Context, sql string) error {
	qctx, cancel := r.withTimeout(ctx)
	defer cancel()
	conn, err := r.exec.Conn(qctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := r.execOnConn(qctx, conn, fmt.Sprintf("USE %s", r.cfg.Database)); err != nil {
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
