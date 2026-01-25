package runner

import (
	"context"
	"fmt"
	"strings"

	"shiro/internal/config"
	"shiro/internal/db"
	"shiro/internal/generator"
	"shiro/internal/schema"
	"shiro/internal/util"
)

func (r *Runner) recordInsert(sql string) {
	trimmed := strings.TrimSpace(sql)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") {
		return
	}
	if r.cfg.MaxInsertStatements <= 0 {
		return
	}
	if len(r.insertLog) >= r.cfg.MaxInsertStatements {
		r.insertLog = r.insertLog[1:]
	}
	r.insertLog = append(r.insertLog, trimmed)
}

func (r *Runner) rotateDatabase(ctx context.Context) error {
	seq := globalDBSeq.Add(1)
	r.cfg.Database = fmt.Sprintf("%s_r%d", r.baseDB, seq)
	if err := ensureDatabaseExists(ctx, r.cfg.DSN, r.cfg.Database); err != nil {
		return err
	}
	r.cfg.DSN = config.UpdateDatabaseInDSN(r.cfg.DSN, r.cfg.Database)
	util.CloseWithErr(r.exec, "db exec")
	exec, err := db.Open(r.cfg.DSN)
	if err != nil {
		return err
	}
	r.exec = exec
	r.state = &schema.State{}
	r.genMu.Lock()
	r.gen = generator.New(r.cfg, r.state, r.cfg.Seed+seq)
	r.genMu.Unlock()
	r.exec.Validate = r.validator.Validate
	r.exec.Observe = r.observeSQL
	r.insertLog = nil
	if r.cfg.QPG.Enabled {
		r.qpgMu.Lock()
		r.qpgState = newQPGState(r.cfg.QPG)
		r.qpgMu.Unlock()
	}
	if err := r.setupDatabase(ctx); err != nil {
		return err
	}
	return r.initState(ctx)
}

func ensureDatabaseExists(ctx context.Context, dsn string, dbName string) error {
	if dbName == "" {
		return nil
	}
	exec, err := db.Open(config.AdminDSN(dsn))
	if err != nil {
		return err
	}
	defer util.CloseWithErr(exec, "db exec")
	_, err = exec.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	return err
}
