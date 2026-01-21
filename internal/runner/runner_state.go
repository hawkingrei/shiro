package runner

import (
	"context"
	"fmt"
	"strings"

	"shiro/internal/generator"
	"shiro/internal/schema"
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
	r.state = &schema.State{}
	r.gen = generator.New(r.cfg, r.state, r.cfg.Seed+seq)
	r.exec.Validate = r.validator.Validate
	r.exec.Observe = r.observeSQL
	r.insertLog = nil
	if r.cfg.QPG.Enabled {
		r.qpgState = newQPGState(r.cfg.QPG)
	}
	if err := r.setupDatabase(ctx); err != nil {
		return err
	}
	return r.initState(ctx)
}
