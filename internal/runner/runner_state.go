package runner

import (
	"context"
	"fmt"
	"strings"
	"time"

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
	r.dbSeq = seq
	r.cfg.Database = fmt.Sprintf("%s_r%d", r.baseDB, seq)
	if err := db.EnsureDatabase(ctx, r.cfg.DSN, r.cfg.Database); err != nil {
		return err
	}
	r.applyRuntimeToggles()
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

func (r *Runner) rotateDatabaseWithRetry(ctx context.Context) error {
	rotateTimeout := time.Duration(r.cfg.StatementTimeoutMs) * time.Millisecond
	if rotateTimeout < 60*time.Second {
		rotateTimeout = 60 * time.Second
	}
	attempts := 2
	backoff := 500 * time.Millisecond
	var lastErr error
	for i := 0; i < attempts; i++ {
		rctx, cancel := context.WithTimeout(ctx, rotateTimeout)
		lastErr = r.rotateDatabase(rctx)
		cancel()
		if lastErr == nil {
			return nil
		}
		if i+1 < attempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}
	return lastErr
}

func (r *Runner) applyRuntimeToggles() {
	if r == nil {
		return
	}
	tqsEnabled := r.baseTQSEnabled
	if r.baseTQSEnabled && r.baseDQEWeight > 0 {
		tqsEnabled = r.dbSeq%2 == 0
	}
	r.cfg.TQS.Enabled = tqsEnabled
	r.cfg.Weights.Actions = r.baseActions
	r.cfg.Weights.DML = r.baseDMLWeights
	r.cfg.Weights.Oracles.DQE = r.baseDQEWeight
	r.cfg.Features.DSG = r.baseDSGEnabled
	if tqsEnabled {
		r.cfg.Features.DSG = true
		r.cfg.Weights.Actions.DML = 0
		if r.cfg.Weights.Oracles.DQE > 0 {
			util.Detailf("tqs config adjusted: disable DQE oracle")
		}
		r.cfg.Weights.Oracles.DQE = 0
		if r.cfg.Features.Views {
			if r.cfg.Weights.Actions.DDL <= 0 {
				r.cfg.Weights.Actions.DDL = 1
			}
		} else {
			r.cfg.Weights.Actions.DDL = 0
		}
	} else if r.baseTQSEnabled && r.baseDQEWeight > 0 {
		util.Detailf("tqs config adjusted: disable TQS because DQE is enabled")
	}
	if r.gen != nil {
		r.gen.Config = r.cfg
	}
}
