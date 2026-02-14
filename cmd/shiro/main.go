package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"shiro/internal/config"
	"shiro/internal/db"
	"shiro/internal/runinfo"
	"shiro/internal/runner"
	"shiro/internal/util"

	"gopkg.in/yaml.v3"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	absConfigPath, absErr := filepath.Abs(*configPath)
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	if err := util.InitLogging(cfg.Logging.LogFile); err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logging: %v\n", err)
	}
	defer util.CloseLogging()
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	util.Infof("starting shiro with %d worker(s)", cfg.Workers)
	logRunInfo(cfg.RunInfo)
	if absErr != nil {
		util.Infof("config path: %s", *configPath)
	} else {
		util.Infof("config path: %s (abs: %s)", *configPath, absConfigPath)
		if info, statErr := os.Stat(absConfigPath); statErr == nil {
			util.Infof("config file: size=%d mtime=%s", info.Size(), info.ModTime().Format(time.RFC3339))
		} else {
			util.Infof("config file: stat failed: %v", statErr)
		}
	}
	if data, err := yaml.Marshal(&cfg); err == nil {
		util.Detailf("config:\n%s", string(data))
	}

	if cfg.Workers == 1 {
		if err := setGlobalTimeZone(cfg.DSN); err != nil {
			fmt.Fprintf(os.Stderr, "failed to set global time_zone: %v\n", err)
			os.Exit(1)
		}
		if err := db.EnsureDatabase(context.Background(), cfg.DSN, cfg.Database); err != nil {
			fmt.Fprintf(os.Stderr, "failed to ensure database: %v\n", err)
			os.Exit(1)
		}
		exec, err := db.Open(cfg.DSN)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect to db: %v\n", err)
			os.Exit(1)
		}
		defer util.CloseWithErr(exec, "db exec")

		r := runner.New(cfg, exec)
		ctx := context.Background()
		if err := r.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	var wg sync.WaitGroup
	errCh := make(chan error, cfg.Workers)
	if err := setGlobalTimeZone(cfg.DSN); err != nil {
		fmt.Fprintf(os.Stderr, "failed to set global time_zone: %v\n", err)
		os.Exit(1)
	}
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerCfg := cfg
			workerCfg.Database = fmt.Sprintf("%s_w%d", cfg.Database, worker)
			workerCfg.DSN = config.UpdateDatabaseInDSN(workerCfg.DSN, workerCfg.Database)
			if err := db.EnsureDatabase(context.Background(), workerCfg.DSN, workerCfg.Database); err != nil {
				errCh <- err
				return
			}
			exec, err := db.Open(workerCfg.DSN)
			if err != nil {
				errCh <- err
				return
			}
			defer util.CloseWithErr(exec, "db exec")
			util.Infof("worker %d using database %s", worker, workerCfg.Database)
			r := runner.New(workerCfg, exec)
			if err := r.Run(context.Background()); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
			os.Exit(1)
		}
	}
}

func setGlobalTimeZone(dsn string) error {
	exec, err := db.Open(config.AdminDSN(dsn))
	if err != nil {
		return err
	}
	defer util.CloseWithErr(exec, "db exec")
	return setGlobalTimeZoneOnConn(exec)
}

func setGlobalTimeZoneOnConn(exec *db.DB) error {
	tz := time.Now().Format("-07:00")
	util.Infof("timezone: local=%s offset=%s", time.Local.String(), tz)
	_, err := exec.ExecContext(context.Background(), fmt.Sprintf("SET GLOBAL time_zone='%s'", tz))
	return err
}

func logRunInfo(info *runinfo.BasicInfo) {
	if info == nil || info.IsZero() {
		return
	}
	util.Infof("run info: ci=%t provider=%s repo=%s branch=%s commit=%s workflow=%s job=%s run_id=%s run_number=%s event=%s pr=%s actor=%s build_url=%s",
		info.CI,
		info.Provider,
		info.Repository,
		info.Branch,
		info.Commit,
		info.Workflow,
		info.Job,
		info.RunID,
		info.RunNumber,
		info.Event,
		info.PullRequest,
		info.Actor,
		info.BuildURL,
	)
}
