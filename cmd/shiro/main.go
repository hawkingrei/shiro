package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"shiro/internal/config"
	"shiro/internal/db"
	"shiro/internal/runner"
	"shiro/internal/util"

	"gopkg.in/yaml.v3"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	util.Infof("starting shiro with %d worker(s)", cfg.Workers)
	if data, err := yaml.Marshal(&cfg); err == nil {
		util.Highlightf("config:\n%s", string(data))
	}

	if cfg.Workers == 1 {
		exec, err := db.Open(cfg.DSN)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect to db: %v\n", err)
			os.Exit(1)
		}
		defer exec.Close()

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
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerCfg := cfg
			workerCfg.Database = fmt.Sprintf("%s_w%d", cfg.Database, worker)
			exec, err := db.Open(workerCfg.DSN)
			if err != nil {
				errCh <- err
				return
			}
			defer exec.Close()
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
