package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"shiro/internal/repro"
)

func main() {
	caseDir := flag.String("case_dir", "", "path to case directory")
	dsn := flag.String("dsn", "", "database DSN")
	database := flag.String("database", "shiro_repro", "database name for reproduction")
	useMin := flag.Bool("use_min", true, "prefer min/repro.sql if present")
	flag.Parse()

	if *caseDir == "" || *dsn == "" {
		fmt.Fprintln(os.Stderr, "case_dir and dsn are required")
		flag.Usage()
		os.Exit(1)
	}

	opts := repro.Options{
		CaseDir:  *caseDir,
		DSN:      *dsn,
		Database: *database,
		UseMin:   *useMin,
	}
	if err := repro.Run(context.Background(), opts); err != nil {
		fmt.Fprintf(os.Stderr, "repro failed: %v\n", err)
		os.Exit(1)
	}
}
