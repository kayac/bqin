package main

import (
	"context"
	"flag"

	"github.com/google/subcommands"
	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
)

type runCmd struct {
	config string
}

func (r *runCmd) Name() string { return "run" }
func (r *runCmd) Synopsis() string {
	return "Start bqin normally"
}

func (r *runCmd) Usage() string {
	return `bqin run [-config <config.yaml> -debug]

Wait for SQS message reception and load the target S3 Object into BigQuery as soon as it is received.
`
}

func (r *runCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&r.config, "config", "config.yaml", "config file path")
}

func (r *runCmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	conf, err := bqin.LoadConfig(r.config)
	if err != nil {
		logger.Errorf("load config failed: %s", err)
		return subcommands.ExitFailure
	}
	if err := bqin.NewApp(conf).Run(ctx); err != nil {
		logger.Errorf("run error: %v", err)
		return subcommands.ExitFailure
	}
	logger.Infof("goodbye.")
	return subcommands.ExitSuccess
}
