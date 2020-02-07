package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	app, err := bqin.NewApp(conf)
	if err != nil {
		logger.Errorf("setup condition failed: %s", err)
		return subcommands.ExitFailure
	}
	idleClosed := make(chan struct{})
	go func() {
		trapSignals := []os.Signal{
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT,
		}
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, trapSignals...)
		<-sigint

		logger.Infof("start sutdown...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := app.Shutdown(ctx); err != nil {
			logger.Errorf("shutdown failed: %v", err)
		} else {
			logger.Infof("finish shutdown.")
		}
		close(idleClosed)
	}()

	if err := app.ReceiveAndProcess(); err != nil {
		if err == context.Canceled {
			logger.Infof("canceled")
			return subcommands.ExitSuccess
		}
		logger.Errorf("run error: %v", err)
		return subcommands.ExitFailure
	}
	<-idleClosed
	logger.Infof("goodbye.")
	return subcommands.ExitSuccess
}
