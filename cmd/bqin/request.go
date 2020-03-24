package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/subcommands"
	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
)

type requestCmd struct {
	config string
}

func (r *requestCmd) Name() string { return "request" }
func (r *requestCmd) Synopsis() string {
	return "Load S3 Object to BigQuery with requeset file (json)"
}

func (r *requestCmd) Usage() string {
	return `bqin request [-config <config.yaml> -debug] <request.json>

Load S3 Object to BigQuery according to request file (json) of specified format
It is assumed to be used for SQS messages that failed to process or communication check.
`
}

func (r *requestCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&r.config, "config", "config.yaml", "config file path")
}

func (r *requestCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if f.NArg() == 0 {
		logger.Errorf("request file is required.")
		return subcommands.ExitFailure
	}

	conf, err := bqin.LoadConfig(r.config)
	if err != nil {
		logger.Errorf("load config failed: %s", err)
		return subcommands.ExitFailure
	}
	app := bqin.NewApp(conf)

	for _, arg := range f.Args() {
		select {
		case <-ctx.Done():
			logger.Errorf("%s", ctx.Err())
			return subcommands.ExitFailure
		default:
		}
		file, err := os.Open(arg)
		if err != nil {
			logger.Errorf("%s", err)
			return subcommands.ExitFailure
		}
		decoder := json.NewDecoder(file)
		req := &bqin.ImportRequest{}
		if err := decoder.Decode(req); err != nil {
			logger.Errorf("can not parse %s :%s", arg, err)
			return subcommands.ExitFailure
		}
		if err := app.Process(ctx, req); err != nil {
			logger.Errorf("process %s failed :%s", arg, err)
			return subcommands.ExitFailure
		}
	}
	logger.Infof("all successed goodbye.")
	return subcommands.ExitSuccess
}
