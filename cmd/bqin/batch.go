package main

import (
	"context"
	"flag"

	"github.com/google/subcommands"
	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
)

type batchCmd struct {
	config string
	queue  string
}

func (r *batchCmd) Name() string { return "batch" }
func (r *batchCmd) Synopsis() string {
	return "Load S3 objects into BigQuery based on messages currently in queue"
}

func (r *batchCmd) Usage() string {
	return `bqin batch [-config <config.yaml> -queue <queueName> -debug]

Load S3 objects into BigQuery based on messages currently in queue
Use this command to reprocess messages in the DLQ.
When all messages in the queue have been processed, the process exit with code 0.
`
}

func (r *batchCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&r.config, "config", "config.yaml", "config file path")
	f.StringVar(&r.queue, "queue", "", "sqs queue name")
}

func (r *batchCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	conf, err := bqin.LoadConfig(r.config)
	if err != nil {
		logger.Errorf("load config failed: %s", err)
		return subcommands.ExitFailure
	}
	err = bqin.NewApp(conf).Run(
		ctx,
		bqin.WithQueueName(r.queue),
		bqin.WithExitNoMessage(true),
		bqin.WithExitError(true),
	)
	if err != nil {
		logger.Errorf("run error: %v", err)
		return subcommands.ExitFailure
	}
	logger.Infof("all successed goodbye.")
	return subcommands.ExitSuccess
}
