package main

import (
	"bufio"
	"context"
	"flag"
	"net/url"
	"os"

	"github.com/google/subcommands"
	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
)

type checkCmd struct {
	config string
}

func (r *checkCmd) Name() string { return "check" }
func (r *checkCmd) Synopsis() string {
	return "check rule"
}

func (r *checkCmd) Usage() string {
	return `bqin check [-config <config.yaml>]

Check rule matching.
By entering the AWS S3 resource URL line by line into the standard input, you can check whether the rule matches.
for example:
$ echo "s3://bucket.example.com/object/data.txt" | bqin check --config config.yaml
`
}

func (r *checkCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&r.config, "config", "config.yaml", "config file path")
}

func (r *checkCmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	conf, err := bqin.LoadConfig(r.config)
	if err != nil {
		logger.Errorf("load config failed: %s", err)
		return subcommands.ExitFailure
	}
	app := bqin.NewApp(conf)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		src, err := url.Parse(scanner.Text())
		if err != nil {
			logger.Errorf("url parse error:%s", err)
			continue
		}
		logger.Debugf("parsed url:%#v", src)
		jobs := app.Resolve([]*url.URL{src})
		if len(jobs) == 0 {
			logger.Errorf("no match rules")
			continue
		}
		for _, job := range jobs {
			logger.Infof("mach job: %s", job)
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Errorf("reading standard input error:%s", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}
