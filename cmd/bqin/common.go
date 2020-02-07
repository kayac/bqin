package main

import (
	"context"
	"flag"
	"os"

	"github.com/google/subcommands"
	"github.com/kayac/bqin/internal/logger"
)

type cmdWrap struct {
	subcommands.Command
	debug bool
	help  bool
}

func (w *cmdWrap) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&w.debug, "debug", false, "enable debug logging")
	f.BoolVar(&w.help, "help", false, "show help")
	w.Command.SetFlags(f)
}

func (w *cmdWrap) Execute(ctx context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {

	if w.help {
		f.Parse([]string{os.Args[1]})
		return subcommands.HelpCommand().Execute(ctx, f, args...)
	}

	minLevel := logger.InfoLevel
	if w.debug {
		minLevel = logger.DebugLevel
	}
	logger.Setup(os.Stderr, minLevel)
	logger.Infof("bqin version: %s", version)
	return w.Command.Execute(ctx, f, args...)
}
