package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/google/subcommands"
	"github.com/kayac/bqin"
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
	logger.Infof("bqin version: %s", bqin.Version)

	return w.Command.Execute(ctx, f, args...)
}

type signalTrapper struct {
	subcommands.Command
}

func (w *signalTrapper) Execute(ctx context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {

	trapSignals := []os.Signal{
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	}
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, trapSignals...)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sig := <-sigint
		logger.Infof("Got signal: %s(%d)", sig, sig)
		logger.Infof("sutdown...")
		cancel()
	}()
	return w.Command.Execute(ctx, f, args...)
}
