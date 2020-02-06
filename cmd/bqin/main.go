package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hashicorp/logutils"
	"github.com/kayac/bqin"
)

var (
	version   string
	buildDate string
)

func main() {
	var (
		config      string
		debug       bool
		showVersion bool
	)
	flag.StringVar(&config, "config", "config.yaml", "config file path")
	flag.StringVar(&config, "c", "config.yaml", "config file path")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.BoolVar(&debug, "d", false, "enable debug logging")
	flag.BoolVar(&showVersion, "version", false, "show version")
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.Parse()

	if showVersion {
		fmt.Println("bqin is a BigQuery data importer with AWS S3 and SQS messaging.")
		fmt.Println("version:", version)
		fmt.Println("build:", buildDate)
		return
	}

	minLevel := "info"
	if debug {
		minLevel = "debug"
	}
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"debug", "info", "error"},
		MinLevel: logutils.LogLevel(minLevel),
		Writer:   os.Stderr,
	}
	log.SetOutput(filter)
	log.Println("[info] bqin version:", version)

	conf, err := bqin.LoadConfig(config)
	if err != nil {
		log.Println("[error] load config failed:", err)
		os.Exit(1)
	}
	app, err := bqin.NewApp(conf)
	if err != nil {
		log.Println("[error] setup condition failed:", err)
		os.Exit(1)
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

		log.Println("[info] start sutdown...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := app.Shutdown(ctx); err != nil {
			log.Printf("[error] shutdown failed: %v", err)
		}
		log.Println("[info] finish shutdown.")
		close(idleClosed)
	}()

	if err := app.ReceiveAndProcess(); err != nil {
		log.Fatalf("[error] ReceiveAndProcess error: %v", err)
	}
	<-idleClosed
	log.Println("[info] goodbye.")
}
