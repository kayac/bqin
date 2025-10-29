package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/google/subcommands"
	"github.com/kayac/bqin"
)

var (
	buildDate string
)

type versionCmd struct{}

func (_ *versionCmd) Name() string { return "version" }
func (_ *versionCmd) Synopsis() string {
	return "show version information"
}

func (_ *versionCmd) Usage() string {
	return `bqin version

`
}

func (_ *versionCmd) SetFlags(_ *flag.FlagSet) {}

func (_ *versionCmd) Execute(_ context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	fmt.Println("bqin is a BigQuery data importer with AWS S3 and SQS messaging.")
	fmt.Println("version:", bqin.Version)
	fmt.Println("build:", buildDate)
	return subcommands.ExitSuccess
}
