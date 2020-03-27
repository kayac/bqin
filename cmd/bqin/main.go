package main

import (
	"context"
	"flag"
	"os"

	"github.com/google/subcommands"
)

func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(&versionCmd{}, "")
	subcommands.Register(&cmdWrap{
		Command: &signalTrapper{
			Command: &runCmd{},
		},
	}, "")
	subcommands.Register(&cmdWrap{
		Command: &signalTrapper{
			Command: &batchCmd{},
		},
	}, "")
	subcommands.Register(&cmdWrap{
		Command: &checkCmd{},
	}, "")
	flag.Parse()

	os.Exit(int(subcommands.Execute(context.Background())))
}
