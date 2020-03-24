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
			Command: &requestCmd{},
		},
	}, "")
	flag.Parse()

	os.Exit(int(subcommands.Execute(context.Background())))
}
