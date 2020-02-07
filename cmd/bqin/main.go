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
	subcommands.Register(&cmdWrap{Command: &runCmd{}}, "")
	subcommands.Register(&cmdWrap{Command: &requestCmd{}}, "")
	flag.Parse()

	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
