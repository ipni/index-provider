package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/filecoin-project/indexer-reference-provider/command"
	"github.com/filecoin-project/indexer-reference-provider/internal/version"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("reference-provider")

func main() {
	// Set up a context that is canceled when the command is interrupted
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a signal handler to cancel the context
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)
		select {
		case <-interrupt:
			cancel()
			fmt.Println("Received interrupt signal, shutting down...")
			fmt.Println("(Hit CTRL-C again to force-shutdown the daemon.)")
		case <-ctx.Done():
		}
		// Allow any further SIGTERM or SIGING to kill the process
		signal.Stop(interrupt)
	}()

	// TODO parameterize log level
	if err := logging.SetLogLevel("*", "info"); err != nil {
		log.Fatal(err)
	}

	app := &cli.App{
		Name:    "provider",
		Usage:   "Indexer Reference Provider Implementation",
		Version: version.String(),
		Commands: []*cli.Command{
			command.DaemonCmd,
			command.InitCmd,
			command.ConnectCmd,
		},
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		log.Fatal(err)
	}
}
