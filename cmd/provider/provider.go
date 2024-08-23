package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	provider "github.com/ipni/index-provider"
	"github.com/urfave/cli/v2"
)

func main() {
	os.Exit(run())
}

func run() int {
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

	app := &cli.App{
		Name:    "provider",
		Usage:   "Indexer Reference Provider Implementation",
		Version: provider.Version,
		Commands: []*cli.Command{
			AnnounceCmd,
			AnnounceHttpCmd,
			ConnectCmd,
			DaemonCmd,
			ImportCmd,
			IndexCmd,
			InitCmd,
			ListCmd,
			RemoveCmd,
			Mirror.Command,
		},
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Fprintln(app.ErrWriter, err)
		return 1
	}
	return 0
}
