package main

import (
	"fmt"

	httpc "github.com/ipni/go-libipni/ingest/client"
	"github.com/ipni/index-provider/cmd/provider/internal/config"
	"github.com/urfave/cli/v2"
)

var RegisterCmd = &cli.Command{
	Name:   "register",
	Usage:  "Register provider information with an indexer that trusts the provider",
	Flags:  registerFlags,
	Action: registerCommand,
}

func registerCommand(cctx *cli.Context) error {
	cfg, err := config.Load("")
	if err != nil {
		return err
	}

	peerID, privKey, err := cfg.Identity.DecodeOrCreate(cctx.App.Writer)
	if err != nil {
		return err
	}

	client, err := httpc.New(cctx.String("indexer"))
	if err != nil {
		return err
	}

	err = client.Register(cctx.Context, peerID, privKey, cctx.StringSlice("addr"))
	if err != nil {
		return fmt.Errorf("failed to register providers: %s", err)
	}

	fmt.Println("Registered provider", cfg.Identity.PeerID, "at indexer", cctx.String("indexer"))
	return nil
}
