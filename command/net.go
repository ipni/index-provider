package command

import (
	"github.com/urfave/cli/v2"
)

var ConnectCmd = &cli.Command{
	Name:   "connect",
	Usage:  "Connects to an indexer through its multiaddr",
	Flags:  DaemonFlags,
	Action: connectCommand,
}

func connectCommand(cctx *cli.Context) error {
	panic("not implemented")
}
