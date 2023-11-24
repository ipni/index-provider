package main

import (
	"fmt"
	"net/http"

	adminserver "github.com/ipni/index-provider/server/admin/http"
	"github.com/urfave/cli/v2"
)

var ConnectCmd = &cli.Command{
	Name:   "connect",
	Usage:  "Connects to an indexer through its multiaddr",
	Flags:  connectFlags,
	Action: connectCommand,
}

var connectFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "indexermaddr",
		Usage:    "Indexer multiaddr to connect",
		Aliases:  []string{"imaddr"},
		Required: true,
	},
	adminAPIFlag,
}

func connectCommand(cctx *cli.Context) error {
	iaddr := cctx.String("indexermaddr")
	req := &adminserver.ConnectReq{Maddr: iaddr}
	resp, err := doHttpPostReq(cctx.Context, adminAPIFlagValue+"/admin/connect", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		return errFromHttpResp(resp)
	}

	log.Infof("connected to peer successfully")
	var res adminserver.ConnectRes
	if _, err := res.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("received OK response from server but cannot decode response body: %w", err)
	}
	_, err = cctx.App.Writer.Write([]byte("Connected to peer successfully"))
	return err
}
