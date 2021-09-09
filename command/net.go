package command

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	adminserver "github.com/filecoin-project/indexer-reference-provider/server/http"
	"github.com/urfave/cli/v2"
)

var ConnectCmd = &cli.Command{
	Name:   "connect",
	Usage:  "Connects to an indexer through its multiaddr",
	Flags:  connectFlags,
	Action: connectCommand,
}

func connectCommand(cctx *cli.Context) error {
	iaddr := cctx.String("indexermaddr")
	adminaddr := cctx.String("listen-admin")
	data, err := json.Marshal(&adminserver.ConnectReq{Maddr: iaddr})
	if err != nil {
		return err
	}
	reqURL := adminaddr + "/admin/connect"
	req, err := http.NewRequestWithContext(cctx.Context, http.MethodPost, reqURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	return sendRequest(req)
}

func sendRequest(req *http.Request) error {
	cl := &http.Client{}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("connecting to peer failed: %v", http.StatusText(resp.StatusCode))
	}
	log.Infof("Successfully connected to peer")
	return nil
}
