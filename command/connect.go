package command

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	adminserver "github.com/filecoin-project/indexer-reference-provider/server/admin/http"
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
	return sendRequest(cctx, req)
}

func sendRequest(cctx *cli.Context, req *http.Request) error {
	cl := &http.Client{}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		statusText := http.StatusText(resp.StatusCode)
		var errRes adminserver.ErrorRes
		if _, err := errRes.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf(
				"failed to connect to peer with response %s. cannot decode error response: %v",
				http.StatusText(resp.StatusCode), err)
		}
		return fmt.Errorf("%s %s", statusText, errRes.Message)
	}

	log.Infof("Successfully connected to peer")
	var res adminserver.ConnectRes
	if _, err := res.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("received OK response from server but cannot decode response body. %v", err)
	}
	_, err = cctx.App.Writer.Write([]byte("Successfully connected to Peer"))
	return err
}
