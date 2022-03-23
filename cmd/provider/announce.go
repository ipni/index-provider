package main

import (
	"net/http"

	"github.com/urfave/cli/v2"
)

var AnnounceCmd = &cli.Command{
	Name:   "announce",
	Usage:  "Publish an announcement message for the latest advertisement",
	Flags:  announceFlags,
	Action: announceCommand,
}

func announceCommand(cctx *cli.Context) error {
	req, err := http.NewRequestWithContext(cctx.Context, http.MethodPost, adminAPIFlagValue+"/admin/announce", nil)
	if err != nil {
		return err
	}

	cl := &http.Client{}
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		return errFromHttpResp(resp)
	}

	_, err = cctx.App.Writer.Write([]byte("Announced latest advertisement\n"))
	return err
}
