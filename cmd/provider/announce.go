package main

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/urfave/cli/v2"
)

var AnnounceCmd = &cli.Command{
	Name:   "announce",
	Usage:  "Publish an announcement message for the latest advertisement",
	Flags:  announceFlags,
	Action: announceCommand,
}

var AnnounceHttpCmd = &cli.Command{
	Name:   "announce-http",
	Usage:  "Publish an announcement message for the latest advertisement to a specific indexer via http",
	Flags:  announceHttpFlags,
	Action: announceHttpCommand,
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
	defer resp.Body.Close()

	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		return errFromHttpResp(resp)
	}

	_, err = cctx.App.Writer.Write([]byte("Announced latest advertisement\n"))
	return err
}

func announceHttpCommand(cctx *cli.Context) error {
	indexer := cctx.String("indexer")

	params := map[string][]byte{
		"indexer": []byte(indexer),
	}
	bodyData, err := json.Marshal(&params)
	if err != nil {
		return err
	}
	body := bytes.NewBuffer(bodyData)

	req, err := http.NewRequestWithContext(cctx.Context, http.MethodPost, adminAPIFlagValue+"/admin/announcehttp", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	cl := &http.Client{}
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errFromHttpResp(resp)
	}

	_, err = cctx.App.Writer.Write([]byte("Announced latest advertisement via HTTP\n"))
	return err
}
