package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	adminserver "github.com/ipni/index-provider/server/admin/http"
	"github.com/urfave/cli/v2"
)

var AnnounceCmd = &cli.Command{
	Name:   "announce",
	Usage:  "Publish an announcement message for the latest advertisement",
	Flags:  announceFlags,
	Action: announceCommand,
}

var announceFlags = []cli.Flag{
	adminAPIFlag,
}

var AnnounceHttpCmd = &cli.Command{
	Name:   "announce-http",
	Usage:  "Publish an announcement message for the latest advertisement to a specific indexer via http",
	Flags:  announceHttpFlags,
	Action: announceHttpCommand,
}

var announceHttpFlags = []cli.Flag{
	adminAPIFlag,
	indexerFlag,
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

	var res adminserver.AnnounceRes
	if _, err := res.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("received ok response from server but cannot decode response body. %v", err)
	}
	msg := fmt.Sprintf("Announced latest advertisement: %s\n", res.AdvId)

	_, err = cctx.App.Writer.Write([]byte(msg))
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

	var res adminserver.AnnounceRes
	if _, err := res.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("received ok response from server but cannot decode response body. %v", err)
	}
	msg := fmt.Sprintf("Announced latest advertisement via HTTP: %s\n", res.AdvId)

	_, err = cctx.App.Writer.Write([]byte(msg))
	return err
}
