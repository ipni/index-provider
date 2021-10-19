package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	adminserver "github.com/filecoin-project/indexer-reference-provider/server/admin/http"
	"github.com/urfave/cli/v2"
)

// TODO: This should change to a code that indicates graphsync.
const providerProtocolID = 0x300001

var ImportCmd = &cli.Command{
	Name:        "import",
	Aliases:     []string{"i"},
	Usage:       "Imports sources of multihashes to the index provider.",
	Subcommands: []*cli.Command{importCarSubCmd},
}

var (
	key             []byte
	importCarSubCmd = &cli.Command{
		Name:    "car",
		Aliases: []string{"c"},
		Usage:   "Imports CAR from a path",
		Flags:   importCarFlags,
		Before:  beforeImportCar,
		Action:  doImportCar,
	}
)

func beforeImportCar(context *cli.Context) error {
	if cctx.IsSet(keyFlag.Name) {
		decoded, err := base64.StdEncoding.DecodeString(keyFlagValue)
		if err != nil {
			return errors.New("key is not a valid base64 encoded string")
		}
		key = decoded
	}
	return nil
}

func doImportCar(cctx *cli.Context) error {
	req := adminserver.ImportCarReq{
		Path: carPathFlagValue,
		Key:  key,
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return err
	}
	bodyReader := bytes.NewReader(reqBody)
	httpReq, err := http.NewRequestWithContext(cctx.Context, http.MethodPost, adminAPIFlagValue+"/admin/import/car", bodyReader)
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	cl := &http.Client{}
	resp, err := cl.Do(httpReq)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		statusText := http.StatusText(resp.StatusCode)
		var errRes adminserver.ErrorRes
		if _, err := errRes.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf(
				"failed to import car, server responsed with %s. cannot decode error response: %v",
				http.StatusText(resp.StatusCode), err)
		}
		return fmt.Errorf("%s %s", statusText, errRes.Message)
	}

	log.Infof("Successfully imported car")
	var res adminserver.ImportCarRes
	if _, err := res.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("received OK response from server but cannot decode response body. %v", err)
	}
	var b bytes.Buffer
	b.WriteString("Successfully imported CAR.\n")
	b.WriteString("\t Advertisement ID: ")
	b.WriteString(res.AdvId.String())
	b.WriteString("\n")
	_, err = cctx.App.Writer.Write(b.Bytes())
	return err
}
