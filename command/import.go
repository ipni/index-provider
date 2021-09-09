package command

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	adminserver "github.com/filecoin-project/indexer-reference-provider/server/http"
	"github.com/urfave/cli/v2"
)

var ImportCmd = &cli.Command{
	Name:        "import",
	Aliases:     []string{"i"},
	Usage:       "Imp",
	Subcommands: []*cli.Command{importCarSubCmd},
}

var (
	metadata        []byte
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
	if metadataFlagValue != "" {
		decoded, err := base64.StdEncoding.DecodeString(metadataFlagValue)
		if err != nil {
			return err
		}
		metadata = decoded
	}
	if keyFlagValue != "" {
		decoded, err := base64.StdEncoding.DecodeString(keyFlagValue)
		if err != nil {
			return err
		}
		key = decoded
	}
	return nil
}

func doImportCar(cctx *cli.Context) error {
	req := adminserver.ImportCarReq{
		Path:     carPathFlagValue,
		Key:      key,
		Metadata: metadata,
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
		return fmt.Errorf("failed to import car, due to failure response from Admin server: %v", http.StatusText(resp.StatusCode))
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var icRes adminserver.ImportCarRes
	if err := json.Unmarshal(respBody, &icRes); err != nil {
		return err
	}

	log.Infof("Successfully imported car")

	msg := fmt.Sprintf("Successfully imported CAR.\n"+
		"\t Advertisement ID: %v\n", icRes.AdvId)

	_, err = cctx.App.Writer.Write([]byte(msg))
	return err
}
