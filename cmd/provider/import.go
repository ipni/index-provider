package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"github.com/filecoin-project/index-provider/cardatatransfer"
	adminserver "github.com/filecoin-project/index-provider/server/admin/http"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/multiformats/go-multicodec"
	"github.com/urfave/cli/v2"
)

var ImportCmd = &cli.Command{
	Name:        "import",
	Aliases:     []string{"i"},
	Usage:       "Imports sources of multihashes to the index provider.",
	Subcommands: []*cli.Command{importCarSubCmd},
}

var (
	importCarKey    []byte
	importCarSubCmd = &cli.Command{
		Name:    "car",
		Aliases: []string{"c"},
		Usage:   "Imports CAR from a path",
		Flags:   importCarFlags,
		Before:  beforeImportCar,
		Action:  doImportCar,
	}
	metadata = stiapi.Metadata{
		ProtocolID: multicodec.TransportGraphsyncFilecoinv1,
	}
)

func beforeImportCar(cctx *cli.Context) error {
	if cctx.IsSet(keyFlag.Name) {
		decoded, err := base64.StdEncoding.DecodeString(keyFlagValue)
		if err != nil {
			return errors.New("key is not a valid base64 encoded string")
		}
		importCarKey = decoded
	} else {
		h := sha256.New()
		h.Write([]byte(carPathFlagValue))
		importCarKey = h.Sum(nil)
	}
	if cctx.IsSet(metadataFlag.Name) {
		decoded, err := base64.StdEncoding.DecodeString(metadataFlagValue)
		if err != nil {
			return errors.New("metadata is not a valid base64 encoded string")
		}
		metadata.Data = decoded
	} else {
		// If no metadata is set, generate metadata that is compatible for FileCoin retrieval base
		// on the context ID
		var err error
		metadata, err = cardatatransfer.MetadataFromContextID(importCarKey)
		if err != nil {
			return err
		}
	}
	return nil
}

func doImportCar(cctx *cli.Context) error {
	req := adminserver.ImportCarReq{
		Path:     carPathFlagValue,
		Key:      importCarKey,
		Metadata: metadata,
	}
	resp, err := doHttpPostReq(cctx.Context, adminAPIFlagValue+"/admin/import/car", req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errFromHttpResp(resp)
	}

	log.Infof("imported car successfully")
	var res adminserver.ImportCarRes
	if _, err := res.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("received ok response from server but cannot decode response body. %v", err)
	}
	var b bytes.Buffer
	b.WriteString("Successfully imported CAR.\n")
	b.WriteString("\t Advertisement ID: ")
	b.WriteString(res.AdvId.String())
	b.WriteString("\n\t Context ID: ")
	b.WriteString(base64.StdEncoding.EncodeToString(importCarKey))
	b.WriteString("\n")
	_, err = cctx.App.Writer.Write(b.Bytes())
	return err
}
