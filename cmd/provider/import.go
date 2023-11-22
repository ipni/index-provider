package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"

	"github.com/ipni/go-libipni/metadata"
	"github.com/ipni/index-provider/cardatatransfer"
	adminserver "github.com/ipni/index-provider/server/admin/http"
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
	md = metadata.Default.New()
)

var importCarFlags = []cli.Flag{
	adminAPIFlag,
	carPathFlag,
	metadataFlag,
	keyFlag,
}

func beforeImportCar(cctx *cli.Context) error {
	if cctx.IsSet(keyFlag.Name) {
		decoded, err := base64.StdEncoding.DecodeString(keyFlagValue)
		if err != nil {
			return errors.New("key is not a valid base64 encoded string")
		}
		importCarKey = decoded
	} else {
		absCarPath, err := filepath.Abs(carPathFlagValue)
		if err != nil {
			return err
		}
		h := sha256.New()
		h.Write([]byte(absCarPath))
		importCarKey = h.Sum(nil)
	}
	if cctx.IsSet(metadataFlag.Name) {
		decoded, err := base64.StdEncoding.DecodeString(metadataFlagValue)
		if err != nil {
			return errors.New("metadata is not a valid base64 encoded string")
		}
		err = md.UnmarshalBinary(decoded)
		if err != nil {
			return err
		}
	} else {
		// If no metadata is set, generate metadata that is compatible for FileCoin retrieval base
		// on the context ID
		var err error
		tp, err := cardatatransfer.TransportFromContextID(importCarKey)
		if err != nil {
			return err
		}
		md = metadata.Default.New(tp)
	}
	return nil
}

func doImportCar(cctx *cli.Context) error {
	mdBytes, err := md.MarshalBinary()
	if err != nil {
		return err
	}

	absCarPath, err := filepath.Abs(carPathFlagValue)
	if err != nil {
		return err
	}

	req := adminserver.ImportCarReq{
		Path:     absCarPath,
		Key:      importCarKey,
		Metadata: mdBytes,
	}
	resp, err := doHttpPostReq(cctx.Context, adminAPIFlagValue+"/admin/import/car", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

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
