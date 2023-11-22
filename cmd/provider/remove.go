package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	adminserver "github.com/ipni/index-provider/server/admin/http"
	"github.com/urfave/cli/v2"
)

var RemoveCmd = &cli.Command{
	Name:        "remove",
	Aliases:     []string{"rm"},
	Usage:       "Removes previously advertised multihashes by the provider.",
	Subcommands: []*cli.Command{removeCarSubCmd},
}

var (
	removeCarKey    []byte
	removeCarSubCmd = &cli.Command{
		Name:    "car",
		Aliases: []string{"c"},
		Usage:   "Removes the multihashes previously advertised via a CAR file.",
		Description: `Publishes an advertisement signalling that the provider no longer provides the
list of multihashes contained a CAR file.

The CAR file must have previously been imported.
See import command.

The CAR file to remove is identified by either:
  - the key option, the key by which the CAR file was previously imported, or
  - the input option, the path to the CAR file that was previously imported.

Specifying both key and input options is not allowed. In the case where the path option is 
specified, they key is simply calculated as the SHA_256 hash of the given path.`,
		Flags:  removeCarFlags,
		Before: beforeRemoveCar,
		Action: doRemoveCar,
	}

	removeCarFlags = []cli.Flag{
		adminAPIFlag,
		optionalCarPathFlag,
		keyFlag,
	}
)

func beforeRemoveCar(cctx *cli.Context) error {
	if !cctx.IsSet(keyFlag.Name) {
		if !cctx.IsSet(optionalCarPathFlag.Name) {
			return fmt.Errorf("either %s or %s must be set", keyFlag.Name, optionalCarPathFlag.Name)
		}
		h := sha256.New()
		h.Write([]byte(optionalCarPathFlagValue))
		removeCarKey = h.Sum(nil)
		return nil
	}

	if cctx.IsSet(optionalCarPathFlag.Name) {
		return fmt.Errorf("only one of %s or %s must be set", keyFlag.Name, optionalCarPathFlag.Name)
	}
	decoded, err := base64.StdEncoding.DecodeString(keyFlagValue)
	if err != nil {
		return errors.New("key is not a valid base64 encoded string")
	}
	removeCarKey = decoded
	return nil
}

func doRemoveCar(cctx *cli.Context) error {
	req := adminserver.RemoveCarReq{
		Key: removeCarKey,
	}
	resp, err := doHttpPostReq(cctx.Context, adminAPIFlagValue+"/admin/remove/car", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errFromHttpResp(resp)
	}

	var res adminserver.RemoveCarRes
	if _, err := res.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("received ok response from server but cannot decode response body. %v", err)
	}
	var b bytes.Buffer
	b.WriteString("Successfully removed CAR.\n")
	b.WriteString("\t Advertisement ID: ")
	b.WriteString(res.AdvId.String())
	b.WriteString("\n\t Context ID: ")
	b.WriteString(base64.StdEncoding.EncodeToString(removeCarKey))
	b.WriteString("\n")
	_, err = cctx.App.Writer.Write(b.Bytes())
	return err
}
