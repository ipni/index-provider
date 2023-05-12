package main

import (
	"bytes"
	"fmt"
	"net/http"

	adminserver "github.com/ipni/index-provider/server/admin/http"
	"github.com/urfave/cli/v2"
)

var ListCmd = &cli.Command{
	Name:        "list",
	Usage:       "List local paths to data",
	Aliases:     []string{"ls"},
	Subcommands: []*cli.Command{listCarSubCmd},
}

var listCarSubCmd = &cli.Command{
	Name:   "car",
	Usage:  "Lists the local paths to CAR files provided by an standalone instance of index-provider daemon.",
	Action: doListCars,
	Flags: []cli.Flag{
		adminAPIFlag,
	},
}

func doListCars(cctx *cli.Context) error {
	resp, err := http.Get(adminAPIFlagValue + "/admin/list/car")
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errFromHttpResp(resp)
	}

	var res adminserver.ListCarRes
	if _, err := res.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("received ok response from server but cannot decode response body. %v", err)
	}
	var b bytes.Buffer
	for _, path := range res.Paths {
		b.WriteString(path)
		b.WriteString(fmt.Sprintln())
	}
	_, err = cctx.App.Writer.Write(b.Bytes())
	return err
}
