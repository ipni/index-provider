package main

import (
	"encoding/base64"
	"fmt"

	httpfinderclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

var FindCmd = &cli.Command{
	Name:   "find",
	Usage:  "Query an indexer for indexed content",
	Flags:  findFlags,
	Action: findCommand,
}

func findCommand(cctx *cli.Context) error {
	cli, err := httpfinderclient.New(cctx.String("indexer"))
	if err != nil {
		return err
	}

	mhArgs := cctx.StringSlice("mh")
	cidArgs := cctx.StringSlice("cid")
	mhs := make([]multihash.Multihash, 0, len(mhArgs)+len(cidArgs))
	for i := range mhArgs {
		m, err := multihash.FromB58String(mhArgs[i])
		if err != nil {
			return err
		}
		mhs = append(mhs, m)
	}
	for i := range cidArgs {
		c, err := cid.Decode(cidArgs[i])
		if err != nil {
			return err
		}
		mhs = append(mhs, c.Hash())
	}

	resp, err := cli.FindBatch(cctx.Context, mhs)
	if err != nil {
		return err
	}

	if len(resp.MultihashResults) == 0 {
		fmt.Println("index not found")
		return nil
	}

	fmt.Println("Content providers:")
	for i := range resp.MultihashResults {
		fmt.Println("   Multihash:", resp.MultihashResults[i].Multihash.B58String())
		for _, pr := range resp.MultihashResults[i].ProviderResults {
			fmt.Println("       Provider:", pr.Provider)
			fmt.Println("       ContextID:", base64.StdEncoding.EncodeToString(pr.ContextID))
			fmt.Println("       Metadata:", base64.StdEncoding.EncodeToString(pr.Metadata))
		}
	}

	return nil
}
