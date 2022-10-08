package main

import (
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/filecoin-project/index-provider/cmd/provider/internal/config"
	"github.com/filecoin-project/index-provider/metadata"
	httpc "github.com/filecoin-project/storetheindex/api/v0/ingest/client/http"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

var IndexCmd = &cli.Command{
	Name:   "index",
	Usage:  "Push a single content index into an indexer",
	Flags:  indexFlags,
	Action: indexCommand,
}

func indexCommand(cctx *cli.Context) error {
	mhArg := cctx.String("mh")
	cidArg := cctx.String("cid")
	if mhArg == "" && cidArg == "" {
		return errors.New("must specify --cid or --mh")
	}
	if mhArg != "" && cidArg != "" {
		return errors.New("only one --cid or --mh allowed")
	}
	var mh multihash.Multihash
	var err error

	if mhArg != "" {
		mh, err = multihash.FromB58String(mhArg)
		if err != nil {
			return err
		}
	} else if cidArg != "" {
		var ccid cid.Cid
		ccid, err = cid.Decode(cidArg)
		if err != nil {
			return err
		}
		mh = ccid.Hash()
	}

	cfg, err := config.Load("")
	if err != nil {
		return err
	}

	peerID, privKey, err := cfg.Identity.DecodeOrCreate(cctx.App.Writer)
	if err != nil {
		return err
	}

	client, err := httpc.New(cctx.String("indexer"))
	if err != nil {
		return err
	}

	decoded, err := base64.StdEncoding.DecodeString(metadataFlagValue)
	if err != nil {
		return errors.New("metadata is not a valid base64 encoded string")
	}
	md = metadata.New()
	err = md.UnmarshalBinary(decoded)
	if err != nil {
		return err
	}

	err = client.IndexContent(cctx.Context, peerID, privKey, mh, []byte(cctx.String("ctxid")), decoded, cctx.StringSlice("addr"))
	if err != nil {
		return err
	}

	fmt.Println("OK Indexed content")
	return nil
}
