package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/cmd/provider/internal/config"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

var ExtendCmd = &cli.Command{
	Name:    "extend",
	Aliases: []string{"e"},
	Usage:   "Extend an advert with additional data.",
	Flags:   extendFlags,
	Action:  extendCmd,
}

func extendCmd(cctx *cli.Context) error {
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

	decoded, err := base64.StdEncoding.DecodeString(metadataFlagValue)
	if err != nil {
		return errors.New("metadata is not a valid base64 encoded string")
	}
	md = metadata.Default.WithProtocol(multicodec.Http, metadata.HTTPV1).New()
	err = md.UnmarshalBinary(decoded)
	if err != nil {
		return err
	}

	store := datastore.NewMapDatastore()
	eng, err := engine.New(
		engine.WithDatastore(store),
	)
	if err != nil {
		return err
	}
	eng.RegisterMultihashLister(func(ctx context.Context, provider peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		mi := singleIterator{mh}
		return &mi, nil
	})
	saddrs := cctx.StringSlice("addr")
	addrs := []multiaddr.Multiaddr{}
	for _, saddr := range saddrs {
		addr, err := multiaddr.NewMultiaddr(saddr)
		if err != nil {
			return err
		}
		addrs = append(addrs, addr)
	}
	advert, err := eng.NotifyPut(context.Background(), &peer.AddrInfo{
		ID:    peerID,
		Addrs: addrs,
	}, []byte(cctx.String("ctxid")), md)
	if err != nil {
		return err
	}

	// make the head object.

	// dump the datastore.
	allKeys, _ := store.Query(context.Background(), query.Query{})

	fmt.Println("dumped")
	return nil
}

type singleIterator struct {
	mh multihash.Multihash
}

func (si *singleIterator) Next() (multihash.Multihash, error) {
	if si.mh == nil {
		return nil, io.EOF
	}
	m := si.mh
	si.mh = nil
	return m, nil
}
