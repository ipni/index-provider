package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/cmd/provider/internal/config"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
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
		fmt.Printf("failed to get config\n")
		return err
	}

	peerID, _, err := cfg.Identity.DecodeOrCreate(cctx.App.Writer)
	if err != nil {
		fmt.Printf("failed to get peerid\n")
		return err
	}

	_, err = base64.StdEncoding.DecodeString(metadataFlagValue)
	if err != nil {
		return errors.New("metadata is not a valid base64 encoded string")
	}
	md = metadata.Default.WithProtocol(multicodec.Http, metadata.HTTPV1).New(metadata.HTTPV1())
	//	err = md.UnmarshalBinary(decoded)
	//	if err != nil {
	//		fmt.Printf("failed to understand metadata\n")
	//		return err
	//	}
	b, _ := md.MarshalBinary()
	fmt.Printf("md is %x\n", b)

	store := datastore.NewMapDatastore()
	fmt.Printf("making engine...\n")
	eng, err := engine.New(
		engine.WithDatastore(store),
		engine.WithPublisherKind(engine.HttpPublisher),
	)
	if err != nil {
		return err
	}
	eng.RegisterMultihashLister(func(ctx context.Context, provider peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		mi := singleIterator{mh}
		return &mi, nil
	})
	if err := eng.Start(context.Background()); err != nil {
		return err
	}
	fmt.Printf("engine started...\n")
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

	fmt.Printf("dumping...\n")

	// get the head object.
	hb, err := http.Get("http://0.0.0.0:3104/head")
	if err != nil {
		return err
	}
	hbb, err := io.ReadAll(hb.Body)
	if err != nil {
		return err
	}
	os.WriteFile(cctx.String("outDir")+"head", hbb, 0644)

	// dump the advert.
	ab, err := http.Get("http://0.0.0.0:3104/" + advert.String())
	if err != nil {
		return err
	}
	abb, err := io.ReadAll(ab.Body)
	if err != nil {
		return err
	}
	os.WriteFile(cctx.String("outDir")+advert.String(), abb, 0644)

	// dump the entries.
	av, err := eng.GetAdv(context.Background(), advert)
	if err != nil {
		return err
	}

	eb, err := http.Get("http://0.0.0.0:3104/" + av.Entries.String())
	if err != nil {
		return err
	}
	ebb, err := io.ReadAll(eb.Body)
	if err != nil {
		return err
	}
	os.WriteFile(cctx.String("outDir")+av.Entries.String(), ebb, 0644)

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
