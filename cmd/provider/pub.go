package main

import (
	"context"
	"fmt"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/libp2p/go-libp2p"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
	"io"
)

var pubFlags = []cli.Flag{
	&cli.StringSliceFlag{
		Name:"contents",
		Usage: "content list to be published",
		Required: true,
	},
	&cli.StringFlag{
		Name: "context",
		Usage: "all the mh related to the context",
		Required: true,
	},
}

// call it via "provider pub --context=xiiiv --contents=francis --contents=cissy --contents=tiger"

var PubCmd = &cli.Command{
	Name: "pub",
	Usage: "publish an ad",
	Flags: pubFlags,
	Action: pubCommand,
}

func pubCommand(cctx *cli.Context) error {
	contents := cctx.StringSlice("contents")
	ctxID := cctx.String("context")

	h,err := libp2p.New()
	if err != nil {
		panic(err)
	}

	eng,err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	if err != nil {
		panic(err)
	}
	fmt.Println("initialized provider")
	defer eng.Shutdown()

	eng.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error){
		if ctxID == string(contextID) {
			return &contentsIter{0,contents},nil
		}

		return nil,fmt.Errorf("no content for context id: %v", contextID)
	})

	if err = eng.Start(context.Background());err!=nil{
		panic(err)
	}
	fmt.Println("provider started")

	ad,err := eng.NotifyPut(context.Background(), []byte(ctxID), metadata.New(metadata.Bitswap{}))
	if err != nil{
		panic(err)
	}
	fmt.Printf("ad cid: %s\n",ad.String())

	return nil
}

type contentsIter struct {
	offset int
	contents []string
}

func (c *contentsIter) Next() (multihash.Multihash,error)  {
	if c.offset==len(c.contents) {
		return nil,io.EOF
	}

	mh,err := multihash.Sum([]byte(c.contents[c.offset]),multihash.SHA2_256,-1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Generated content multihash: %s\n", mh.B58String())
	c.offset++

	return mh,nil
}