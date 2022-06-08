package main

import (
	"context"
	"fmt"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/cmd/provider/internal/config"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	httpc "github.com/filecoin-project/storetheindex/api/v0/ingest/client/http"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
	"io"
	"os"
	"strings"
)

var (
	topicName = "/indexer/ingest/mainnet"
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
	&cli.StringFlag{
		Name: "indexer",
		Aliases: []string{"i"},
		Usage: "Host or host:port of indexer to use",
		Required: false,
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
	var (
		eng *engine.Engine
		err error

		//pAddrInfo *peer.AddrInfo
	)
	contents := cctx.StringSlice("contents")
	ctxID := cctx.String("context")
	ingestStr := cctx.String("indexer")

	identity, err := config.CreateIdentity(os.Stdout)
	privKey, err := identity.DecodePrivateKey("")
	if err != nil {
		panic(err)
	}
	peerID, err := peer.Decode(identity.PeerID)
	if err != nil {
		panic(err)
	}

	h,err := libp2p.New(libp2p.Identity(privKey))
	if err != nil {
		panic(err)
	}

	//fmt.Printf("pAddrInfo: %v ingestStr: %s\n", pAddrInfo,ingestStr)

	if len(ingestStr) == 0 {
		eng,err = engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	} else {
		pub,err := pubsub.NewGossipSub(context.Background(),
			h,
			pubsub.WithDirectConnectTicks(1),
			//pubsub.WithDirectPeers([]peer.AddrInfo{*pAddrInfo}),
		)
		if err != nil {
			panic(err)
		}

		t,err := pub.Join(topicName)
		if err != nil {
			panic(err)
		}

		eng,err = engine.New(
			engine.WithHost(h),
			engine.WithPublisherKind(engine.DataTransferPublisher),
			engine.WithTopic(t),
			engine.WithTopicName(topicName),
		)

		client, err := httpc.New(ingestStr)
		if err != nil {
			return err
		}

		err = client.Register(cctx.Context, peerID, privKey, toStringArray(h.Addrs()))
		if err != nil {
			panic(err)
		}
		fmt.Println("Registered provider:", identity.PeerID, " addresses:" , toStringArray(h.Addrs()), " at indexer:", ingestStr)
	}

	if err != nil {
		panic(err)
	}
	fmt.Println("initialized provider")

	if err = eng.Start(context.Background());err!=nil{
		panic(err)
	}
	fmt.Println("provider started")
	defer eng.Shutdown()

	eng.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error){
		if ctxID == string(contextID) {
			return &contentsIter{0,contents},nil
		}

		return nil,fmt.Errorf("no content for context id: %v", contextID)
	})

	ad,err := eng.NotifyPut(context.Background(), []byte(ctxID), metadata.New(metadata.Bitswap{}))
	if err != nil{
		panic(err)
	}
	fmt.Printf("ad cid: %s\n",ad.String())
	
	err = eng.PublishLatest(context.Background())
	if err != nil{
		panic(err)
	}

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

func toStringArray(mas []multiaddr.Multiaddr) []string {
	strArray := make([]string, 0)
	for _,ma := range mas {
		strArray = append(strArray, ma.String())
	}

	return strArray
}

// extract 12D3KooWSTYbrZrtw7FHxi4zkxahKt7oaV5kmHAdQkHXJ8CrvRp5@/ip4/15.7.1.42/tcp/3003
func extractAddrInfo(addrInfoStr string) (*peer.AddrInfo,error){
	trimedAddrInfoStr := strings.TrimSpace(addrInfoStr)
	if len(trimedAddrInfoStr) == 0  || !strings.Contains(trimedAddrInfoStr,"@"){
		return nil,fmt.Errorf("bad format: %s", addrInfoStr)
	}

	parts := strings.Split(trimedAddrInfoStr, "@")
	id := parts[0]
	ma := parts[1]

	pid,err := peer.Decode(id)
	if err != nil{
		return nil,err
	}
	muaddr,err := multiaddr.NewMultiaddr(ma)
	if err != nil{
		return nil,err
	}

	return &peer.AddrInfo{
		pid,[]multiaddr.Multiaddr{muaddr},
	},nil
}

