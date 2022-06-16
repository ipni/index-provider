package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ipfs/core/bootstrap"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	"golang.org/x/time/rate"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	BOOTSTRAP_NODES = []string{
		"/dns4/bootstrap-4.mainnet.filops.net/tcp/1347/p2p/12D3KooWL6PsFNPhYftrJzGgF5U18hFoaVhfGk7xwzD8yVrHJ3Uc",
		"/dns4/bootstrap-5.mainnet.filops.net/tcp/1347/p2p/12D3KooWLFynvDQiUpXoHroV1YxKHhPJgysQGH2k3ZGwtWzR4dFH",
		"/dns4/bootstrap-1.starpool.in/tcp/12757/p2p/12D3KooWQZrGH1PxSNZPum99M1zNvjNFM33d1AAu5DcvdHptuU7u",
		"/dns4/bootstrap-8.mainnet.filops.net/tcp/1347/p2p/12D3KooWScFR7385LTyR4zU1bYdzSiiAb5rnNABfVahPvVSzyTkR",
		"/dns4/bootstrap-0.starpool.in/tcp/12757/p2p/12D3KooWGHpBMeZbestVEWkfdnC9u7p6uFHXL1n7m1ZBqsEmiUzz",
		"/dns4/node.glif.io/tcp/1235/p2p/12D3KooWBF8cpp65hp2u9LK5mh19x67ftAam84z9LsfaquTDSBpt",
		"/dns4/bootstrap-1.ipfsmain.cn/tcp/34723/p2p/12D3KooWMKxMkD5DMpSWsW7dBddKxKT7L2GgbNuckz9otxvkvByP",
		"/dns4/bootstrap-0.ipfsmain.cn/tcp/34721/p2p/12D3KooWQnwEGNqcM2nAcPtRR9rAX8Hrg4k9kJLCHoTR5chJfz6d",
		"/dns4/lotus-bootstrap.ipfsforce.com/tcp/41778/p2p/12D3KooWGhufNmZHF3sv48aQeS13ng5XVJZ9E6qy2Ms4VzqeUsHk",
		"/dns4/bootstrap-0.mainnet.filops.net/tcp/1347/p2p/12D3KooWCVe8MmsEMes2FzgTpt9fXtmCY7wrq91GRiaC8PHSCCBj",
		"/dns4/bootstrap-1.mainnet.filops.net/tcp/1347/p2p/12D3KooWCwevHg1yLCvktf2nvLu7L9894mcrJR4MsBCcm4syShVc",
		"/dns4/bootstrap-2.mainnet.filops.net/tcp/1347/p2p/12D3KooWEWVwHGn2yR36gKLozmb4YjDJGerotAPGxmdWZx2nxMC4",
		"/dns4/bootstrap-3.mainnet.filops.net/tcp/1347/p2p/12D3KooWKhgq8c7NQ9iGjbyK7v7phXvG6492HQfiDaGHLHLQjk7R",
		"/dns4/bootstrap-6.mainnet.filops.net/tcp/1347/p2p/12D3KooWP5MwCiqdMETF9ub1P3MbCvQCcfconnYHbWg6sUJcDRQQ",
		"/dns4/bootstrap-7.mainnet.filops.net/tcp/1347/p2p/12D3KooWRs3aY1p3juFjPy8gPN95PEQChm2QKGUCAdcDCC4EBMKf",
	}
)

var subFlags = []cli.Flag{
	&cli.StringFlag{
		Name: "context",
		Usage: "all the mh related to the context",
		Required: false,
	},
}

var SubCmd = &cli.Command{
	Name: "sub",
	Usage: "subscribe  ads",
	Flags: subFlags,
	Action: subCommand,
}

func subCommand(cctx *cli.Context) error {
	ctx,cancel := context.WithCancel(context.Background())

	subHost, err := libp2p.New()
	if err != nil {
		return err
	}

	fmt.Println("peerId",subHost.ID().String(),"maddrs",subHost.Addrs())

	subG, err := pubsub.NewGossipSub(ctx, subHost,
	)
	subT, err := subG.Join(topicName)
	if err != nil {
		return err
	}

	subsc, err := subT.Subscribe()
	if err != nil {
		return err
	}

	closeIt,err := doBootstrap(subHost.ID(), subHost)
	if err != nil {
		return err
	}

	//go func() {
	//	for {
	//		//select {
	//		//case <-time.NewTimer(3 * time.Second).C:
	//		//	if len(subHost.Network().Peers()) > 0 {
	//		//		fmt.Println(subHost.Network().Peers())
	//		//	}
	//		//default:
	//		//
	//		//}
	//		time.Sleep(5 * time.Second)
	//		if len(subHost.Network().Peers()) > 0 {
	//			fmt.Println("my id",subHost.ID().String())
	//			fmt.Println(subHost.Network().Peers())
	//		}
	//	}
	//}()

	go func() error{
		for {
			pubsubMsg, err := subsc.Next(ctx)
			if err != nil {
				fmt.Println("subsc.Next",err)
				return err
			}

			//fmt.Println("from",pubsubMsg.GetFrom(),"topic",pubsubMsg.GetTopic())

			gotMessage := dtsync.Message{}
			err = gotMessage.UnmarshalCBOR(bytes.NewBuffer(pubsubMsg.Data))
			if err != nil {
				fmt.Println("Could not decode pubsub message",err)
				return err
			}

			var addrs []multiaddr.Multiaddr
			if len(gotMessage.Addrs) != 0 {
				addrs, err = gotMessage.GetAddrs()
				if err != nil {
					log.Errorw("Could not decode pubsub message", "err", err)
				}
			}

			peerStore := subHost.Peerstore()
			if peerStore != nil && len(addrs) != 0 && len(peerStore.PeerInfo(pubsubMsg.GetFrom()).Addrs) == 0{
				fmt.Println("add new addrs: ", "from:",pubsubMsg.GetFrom(),"addrs:",addrs)
				peerStore.AddAddrs(pubsubMsg.GetFrom(), addrs, 10 * time.Second)
			}



			ds := dssync.MutexWrap(datastore.NewMapDatastore())
			ls := cidlink.DefaultLinkSystem()
			store := &memstore.Store{}
			ls.SetReadStorage(store)
			ls.SetWriteStorage(store)

			sync, err := dtsync.NewSync(subHost, ds, ls, nil, func(publisher peer.ID) *rate.Limiter {
				return rate.NewLimiter(100, 10)
			})

			syncer := sync.NewSyncer(pubsubMsg.GetFrom(), topicName, rate.NewLimiter(100, 10))
			gotHead, err := syncer.GetHead(ctx)

			if err != nil {
				fmt.Println(fmt.Println("from:",pubsubMsg.GetFrom(),"error:", err))
				continue
			}
			fmt.Println("from",pubsubMsg.GetFrom(),"topic",pubsubMsg.GetTopic(),"latest cid", gotHead.String())

			//ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
			//adSel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreFields(
			//	func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
			//		efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
			//		efsb.Insert("Next", ssb.ExploreRecursiveEdge())
			//		efsb.Insert("Entries", ssb.ExploreRecursiveEdge())
			//	})).Node()
			//err = syncer.Sync(ctx, gotPublishedAdCid, adSel)
		}


		return nil
	}()


	chanel := make(chan os.Signal)
	signal.Notify(chanel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-chanel
	defer cancel()
	defer closeIt()

	return nil
}

func parsePeers(addrs []string) ([]peer.AddrInfo, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, addr := range addrs {
		var err error
		maddrs[i], err = multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, err
		}
	}
	return peer.AddrInfosFromP2pAddrs(maddrs...)
}

func doBootstrap(peerID peer.ID,p2pHost host.Host) (func() error,error){
	addrs, err := parsePeers(BOOTSTRAP_NODES)
	if err != nil {
		return nil,fmt.Errorf("bad bootstrap peer: %s", err)
	}

	bootCfg := bootstrap.BootstrapConfigWithPeers(addrs)
	bootCfg.MinPeerThreshold = 5

	bootstrapper, err := bootstrap.Bootstrap(peerID, p2pHost, nil, bootCfg)
	if err != nil {
		return nil,fmt.Errorf("bootstrap failed: %s", err)
	}

	return bootstrapper.Close,nil
}