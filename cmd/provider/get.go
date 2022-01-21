package main

import (
	"fmt"
	"github.com/filecoin-project/index-provider/cmd/provider/internal"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var (
	adCid      = cid.Undef
	provClient internal.ProviderClient

	pAddrInfo    string
	topic        string
	printEntries bool
	GetAdCmd     = &cli.Command{
		Name:        "get-ad",
		Usage:       "Gets advertisement",
		ArgsUsage:   "[ad-cid]",
		Description: "Advertisement CID may optionally be specified as the first argument. If not specified the latest advertisement is used.",
		Before:      beforeGetAdvertisements,
		Action:      doGetAdvertisements,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "provider-addr-info",
				Usage: "The provider's endpoint address in form of libp2p multiaddr info. " +
					"Example GraphSync endpoint: /ip4/1.2.3.4/tcp/1234/p2p/12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ  " +
					"Example HTTP endpoint: /ip4/1.2.3.4/tcp/1234/http/12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ",
				Aliases:     []string{"p"},
				Destination: &pAddrInfo,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "topic",
				Usage:       "The topic on which index advertisements are published. Only needed if connecting to provider via Graphsync endpoint.",
				Value:       "indexer/ingest",
				Aliases:     []string{"t"},
				Destination: &topic,
			},
			&cli.BoolFlag{
				Name:        "print-entries",
				Usage:       "Whether to print the list of entries in advertisement",
				Aliases:     []string{"e"},
				Destination: &printEntries,
			},
		},
	}
)

func beforeGetAdvertisements(cctx *cli.Context) error {
	var err error
	if cctx.NArg() > 1 {
		return cli.Exit("At most one argument [ad-cid] must be specified. If none is specified, the current head advertisement is fetched.", 1)
	}
	if cctx.Args().Present() {
		adCid, err = cid.Decode(cctx.Args().First())
		if err != nil {
			return err
		}
	}

	addr, err := multiaddr.NewMultiaddr(pAddrInfo)
	if err != nil {
		return err
	}
	addrInfos, err := peer.AddrInfosFromP2pAddrs(addr)
	if err != nil {
		return err
	}

	addrInfo := addrInfos[0]

	var isHttp bool
	for _, p := range addrInfo.Addrs[0].Protocols() {
		if p.Code == multiaddr.P_HTTP || p.Code == multiaddr.P_HTTPS {
			isHttp = true
			break
		}
	}

	if isHttp {
		provClient, err = internal.NewHttpProviderClient(addrInfo)
		if err != nil {
			return err
		}
	} else {
		if topic == "" {
			return cli.Exit("topic must be configured when graphsync endpoint is specified.", 1)
		}
		provClient, err = internal.NewGraphSyncProviderClient(addrInfo, topic)
		if err != nil {
			return err
		}
	}

	return nil
}

func doGetAdvertisements(cctx *cli.Context) error {
	ad, err := provClient.GetAdvertisement(cctx.Context, adCid)
	if err != nil {
		return err
	}

	fmt.Printf("ID:          %s\n", ad.ID)
	fmt.Printf("PreviousID:  %s\n", ad.PreviousID)
	fmt.Printf("ProviderID:  %s\n", ad.ProviderID)
	fmt.Printf("Addresses:   %v\n", ad.Addresses)
	fmt.Printf("Is Remove:   %v\n", ad.IsRemove)

	fmt.Println("Entries:")
	entries, cc, err := ad.Entries.Drain()
	if err != nil {
		return err
	}

	if printEntries {
		for _, mh := range entries {
			fmt.Printf("  %s\n", mh)
		}
		fmt.Println("  ---------------------")
	}
	fmt.Printf("  Chunk Count: %d\n", cc)
	fmt.Printf("  Total Count: %d\n", len(entries))
	return nil
}
