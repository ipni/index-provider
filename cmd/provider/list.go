package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/filecoin-project/index-provider/cmd/provider/internal"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime/traversal/selector"
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
		Name:        "list",
		Usage:       "Lists advertisements",
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
				Value:       "/indexer/ingest/mainnet",
				Aliases:     []string{"t"},
				Destination: &topic,
			},
			&cli.BoolFlag{
				Name:        "print-entries",
				Usage:       "Whether to print the list of entries in advertisement",
				Aliases:     []string{"e"},
				Destination: &printEntries,
			},
			adEntriesRecurLimitFlag,
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

	provClient, err = toProviderClient(pAddrInfo, topic)
	return err
}

func toProviderClient(addrStr string, topic string) (internal.ProviderClient, error) {
	addr, err := multiaddr.NewMultiaddr(addrStr)
	if err != nil {
		return nil, err
	}
	addrInfos, err := peer.AddrInfosFromP2pAddrs(addr)
	if err != nil {
		return nil, err
	}
	addrInfo := addrInfos[0]

	if topic == "" {
		return nil, errors.New("topic must be configured when graphsync endpoint is specified")
	}

	if adEntriesRecurLimitFlagValue < 0 {
		return nil, fmt.Errorf("ad entries recursion depth limit cannot be less than zero; got %d", adEntriesRecurLimitFlagValue)
	}

	var entRecurLim selector.RecursionLimit
	if adEntriesRecurLimitFlagValue == 0 {
		entRecurLim = selector.RecursionLimitNone()
	} else {
		entRecurLim = selector.RecursionLimitDepth(adEntriesRecurLimitFlagValue)
	}

	return internal.NewProviderClient(addrInfo, internal.WithTopicName(topic), internal.WithEntriesRecursionLimit(entRecurLim))
}

func doGetAdvertisements(cctx *cli.Context) error {
	ad, err := provClient.GetAdvertisement(cctx.Context, adCid)
	if err != nil {
		if ad == nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "⚠️ Failed to fully sync advertisement %s. Output shows partially synced ad.\n  Error: %s\n", adCid, err.Error())
	}

	fmt.Printf("ID:          %s\n", ad.ID)
	fmt.Printf("PreviousID:  %s\n", ad.PreviousID)
	fmt.Printf("ProviderID:  %s\n", ad.ProviderID)
	fmt.Printf("Addresses:   %v\n", ad.Addresses)
	fmt.Printf("Is Remove:   %v\n", ad.IsRemove)

	if ad.IsRemove {
		if ad.HasEntries() {
			fmt.Println("Entries: sync skipped")
			fmt.Printf("  ⚠️ Removal advertisement with non-empty entries root cid: %s\n", ad.Entries.Root())
		} else {
			fmt.Println("Entries: None")
		}
		return nil
	}
	fmt.Println("Entries:")
	var entriesOutput string
	entries, err := ad.Entries.Drain()
	if err == datastore.ErrNotFound {
		entriesOutput = "⚠️ Note: More entries were available but not synced due to the configured entries recursion limit or error during traversal."
	} else if err != nil {
		return err
	}

	if printEntries {
		for _, mh := range entries {
			fmt.Printf("  %s\n", mh.B58String())
		}
		fmt.Println("  ---------------------")
	}
	fmt.Printf("  Chunk Count: %d\n", ad.Entries.ChunkCount())
	fmt.Printf("  Total Count: %d\n", len(entries))
	if entriesOutput != "" {
		fmt.Println(entriesOutput)
	}
	return nil
}
