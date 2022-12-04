package main

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipni/index-provider/cmd/provider/internal"
	httpfinderclient "github.com/ipni/storetheindex/api/v0/finder/client/http"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

var (
	carPath           string
	carIndexPath      string
	indexerAddr       string
	provId            string
	samplingProb      float64
	rngSeed           int64
	adCidStr          string
	adRecurLimit      int
	printUnindexedMhs bool
	include           internal.Sampler
	VerifyIngestCmd   = &cli.Command{
		Name:  "verify-ingest",
		Usage: "Verifies ingestion of multihashes to an indexer node from a CAR file or a CARv2 Index",
		Description: `This command verifies whether a list of multihashes are ingested by an indexer node with the 
expected provider Peer ID. The multihashes to verify can be supplied from one of the following 
sources:
- Multiaddr info to the provider's GraphSync or HTTP endpoint.
- Path to a CAR file (i.e. --from-car)
- Path to a CARv2 index file in iterable multihash format (i.e. --from-car-index)

The multiaddr info may point to the provider's GraphSync or HTTP publisher endpoint. Note that if 
GraphSync endpoint is specified the "topic" flag will be taken into effect and must point to the 
topic name on which advertisements are published. The user may optionally specify an advertisement
CID as the source of multihash entries. If not specified, the latest advertisement is fetched
dynamically and its entries are used as the source of multihashes.

The path to CAR files may point to any CAR version (CARv1 or CARv2). The list of multihashes are 
generated automatically from the CAR payload if no suitable index is not present. Note that the 
index is generated if either no index is present, or the existing index format or characteristics do
not match the desired values.

The path to CARv2 index file must point to an index in iterable multihash format, i.e. have 
'multicodec.CarMultihashIndexSorted'. See: https://github.com/ipld/go-car

In addition to the source, the user must also specify the address of indexer node and the expected 
provider's Peer ID.

By default, all of multihashes from source are used for verification. The user may specify a 
sampling probability to define the chance of each multihash being selected for verification. A
uniform random distribution is used to select whether a multihash should be used. The random number
generator is non-deterministic by default. However, a seed may be specified to make the selection
deterministic for debugging purposes.

Example usage:

* Verify ingest from provider's' GraphSync endpoint for a specific advertisement CID, selecting 50%
 of available multihashes using deterministic random number generator, seeded with '1413':
	provider verify-ingest \
		--from-provider '/ip4/1.2.3.4/tcp/24002/p2p/12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ' \
		--to 192.168.2.100:3000 \
		--provider-id 12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ \
		--ad-cid baguqeeqqcbuegh2hzk7sukqpsz24wg3tk4
		--sampling-prob 0.5 \
		--rng-seed 1413

* Verify ingestion from CAR file, selecting 50% of available multihashes using a deterministic 
random number generator, seeded with '1413':
	provider verify-ingest \
		--from-car my-dag.car \
		--to 192.168.2.100:3000 \
		--provider-id 12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ \
		--sampling-prob 0.5 \
		--rng-seed 1413

* Verify ingestion from CARv2 index file using all available multihashes, i.e. unspecified 
sampling probability defaults to 1.0 (100%):
	provider verify-ingest \
		--from-car my-idx.idx \
		--to 192.168.2.100:3000 \
		--provider-id 12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ

The output respectively prints:
- The number of multihashes the tool failed to verify, e.g. due to communication error.
- The number of multihashes not indexed by the indexer.
- The number of multihashes known by the indexer but not associated to the given provider Peer ID.
- The number of multihashes known with expected provider Peer ID.
- And finally, total number of multihashes verified.

A verification is considered as passed when the total number of multihashes checked matches the 
number of multihashes that are indexed with expected provider Peer ID.

Example output:

* Passed verification:
	Verification result:
	  # failed to verify:                   0
	  # unindexed:                          0
	  # indexed with another provider ID:   0
	  # indexed with expected provider ID:  1049
	--------------------------------------------
	total Multihashes checked:              1049
	
	sampling probability:                   1.00
	RNG seed:                               0
	
	🎉 Passed verification check.

* Failed verification:
	Verification result:
	  # failed to verify:                   0
	  # unindexed:                          20
	  # indexed with another provider ID:   0
	  # indexed with expected provider ID:  0
	--------------------------------------------
	total Multihashes checked:              20
	
	sampling probability:                   0.50
	RNG seed:                               42
	
	❌ Failed verification check.
`,
		Aliases: []string{"vi"},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "from-provider",
				Usage: "The provider's endpoint address in form of libp2p multiaddr info. If set, an optional advertisement CID may be specified." +
					"Example GraphSync endpoint: /ip4/1.2.3.4/tcp/1234/p2p/12D3KooWE8yt84RVwW3sFcd6WMjbUdWrZer2YtT4dmtj3dHdahSZ  ",
				Aliases:     []string{"fp"},
				Destination: &pAddrInfo,
			},
			&cli.PathFlag{
				Name:        "from-car",
				Usage:       "Path to the CAR file from which to extract the list of multihash for verification.",
				Aliases:     []string{"fc"},
				Destination: &carPath,
			},
			&cli.PathFlag{
				Name:        "from-car-index",
				Usage:       "Path to the CAR index file from which to extract the list of multihash for verification.",
				Aliases:     []string{"fci"},
				Destination: &carIndexPath,
			},

			&cli.StringFlag{
				Name:        "to",
				Aliases:     []string{"i"},
				Usage:       "The host:port of the indexer node to which to verify ingestion.",
				Destination: &indexerAddr,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "provider-id",
				Aliases:     []string{"pid"},
				Usage:       "The peer ID of the provider that should be associated to multihashes.",
				Required:    true,
				Destination: &provId,
			},
			&cli.Float64Flag{
				Name:        "sampling-prob",
				Aliases:     []string{"sp"},
				Usage:       "The uniform random probability of selecting a multihash for verification specified as a value between 0.0 and 1.0.",
				DefaultText: "'1.0' i.e. 100% of multihashes will be checked for verification.",
				Value:       1.0,
				Destination: &samplingProb,
			},
			&cli.Int64Flag{
				Name:    "rng-seed",
				Aliases: []string{"rs"},
				Usage: "The seed to use for the random number generator that selects verification samples. " +
					"This flag has no impact if sampling probability is set to 1.0.",
				DefaultText: "Non-deterministic.",
				Destination: &rngSeed,
			},
			&cli.StringFlag{
				Name:        "ad-cid",
				Aliases:     []string{"a"},
				Usage:       "The advertisement CID. This Option only takes effect if source of multihashes is set to a provider.",
				DefaultText: "Dynamically fetch the latest advertisement CID",
				Destination: &adCidStr,
			},
			&cli.IntFlag{
				Name:        "ad-recursive-limit",
				Aliases:     []string{"r"},
				Usage:       "The number of advertisements to verify. This option only takes effect if the source of multihashes is set to provider's advertisement chain.",
				Value:       1,
				DefaultText: "Verify a single advertisement only.",
				Destination: &adRecurLimit,
			},
			adEntriesRecurLimitFlag,
			&cli.StringFlag{
				Name:        "topic",
				Aliases:     []string{"t"},
				Usage:       "The topic name on which advertisements are published by the provider. This Option only takes effect if source of multihashes is set to a provider.",
				Value:       "/indexer/ingest/mainnet",
				Destination: &topic,
			},
			&cli.BoolFlag{
				Name:        "print-unindexed-mhs",
				Usage:       "Whether to print the multihashes that are not indexed by the indexer. Note that the multihashes are only printed if the indexer is successfully contacted and multihash is not found.",
				Aliases:     []string{"pum"},
				Destination: &printUnindexedMhs,
			},
		},
		Before: beforeVerifyIngest,
		Action: doVerifyIngest,
	}
)

func beforeVerifyIngest(_ *cli.Context) error {
	if samplingProb <= 0 || samplingProb > 1 {
		return cli.Exit("Sampling probability must be larger than 0.0 and smaller or equal to 1.0.", 1)
	}

	if samplingProb == 1 {
		include = func() bool {
			return true
		}
	} else {
		if rngSeed == 0 {
			rngSeed = time.Now().UnixNano()
		}
		rng := rand.New(rand.NewSource(rngSeed))
		include = func() bool {
			return rng.Float64() <= samplingProb
		}
	}
	if adRecurLimit < 0 {
		return fmt.Errorf("advertiserment recursion limit cannot be less than zero; got %d", adRecurLimit)
	}
	return nil
}

func doVerifyIngest(cctx *cli.Context) error {
	if pAddrInfo != "" {
		if carPath != "" || carIndexPath != "" {
			return errOneMultihashSourceOnly(cctx)
		}
		var err error
		provClient, err = toProviderClient(pAddrInfo, topic)
		if err != nil {
			return err
		}
		return doVerifyIngestFromProvider(cctx)
	}

	if carPath != "" {
		if carIndexPath != "" {
			return errVerifyIngestMultipleSources()
		}
		carPath = path.Clean(carPath)
		return doVerifyIngestFromCar(cctx)
	}

	if carIndexPath == "" {
		return errOneMultihashSourceOnly(cctx)
	}
	carIndexPath = path.Clean(carIndexPath)
	return doVerifyIngestFromCarIndex()
}

func doVerifyIngestFromProvider(cctx *cli.Context) error {
	if adCidStr != "" {
		var err error
		adCid, err = cid.Decode(adCidStr)
		if err != nil {
			return err
		}
	}

	var adRecurLimitStr string
	if adRecurLimit == 0 {
		adRecurLimitStr = "∞"
	} else {
		adRecurLimitStr = fmt.Sprintf("%d", adRecurLimit)
	}
	var aggResult internal.VerifyIngestResult
	finder, err := httpfinderclient.New(indexerAddr)
	if err != nil {
		return err
	}

	stats := internal.NewAdStats(include)

	for i := 1; i <= adRecurLimit; i++ {
		ad, err := provClient.GetAdvertisement(cctx.Context, adCid)
		if err != nil {
			if ad == nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "⚠️ Failed to fully sync advertisement %s. Output shows partially synced ad.\n  Error: %s\n", adCid, err.Error())
		}
		ads := stats.Sample(ad)

		fmt.Printf("Advertisement ID:          %s\n", ad.ID)
		fmt.Printf("Previous Advertisement ID: %s\n", ad.PreviousID)
		fmt.Printf("Verifying ingest... (%d/%s)\n", i, adRecurLimitStr)
		if ads.NoLongerProvided {
			fmt.Println("🧹 Removed in later advertisements; skipping verification.")
		} else if ad.IsRemove {
			fmt.Println("✂️ Removal advertisement; skipping verification.")
		} else if !ad.HasEntries() {
			fmt.Println("Has no entries; skipping verification.")
		} else {
			var entriesOutput string
			if ads.PartiallySynced {
				entriesOutput = "; ad entries are partially synced due to: " + ads.SyncErr.Error()
			}

			fmt.Printf("Total Entries:             %d over %d chunk(s)%s\n", ads.MhCount, ads.ChunkCount, entriesOutput)
			fmt.Print("Verification: ")
			if len(ads.MhSample) == 0 {
				fmt.Println("🔘 Skipped; sampling did not include any multihashes.")
			} else {
				result, err := internal.VerifyIngestFromMhs(finder, provId, ads.MhSample)
				if err != nil {
					return err
				}
				aggResult.Add(result)
				if result.PassedVerification() {
					fmt.Println("✅ Pass")
				} else {
					fmt.Println("❌ Fail")
				}
			}
		}
		fmt.Println("-----------------------")

		// Stop verification if there is no link to previous advertisement.
		if ad.PreviousID == cid.Undef {
			break
		}

		adCid = ad.PreviousID
	}

	printVerificationResult(&aggResult)
	printAdStats(stats)
	return nil
}

func errOneMultihashSourceOnly(cctx *cli.Context) error {
	return cli.Exit("Exactly one multihash source must be specified.", 1)
}

func doVerifyIngestFromCar(_ *cli.Context) error {
	idx, err := getOrGenerateCarIndex()
	if err != nil {
		return err
	}

	finder, err := httpfinderclient.New(indexerAddr)
	if err != nil {
		return err
	}

	result, err := verifyIngestFromCarIterableIndex(finder, idx)
	if err != nil {
		return err
	}

	printVerificationResult(result)
	return nil
}

func getOrGenerateCarIndex() (index.IterableIndex, error) {
	cr, err := car.OpenReader(carPath)
	if err != nil {
		return nil, err
	}
	idxReader, err := cr.IndexReader()
	if err != nil {
		return nil, err
	}

	if idxReader == nil {
		return generateIterableIndex(cr)
	}

	idx, err := index.ReadFrom(idxReader)
	if err != nil {
		return nil, err
	}
	if idx.Codec() != multicodec.CarMultihashIndexSorted {
		// Index doesn't contain full multihashes; generate it.
		return generateIterableIndex(cr)
	}
	return idx.(index.IterableIndex), nil
}

func generateIterableIndex(cr *car.Reader) (index.IterableIndex, error) {
	idx := index.NewMultihashSorted()
	dr, err := cr.DataReader()
	if err != nil {
		return nil, err
	}
	if err := car.LoadIndex(idx, dr); err != nil {
		return nil, err
	}
	return idx, nil
}

func doVerifyIngestFromCarIndex() error {
	idxFile, err := os.Open(carIndexPath)
	if err != nil {
		return err
	}
	idx, err := index.ReadFrom(idxFile)
	if err != nil {
		return err
	}

	iterIdx, ok := idx.(index.IterableIndex)
	if !ok {
		return errInvalidCarIndexFormat()
	}

	finder, err := httpfinderclient.New(indexerAddr)
	if err != nil {
		return err
	}

	result, err := verifyIngestFromCarIterableIndex(finder, iterIdx)
	if err != nil {
		return err
	}

	printVerificationResult(result)
	return nil
}

func errInvalidCarIndexFormat() cli.ExitCoder {
	return cli.Exit("CAR index must be in iterable multihash format; see: multicodec.CarMultihashIndexSorted", 1)
}

func errVerifyIngestMultipleSources() error {
	return cli.Exit("Multiple multihash sources are specified. Only a single source at a time is supported.", 1)
}

func verifyIngestFromCarIterableIndex(finder *httpfinderclient.Client, idx index.IterableIndex) (*internal.VerifyIngestResult, error) {
	var mhs []multihash.Multihash
	if err := idx.ForEach(func(mh multihash.Multihash, _ uint64) error {
		if include() {
			mhs = append(mhs, mh)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return internal.VerifyIngestFromMhs(finder, provId, mhs)
}
