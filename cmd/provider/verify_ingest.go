package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"time"

	httpfinderclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

const verifyChunkSize = 4096

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
	include           sampleSelector
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

type sampleSelector func() bool

func beforeVerifyIngest(cctx *cli.Context) error {
	if samplingProb <= 0 || samplingProb > 1 {
		showVerifyIngestHelp(cctx)
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
			return errVerifyIngestMultipleSources(cctx)
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
	var aggResult verifyIngestResult
	for i := 1; i <= adRecurLimit; i++ {
		ad, err := provClient.GetAdvertisement(cctx.Context, adCid)
		if err != nil {
			return err
		}

		if ad.IsRemove {
			// TODO implement verificiation when ad is about removal
			// When implementing note that entries may be empty and if so we need to walk back the chain to get the list of entries.
			return fmt.Errorf("ad %s is a removal advertisement; verifying ingest from removal advertisement is not yet supported", adCid)
		}

		var entriesOutput string
		allMhs, err := ad.Entries.Drain()
		if err == datastore.ErrNotFound {
			entriesOutput = "; skipped syncing the remaining chunks due to recursion limit"
		} else if err != nil {
			return err
		}

		var mhs []multihash.Multihash
		for _, mh := range allMhs {
			if include() {
				mhs = append(mhs, mh)
			}
		}

		fmt.Printf("Advertisement ID:          %s\n", ad.ID)
		fmt.Printf("Previous Advertisement ID: %s\n", ad.PreviousID)
		fmt.Printf("Total Entries:             %d over %d chunk(s)%s\n", len(allMhs), ad.Entries.ChunkCount(), entriesOutput)
		fmt.Printf("Verifying ingest... (%d/%s)\n", i, adRecurLimitStr)
		finder, err := httpfinderclient.New(indexerAddr)
		if err != nil {
			return err
		}
		result, err := verifyIngestFromMhs(finder, mhs)
		if err != nil {
			return err
		}
		aggResult.add(result)

		// Stop verification if there is no link to previous advertisement.
		if ad.PreviousID == cid.Undef {
			break
		}

		adCid = ad.PreviousID
	}

	return aggResult.printAndExit()
}

func errOneMultihashSourceOnly(cctx *cli.Context) error {
	showVerifyIngestHelp(cctx)
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

	return result.printAndExit()
}

func getOrGenerateCarIndex() (index.IterableIndex, error) {
	cr, err := car.OpenReader(carPath)
	if err != nil {
		return nil, err
	}
	idxReader := cr.IndexReader()
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
	if err := car.LoadIndex(idx, cr.DataReader()); err != nil {
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

	return result.printAndExit()
}

func errInvalidCarIndexFormat() cli.ExitCoder {
	return cli.Exit("CAR index must be in iterable multihash format; see: multicodec.CarMultihashIndexSorted", 1)
}

func errVerifyIngestMultipleSources(cctx *cli.Context) error {
	showVerifyIngestHelp(cctx)
	return cli.Exit("Multiple multihash sources are specified. Only a single source at a time is supported.", 1)
}

func showVerifyIngestHelp(cctx *cli.Context) {
	// Ignore error since it only occues if usage is not found for command.
	_ = cli.ShowCommandHelp(cctx, "verify-ingest")
}

type verifyIngestResult struct {
	total             int
	providerMissmatch int
	present           int
	absent            int
	err               int
	errCauses         []error
	mhs               []multihash.Multihash
}

func (r *verifyIngestResult) passedVerification() bool {
	return r.present == r.total
}

func (r *verifyIngestResult) printAndExit() error {
	fmt.Println()
	fmt.Println("Verification result:")
	fmt.Printf("  # failed to verify:                   %d\n", r.err)
	fmt.Printf("  # unindexed:                          %d\n", r.absent)
	fmt.Printf("  # indexed with another provider ID:   %d\n", r.providerMissmatch)
	fmt.Printf("  # indexed with expected provider ID:  %d\n", r.present)
	fmt.Println("--------------------------------------------")
	fmt.Printf("total Multihashes checked:              %d\n", r.total)
	fmt.Println()
	fmt.Printf("sampling probability:                   %.2f\n", samplingProb)
	fmt.Printf("RNG seed:                               %d\n", rngSeed)
	fmt.Println()
	if r.passedVerification() {
		return cli.Exit("🎉 Passed verification check.", 0)
	}

	if len(r.errCauses) != 0 {
		fmt.Println("Error(s):")
		for _, err := range r.errCauses {
			fmt.Printf("  %s\n", err)
		}
		fmt.Println()
	}

	if printUnindexedMhs && len(r.mhs) != 0 {
		fmt.Println("Unindexed Multihash(es):")
		for _, mh := range r.mhs {
			fmt.Printf("  %s\n", mh.B58String())
		}
		fmt.Println()
	}

	return cli.Exit("❌ Failed verification check.", 1)
}

func (r *verifyIngestResult) add(other *verifyIngestResult) {
	r.total += other.total
	r.providerMissmatch += other.providerMissmatch
	r.present += other.present
	r.absent += other.absent
	r.err += other.err
	r.errCauses = append(r.errCauses, other.errCauses...)
	r.mhs = append(r.mhs, other.mhs...)
}

func verifyIngestFromCarIterableIndex(finder *httpfinderclient.Client, idx index.IterableIndex) (*verifyIngestResult, error) {
	var mhs []multihash.Multihash
	if err := idx.ForEach(func(mh multihash.Multihash, _ uint64) error {
		if include() {
			mhs = append(mhs, mh)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return verifyIngestFromMhs(finder, mhs)
}

func verifyIngestFromMhs(finder *httpfinderclient.Client, mhs []multihash.Multihash) (*verifyIngestResult, error) {
	aggResult := &verifyIngestResult{}
	for len(mhs) >= verifyChunkSize {
		result, err := verifyIngestChunk(finder, mhs[:verifyChunkSize])
		if err != nil {
			return nil, err
		}
		aggResult.add(result)
		mhs = mhs[verifyChunkSize:]
	}
	if len(mhs) != 0 {
		result, err := verifyIngestChunk(finder, mhs)
		if err != nil {
			return nil, err
		}
		aggResult.add(result)
	}
	return aggResult, nil
}

func verifyIngestChunk(finder *httpfinderclient.Client, mhs []multihash.Multihash) (*verifyIngestResult, error) {
	result := &verifyIngestResult{}
	mhsCount := len(mhs)
	result.total = mhsCount
	response, err := finder.FindBatch(context.Background(), mhs)
	if err != nil {
		// Set number multihashes failed to verify instead of returning error since at this point
		// the number of multihashes is known.
		result.err = mhsCount
		err = fmt.Errorf("failed to connect to indexer: %w", err)
		result.errCauses = append(result.errCauses, err)
		return result, nil
	}
	result.mhs = mhs

	if len(response.MultihashResults) == 0 {
		result.absent = mhsCount
		return result, nil
	}

	mhLookup := make(map[string]model.MultihashResult, len(response.MultihashResults))
	for _, mr := range response.MultihashResults {
		mhLookup[mr.Multihash.String()] = mr
	}

	for _, mh := range mhs {
		mr, ok := mhLookup[mh.String()]
		if !ok || len(mr.ProviderResults) == 0 {
			result.absent++
			continue
		}

		var matchedProvider bool
		for _, p := range mr.ProviderResults {
			id := p.Provider.ID.String()
			if id == provId {
				result.present++
				matchedProvider = true
				break
			}
		}
		if !matchedProvider {
			result.providerMissmatch++
		}
	}
	return result, nil
}
