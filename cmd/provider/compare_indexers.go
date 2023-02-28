package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipni/index-provider/cmd/provider/internal"
	finderhttpclient "github.com/ipni/storetheindex/api/v0/finder/client/http"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var CompareIndexersCmd = &cli.Command{
	Name:    "compare",
	Aliases: []string{"cmp"},
	Usage:   "Compares progress between two indexers and prints out how far off are they.",
	Flags:   compareIndexersFlags,
	Action:  doCompareIndexers,
}

const (
	unknownByTargetErr = "unknown by target"
	noMatchErr         = "no match"
)

// targets is a list of providers fetched from the target
var targets []*model.ProviderInfo

// sources is a list from providers fetched from the source
var sources []*model.ProviderInfo

// targetsMap is a list of target providers indexed by peer.ID
var targetsMap map[peer.ID]*model.ProviderInfo

// stat is a stats about provider
type stat struct {
	// pid is id of the provider
	pid peer.ID
	// lag represents ad chain difference between the source and the target. If its positive then the target lags from the source.
	// if it's negative then the source lags from the target. If it's zero then source and target match.
	lag int
	// err is an error that occured during invocation. If it's not nil then lag value should be ignored.
	err error
	// dur is the total time taken to produce stats for this peer
	dur time.Duration
}

func (s *stat) print() {
	fmt.Printf("peer: %s, timeTaken: %v, lag: %d, error: %v\n", s.pid.String(), s.dur, s.lag, s.err)
}

func doCompareIndexers(cctx *cli.Context) error {
	flag.Parse()

	sourceClient, err := finderhttpclient.New(sourceIndexerFlagValue)
	if err != nil {
		log.Fatal(err)
	}

	targetClient, err := finderhttpclient.New(targetIndexerFlagValue)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	sources, err = sourceClient.ListProviders(ctx)
	if err != nil {
		log.Fatal(err)
	}

	targets, err = targetClient.ListProviders(ctx)
	if err != nil {
		log.Fatal(err)
	}

	targetsMap = make(map[peer.ID]*model.ProviderInfo)
	for _, target := range targets {
		if target.AddrInfo.ID == "" {
			continue
		}
		targetsMap[target.AddrInfo.ID] = target
	}

	numJobs := len(sources)

	jobs := make(chan *model.ProviderInfo, numJobs)
	results := make(chan stat, numJobs)
	parallelism := int(math.Min(float64(parallelismFlagValue), float64(numJobs)))

	for i := 1; i < parallelism; i++ {
		go worker(jobs, results)
	}

	for i := 1; i < numJobs; i++ {
		jobs <- sources[i]
	}
	close(jobs)

	stats := make([]stat, 0, numJobs)
	for i := 1; i <= numJobs; i++ {
		s := <-results
		stats = append(stats, s)
	}

	printStats(stats)

	return nil
}

func printStats(stats []stat) {
	unknownByTargetErrors := 0
	fetchErrors := 0
	noMatchErrors := 0
	match := 0
	lagsAtTarget := 0
	lagsAtSource := 0

	for _, s := range stats {
		if s.err == nil {
			if s.lag == 0 {
				match++
			} else if s.lag > 0 {
				lagsAtTarget++
			} else {
				lagsAtSource++
			}
			continue
		}

		switch s.err.Error() {
		case unknownByTargetErr:
			unknownByTargetErrors++
		case noMatchErr:
			noMatchErrors++
		default:
			fetchErrors++
		}
	}

	fmt.Println("Stats:")
	fmt.Printf("     Total scanned:        %d\n", len(stats))
	fmt.Printf("     Match:                %d\n", match)
	fmt.Printf("     Lags at target:       %d\n", lagsAtTarget)
	fmt.Printf("     Lags at source:       %d\n", lagsAtSource)
	fmt.Printf("     Fetch errors:         %d\n", fetchErrors)
	fmt.Printf("     Diff to deep:         %d\n", noMatchErrors)
	fmt.Printf("     Unknown by target:    %d\n", unknownByTargetErrors)

}

// worker is a function that gets executed by concurrent goroutines.
func worker(jobs <-chan *model.ProviderInfo, results chan<- stat) {
	ctx := context.Background()
	for j := range jobs {
		stat := stat{
			pid: j.AddrInfo.ID,
		}

		start := time.Now()
		if target, exists := targetsMap[stat.pid]; !exists {
			stat.err = errors.New(unknownByTargetErr)
		} else if j.LastAdvertisement != target.LastAdvertisement {
			lag, err := findLag(ctx, *j.Publisher, j.LastAdvertisement, target.LastAdvertisement, topicFlagValue, int(depthFlagValue))

			if err != nil {
				stat.err = err
			} else {
				stat.lag = lag
			}
		}
		stat.dur = time.Since(start)
		stat.print()
		results <- stat
	}
}

// findLag finds a lag between source and target cids. If the value is positive then target lags form the source. If it's negative - then the source lags form the target.
// If error is not nil then the value of the lag should be ignored.
func findLag(ctx context.Context, addr peer.AddrInfo, scid, tcid cid.Cid, topic string, depth int) (int, error) {
	client, scids, err := fetchAdChain(ctx, nil, addr, topic, scid, depth)
	if err != nil {
		return 0, err
	}

	_, tcids, err := fetchAdChain(ctx, client, addr, topic, tcid, depth)
	if err != nil {
		return 0, err
	}

	// index source and target cids. Key is a cid, value is its index in the ad chain.
	smap := map[cid.Cid]int{}
	for i, c := range scids {
		smap[c] = i
	}

	tmap := map[cid.Cid]int{}
	for i, c := range tcids {
		tmap[c] = i
	}

	if lag, ok := smap[tcids[0]]; ok {
		return lag, nil
	}
	if lag, ok := tmap[scids[0]]; ok {
		return -lag, nil
	}

	return 0, errors.New(noMatchErr)
}

// fetchAdChain fetches an ad chain using underlying client
func fetchAdChain(ctx context.Context, c internal.ProviderClient, addr peer.AddrInfo, topic string, startCid cid.Cid, depth int) (internal.ProviderClient, []cid.Cid, error) {
	var err error
	if c == nil {
		c, err = internal.NewProviderClient(addr, internal.WithTopicName(topic),
			internal.WithMaxSyncRetry(2),
			internal.WithEntriesRecursionLimit(selector.RecursionLimitDepth(1)))

		if err != nil {
			return nil, nil, err
		}
	}

	ads, err := c.GetAdvertisments(ctx, startCid, depth)
	if err != nil {
		return nil, nil, err
	}

	cids := make([]cid.Cid, 0, len(ads))
	for _, ad := range ads {
		cids = append(cids, ad.ID)
	}

	return c, cids, nil
}
