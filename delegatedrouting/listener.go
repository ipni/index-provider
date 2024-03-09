package delegatedrouting

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/server"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/libp2p/go-libp2p/core/peer"

	logging "github.com/ipfs/go-log/v2"

	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

/**
index-provider integrates with Kubo by utilising HTTP Delegated Routing API. The API currently supports only GETs requests.
See [IPIP-378](https://github.com/ipfs/specs/pull/378) for the latest updates on PUTs.

index-provider listens to annnouncement from Kubo that are handled by the ProvideBitswap method.
Provide announcements can come either for individual CIDs, for example when a new file gets added to Kubo or for Snapshots.
Snapshot - is a collection of all CIDs that the Kubo node has. Snapshots get reprovided every 12/24 hours or on demand when
one invokes "ipfs bitswap reprovide".

The main job of index-provider is to convert CIDs coming from Kubo into Advertisements and announce them to IPNI using Engine.
index-provider makes sure that all new CIDs are processed sequentuially by protecting ProvideBitswap with Listener.lock.
When new CIDs come in, index-provider adds them to the "current chunk" up until it gets full. There can be only one current chunk at any point of time.
The chunk size is driven by Listener.chunkSize parameter. Once the current chunk is full, index-provider generates a ContextID for it (sha256 over the chunk's CIDs),
advertises the chunk'ss CIDs to IPNI and creates a new empty current chunk. This process happens over and over again. The logic for handling chunks
is located in chunker.go.

Convertion between chunks and Advertisements is done via a custom MultihashLister that is registered with the Engine. When the Engine
asks for a ContextID, MultihashLister will try to find a chunk with that ContextID in in-memory map with a fallabcl to the underlying datastore.
After the chunk has been requested at least once it gets evicted from RAM.

There can be significant time gaps between ProvideBitswap calls, for example when Kubo doesn't have any new data. That can result into
long time before the current chunk gets full. To prevent the current chunk from being stuck, index-provider periodically flushes it - adds whatever CIDs
are in it into the new Advertisement and replaces it with a new current chunk.  Flush frequency is driven by the Listener.adFlushFrequency parameter.

Kubo doesn't give any context on which CIDs have been removed. A CID is considered to be removed if it disappears between
two consequitive Snapshots. To remove a CID index-provider needs to find the Advertisement where that CID has been announced, send IsRM
Advertisement for that ContextID and re-advertise the remaining CIDs with a new ContextID. To determine which CIDs have been removed
index-provider maintains an ordered queue of CIDs by their expiry time (cid_queue.go). When a CID is seen in ProvideBitswap call - it
gets pushed to the end of the queue. In the end of each ProvideBitswap invocation index-provider checks whether any CIDs have expired by looking at the head of the queue
and generates IsRm advertisements for them. This is handled in Listener.removeExpiredCids method. CID "time to live" is driven by Listener.cidTtl parameter.

index-provider offers persistence too. It is handled in ds_wrapper.go. index-provider persists two different datasets: 1. Chunks and 2. CIDs with their expiry times.
Chunks are persisted as a map by their ContextID. CIDs are persisted as "snapshots" - that was done because persisting each CID individually resulted into
significant database load for large nodes. CID snapshot is a binary blob of all CIDs with their expiry times. CIDs snapshot gets persisted only when Kubo reprovides
its snapshot (i.e. once in 12/24 hours). Kubo doesn't give any context on whether BitswapWriteProvideRequest is a snapshot or not. To determine that index-provider
uses Listener.snapshotSize parameter. CIDs snapshots can get big in size to the point that they can't be stored under a single key. To tackle that index-provider slices
each snapshot up which is driven by dsWrapper.snapshotChunkMaxSize parameter. Once a new CIDs snapshot gets persisted, the old one gets removed. Chunks for IsRm Advertisements
are removed from the database too as soon as the Advertisement has been successfully published to the Engine.

On start, index-provider initialises itself from the datastore. First it reads CIDs snapshot and puts all CIDs into the expiry queue. Then index-provider
reads all chunks in pages that is driven by dsWrapper.pageSize parameter. index-provider scans through all CIDs from each chunk and adds them to the expiry queue too if they are
not there already. CIDs might be missing from the expiry queue if the latest snapshot hasn't been persisted due to an error for example. The initialisation logic is handled in
Listener.New.

index-provider periodically reports its operational stats from Listener.stats (number of Advertisements sent, number of CIDs under management and etc.).
*/

var log = logging.Logger("delegatedrouting/listener")
var bitswapMetadata = metadata.Default.New(metadata.Bitswap{})

const (
	// keeping this as "reframe" for backwards compatibility
	delegatedRoutingDSName      = "reframe"
	statsPrintFrequency         = time.Minute
	retryWithBackoffInterval    = 5 * time.Second
	retryWithBackoffMaxAttempts = 3
)

var _ server.ContentRouter = (*Listener)(nil)

type Listener struct {
	dsWrapper    *dsWrapper
	engine       provider.Interface
	cidTtl       time.Duration
	chunkSize    int
	snapshotSize int
	// Listener maintains in memory indexes for fast key value lookups as well as a rolling double-linked list of CIDs
	// ordered by their timestamp. Once a CID gets advertised, the respective linked list node gets moved to the
	// beginning of the list. To identify CIDs to expire, Listener would walk the list tail to head.
	//
	// TODO: offload cid chunks to disk to save RAM
	chunker                *chunker
	cidQueue               *cidQueue
	lastSeenProviderInfo   *peer.AddrInfo
	configuredProviderInfo *peer.AddrInfo
	stats                  *statsReporter
	lock                   sync.Mutex
	adFlushFrequency       time.Duration
	contextCancelFunc      context.CancelFunc
}

func (listener *Listener) FindIPNSRecord(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	return nil, errors.New("not implemented")
}

func (listener *Listener) ProvideIPNSRecord(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	return errors.New("not implemented")
}

type MultihashLister struct {
	CidFetcher func(contextID []byte) (map[cid.Cid]struct{}, error)
}

func (lister *MultihashLister) MultihashLister(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
	contextIdStr := contextIDToStr(contextID)
	cids, err := lister.CidFetcher(contextID)

	if err != nil {
		return nil, err
	}

	mhs := make([]multihash.Multihash, len(cids))
	var i int
	for c := range cids {
		mhs[i] = c.Hash()
		i++
	}

	slices.SortStableFunc(mhs, func(a, b multihash.Multihash) int {
		return bytes.Compare(a, b)
	})

	log.Infow("Returning a chunk from MultihashLister", "contextId", contextIdStr, "size", len(mhs))

	return provider.SliceMultihashIterator(mhs), nil
}

// New creates a delegated routing listener and initialises its state from the provided datastore.
func New(ctx context.Context, engine provider.Interface,
	cidTtl time.Duration,
	chunkSize int,
	snapshotSize int,
	providerId string,
	addresses []string,
	ds datastore.Datastore,
	nonceGen func() []byte,
	opts ...Option,
) (*Listener, error) {

	options := ApplyOptions(opts...)

	cctx, cancelFunc := context.WithCancel(ctx)

	listener := &Listener{
		engine:                 engine,
		cidTtl:                 cidTtl,
		chunkSize:              chunkSize,
		snapshotSize:           snapshotSize,
		dsWrapper:              newDSWrapper(namespace.Wrap(ds, datastore.NewKey(delegatedRoutingDSName)), options.SnapshotMaxChunkSize, options.PageSize),
		lastSeenProviderInfo:   &peer.AddrInfo{},
		configuredProviderInfo: nil,
		chunker:                newChunker(func() int { return chunkSize }, nonceGen),
		cidQueue:               newCidQueue(),
		adFlushFrequency:       options.AdFlushFrequency,
		contextCancelFunc:      cancelFunc,
	}

	listener.stats = newStatsReporter(
		func() int { return len(listener.cidQueue.listNodeByCid) },
		func() int { return len(listener.chunker.chunkByContextId) },
		func() int { return len(listener.chunker.currentChunk.Cids) },
	)

	lister := &MultihashLister{
		CidFetcher: func(contextID []byte) (map[cid.Cid]struct{}, error) {
			ctxIdStr := contextIDToStr(contextID)
			chunk := listener.chunker.getChunkByContextID(ctxIdStr)
			if chunk != nil {
				// remove chunk from the in-memory index as it will be indexed by engine and should not be re-requested anymore
				listener.chunker.removeChunk(chunk)
				return chunk.Cids, nil
			}
			// if chunk doesn't exist in memory - it might have been evicted during deletion
			chunk, err := listener.dsWrapper.getChunkByContextID(ctx, contextID)
			if err == nil {
				listener.stats.incChunkCacheMisses()
				return chunk.Cids, nil
			}
			listener.stats.incChunksNotFound()
			return nil, fmt.Errorf("multihasLister couldn't find a chunk for contextID %s", contextIDToStr(contextID))
		},
	}
	engine.RegisterMultihashLister(lister.MultihashLister)

	log.Info("Initialising from the datastore")
	err := listener.dsWrapper.initialiseFromTheDatastore(ctx, func(n *cidNode) {
		listener.cidQueue.recordCidNode(n)
	}, func(chunk *cidsChunk) {
		// Do not need to add chunk to the in-memory index as old chunks have been already processed by the engine
		now := time.Now()
		for c := range chunk.Cids {
			// if the cid has already been registered - assign the chunk to it
			if elem := listener.cidQueue.getNodeByCid(c); elem != nil {
				node := elem.Value.(*cidNode)
				if node.chunk != nil {
					log.Warnf("Chunk for CID %s has already been assigned. This should never happen", c.String())
				}
				node.chunk = chunk
				continue
			}
			// if the cid hasn't been registered then backfill it with the curent timestamp.
			// some timestamps might be missing in the case if the latest snapshot hasn't been persisted due to an error
			// while some chunks containing those CIDs haven been persisted and sent out. In that case - backfilling the
			// missing CIDs with the current timestamp. That is safe to do. Even if those CIDs have expired, they will still
			// expire from the index-provider just at a later date.
			listener.cidQueue.recordCidNode(&cidNode{C: c, Timestamp: now, chunk: chunk})
		}

	})

	if err != nil {
		return nil, err
	}

	// recording the merged snapshot and cleaning up individual mappings from the datastore
	if len(listener.cidQueue.listNodeByCid) > 0 {
		listener.dsWrapper.recordTimestampsSnapshot(ctx, listener.cidQueue.getTimestampsSnapshot())
	}

	log.Infof("Loaded up %d cids and %d chunks from the datastore.", len(listener.cidQueue.listNodeByCid), len(listener.chunker.chunkByContextId))

	if providerId != "" {
		p, err := peer.Decode(providerId)
		if err != nil {
			return nil, err
		}

		maddrs := make([]multiaddr.Multiaddr, len(addresses))
		for i, s := range addresses {
			a, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				return nil, err
			}
			maddrs[i] = a
		}

		listener.configuredProviderInfo = &peer.AddrInfo{
			ID:    p,
			Addrs: maddrs,
		}
	}

	listener.stats.start()

	// start flush worker
	if options.AdFlushFrequency > 0 {
		go listener.flushWorker(cctx)
	}

	return listener, nil
}

func (listener *Listener) Shutdown() {
	listener.stats.shutdown()
	listener.contextCancelFunc()
}

func (listener *Listener) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	log.Warn("Received unsupported GetIPNS request")
	return nil, errors.New("unsupported get ipns request")
}

func (listener *Listener) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	log.Warn("Received unsupported PutIPNS request")
	return errors.New("unsupported put ipns request")
}

func (listener *Listener) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	log.Warn("Received unsupported FindPeers request")
	return nil, errors.New("unsupported find peers request")
}

func (listener *Listener) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	log.Warn("Received unsupported FindProviders request")
	return nil, errors.New("unsupported find providers request")
}

func (listener *Listener) ProvideBitswap(ctx context.Context, req *server.BitswapWriteProvideRequest) (time.Duration, error) {
	const printFrequency = 10_000
	cids := req.Keys
	pid := req.ID
	paddrs := req.Addrs
	startTime := time.Now()
	listener.lock.Lock()
	defer func() {
		listener.stats.incDelegatedRoutingCallsProcessed()
		log.Infow("Finished processing Provide request.", "time", time.Since(startTime), "len", len(cids))
		listener.lock.Unlock()
	}()

	log.Infof("Received Provide request with %d cids.", len(cids))
	listener.stats.incDelegatedRoutingCallsReceived()

	// shadowing the calling function's context so that cancellation of it doesn't affect processing
	ctx = context.Background()
	// Using mutex to prevent concurrent Provide requests

	if listener.configuredProviderInfo != nil && listener.configuredProviderInfo.ID != pid {
		log.Warnw("Skipping Provide request as its provider is different from the configured one.", "configured", listener.configuredProviderInfo.ID, "received", pid)
		return 0, fmt.Errorf("provider %s isn't allowed", pid)
	}

	if len(listener.lastSeenProviderInfo.ID) > 0 && listener.lastSeenProviderInfo.ID != pid {
		log.Warnw("Skipping Provide request as its provider is different from the last seen one.", "lastSeen", listener.lastSeenProviderInfo.ID, "received", pid)
		return 0, fmt.Errorf("provider %s isn't allowed", pid)
	}

	listener.lastSeenProviderInfo.ID = pid
	listener.lastSeenProviderInfo.Addrs = paddrs

	for i, c := range cids {
		// persisting timestamp only if this is not a snapshot
		if len(cids) < listener.snapshotSize {
			err := listener.dsWrapper.recordCidTimestamp(ctx, c, startTime)
			if err != nil {
				log.Errorw("Error persisting timestamp. Continuing.", "cid", c, "err", err)
				continue
			}
		}

		listElem := listener.cidQueue.getNodeByCid(c)
		if listElem == nil {
			listener.cidQueue.recordCidNode(&cidNode{
				C:         c,
				Timestamp: startTime,
			})
			err := listener.chunker.addCidToCurrentChunk(ctx, c, func(cc *cidsChunk) error {
				return listener.notifyPutAndPersist(ctx, cc)
			})
			if err != nil {
				log.Errorw("Error adding a cid to the current chunk. Continuing.", "cid", c, "err", err)
				listener.cidQueue.removeCidNode(c)
				continue
			}
		} else {
			node := listElem.Value.(*cidNode)
			node.Timestamp = startTime
			listener.cidQueue.recordCidNode(node)
			// if no existing chunk has been found for the cid - adding it to the current one
			// This can happen in the following cases:
			//     * when currentChunk disappears between restarts as it doesn't get persisted until it's advertised
			//     * when the same cid comes multiple times within the lifespan of the same chunk
			//	   * after a error to generate a replacement chunk
			if node.chunk == nil {
				err := listener.chunker.addCidToCurrentChunk(ctx, c, func(cc *cidsChunk) error {
					return listener.notifyPutAndPersist(ctx, cc)
				})
				if err != nil {
					log.Errorw("Error adding a cid to the current chunk. Continuing.", "cid", c, "err", err)
					continue
				}
			}
			listener.stats.incExistingCidsProcessed()
		}

		listener.stats.incCidsProcessed()
		// Doing some logging for larger requests
		if i != 0 && i%printFrequency == 0 {
			log.Infof("Processed %d out of %d CIDs. startTime=%v", i, len(cids), startTime)
		}
	}
	removedSomething, err := listener.removeExpiredCids(ctx)
	if err != nil {
		log.Warnw("Error removing expired cids.", "err", err)
	}

	// if that was a snapshot or some cids have expired - persisting timestamps as binary blob
	if removedSomething || len(cids) >= listener.snapshotSize {
		listener.dsWrapper.recordTimestampsSnapshot(ctx, listener.cidQueue.getTimestampsSnapshot())
	}
	return time.Duration(listener.cidTtl), nil
}

// Revise logic here
func (listener *Listener) removeExpiredCids(ctx context.Context) (bool, error) {
	const printFrequency = 100
	lastElem := listener.cidQueue.nodesLl.Back()
	currentTime := time.Now()
	chunksToRemove := make(map[string]*cidsChunk)
	cidsToRemove := make(map[cid.Cid]struct{})
	removedSomeCids := false
	var cidsRemoved, chunksRemoved, chunksReplaced int
	// find expired cids and their respective chunks
	for {
		if lastElem == nil {
			break
		}
		lastNode := lastElem.Value.(*cidNode)

		if currentTime.Sub(lastNode.Timestamp) <= listener.cidTtl {
			break
		}

		chunk := lastNode.chunk
		lastElem = lastElem.Prev()
		removedSomeCids = true
		// chunk field can be nil for cids from the current chunk that has not been advertised yet
		if chunk != nil {
			cidsToRemove[lastNode.C] = struct{}{}
			ctxIdStr := contextIDToStr(chunk.ContextID)
			chunksToRemove[ctxIdStr] = chunk
		} else {
			listener.cidQueue.removeCidNode(lastNode.C)
		}
	}

	// remove old chunks and generate new chunks less the expired cids
	counter := 0
	for _, chunkToRemove := range chunksToRemove {
		counter++
		oldCtxIdStr := contextIDToStr(chunkToRemove.ContextID)

		// removing the expired chunk first. If that fails - don't update indexs / datastore so that we can retry deletion
		// on the next iteration
		err := listener.notifyRemoveAndPersist(ctx, chunkToRemove)
		if err != nil {
			log.Warnw("Error removing a chunk. Continuing.", "contextID", oldCtxIdStr, "err", err)
			for c := range chunkToRemove.Cids {
				delete(cidsToRemove, c)
			}
			continue
		}
		chunksRemoved++

		replacementChunk := &cidsChunk{Cids: make(map[cid.Cid]struct{}, listener.chunkSize), Removed: false}

		for c := range chunkToRemove.Cids {
			// if cid hasn't expired - adding it to the replacement chunk
			if _, ok := cidsToRemove[c]; !ok {
				replacementChunk.Cids[c] = struct{}{}
				continue
			}

			// cleaning up the expired cid
			listener.cidQueue.removeCidNode(c)
			delete(cidsToRemove, c)
			listener.stats.incCidsExpired()
			cidsRemoved++
		}
		// only generating a new chunk if it has some cids left in it
		if len(replacementChunk.Cids) > 0 {
			replacementChunk.ContextID = listener.chunker.generateContextID(replacementChunk.Cids)
			newCtxIdStr := contextIDToStr(replacementChunk.ContextID)
			err = listener.notifyPutAndPersist(ctx, replacementChunk)
			if err != nil {
				log.Warnw("Error creating replacement chunk. Continuing.", "contextID", newCtxIdStr, "err", err)
				// it's ok to continue - remaining CIDs are going to be picked up on the next snapshot
				continue
			}
			chunksReplaced++
		} else {
			log.Infof("No CIDs left to generate a replacement chunk for %s.", contextIDToStr(chunkToRemove.ContextID))
		}

		if counter != 0 && counter%printFrequency == 0 {
			log.Infof("Cleaning up chunk %d out of %d.", counter, len(chunksToRemove))
		}
	}

	// we might have still some expired cids left, that didn't have any chunk associated to them
	for c := range cidsToRemove {
		// cleaning up the expired cid
		listener.cidQueue.removeCidNode(c)
	}

	log.Infow("Finished cleaning up.", "cidsExpired", cidsRemoved, "chunksExpired", chunksRemoved, "chunksReplaced", chunksReplaced)

	return removedSomeCids, nil
}

func (listener *Listener) notifyRemoveAndPersist(ctx context.Context, chunk *cidsChunk) error {
	ctxIdStr := contextIDToStr(chunk.ContextID)
	log.Infof("Notifying Remove for chunk=%s", ctxIdStr)

	// notify the indexer
	err := RetryWithBackoff(func() error {
		_, e := listener.engine.NotifyRemove(ctx, listener.provider(), chunk.ContextID)
		if e == provider.ErrAlreadyAdvertised {
			e = nil
		}
		return e
	}, retryWithBackoffInterval, retryWithBackoffMaxAttempts)

	if err != nil {
		return err
	}
	listener.stats.incRemoveAdsSent()

	// remove the chunk from the in-memory index
	listener.chunker.removeChunk(chunk)

	// delete chunk from the datastore
	return listener.dsWrapper.deleteChunk(ctx, chunk)
}

func (listener *Listener) notifyPutAndPersist(ctx context.Context, chunk *cidsChunk) error {
	ctxIdStr := contextIDToStr(chunk.ContextID)
	log.Infof("Notifying Put for chunk=%s, provider=%s, addrs=%q, cidsTotal=%d", ctxIdStr, listener.provider(), listener.addrs(), len(chunk.Cids))

	// add chunk into in-memory indexes so that multihash listed can find it
	listener.chunker.addChunk(chunk)

	// update the datastore
	err := listener.dsWrapper.recordChunkByContextID(ctx, chunk)
	if err != nil {
		return err
	}

	// delete the chunk from the datastore
	err = RetryWithBackoff(func() error {
		_, e := listener.engine.NotifyPut(ctx, &peer.AddrInfo{ID: listener.provider(), Addrs: listener.addrs()}, chunk.ContextID, bitswapMetadata)
		if e == provider.ErrAlreadyAdvertised {
			e = nil
		}
		return e
	}, retryWithBackoffInterval, retryWithBackoffMaxAttempts)

	if err != nil {
		// if there was an error - reverting index update
		listener.chunker.removeChunk(chunk)
		return err
	}

	listener.stats.incPutAdsSent()

	// update the chunk in the cid queue
	for c := range chunk.Cids {
		listener.cidQueue.assignCidsChunk(c, chunk)
	}

	return nil
}

func (listener *Listener) provider() peer.ID {
	if listener.configuredProviderInfo == nil {
		return listener.lastSeenProviderInfo.ID
	}
	return listener.configuredProviderInfo.ID
}

func (listener *Listener) addrs() []multiaddr.Multiaddr {
	if listener.configuredProviderInfo == nil {
		return listener.lastSeenProviderInfo.Addrs
	}
	return listener.configuredProviderInfo.Addrs
}

func contextIDToStr(contextID []byte) string {
	return base64.StdEncoding.EncodeToString(contextID)
}

func (listener *Listener) flushWorker(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	flushFunc := func() {
		// we don't want flush to happen while re-provide is running
		listener.lock.Lock()
		defer listener.lock.Unlock()
		// flush only if the current chunk has some cids in it and the time since the current chunk has been created is
		// greater than the flush frequency
		if len(listener.chunker.currentChunk.Cids) > 0 &&
			time.Since(listener.chunker.currentChunkTime) > listener.adFlushFrequency {
			err := listener.chunker.flushCurrentChunk(ctx, func(cc *cidsChunk) error {
				return listener.notifyPutAndPersist(ctx, cc)
			})
			if err != nil {
				log.Warnw("Error flushing current chunk", "err", err)
			}
		}
	}

	t := time.NewTicker(listener.adFlushFrequency)
	defer t.Stop()

	for {
		flushFunc()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func RetryWithBackoff(f func() error, initialInterval time.Duration, times int) error {
	sleepTime := initialInterval
	attempt := 0
	for {
		err := f()
		if err == nil {
			return nil
		}
		attempt++
		if attempt == times {
			return err
		}
		log.Infow("Retrying execution because of an error", "err", err, "attempt", attempt, "sleepTime", sleepTime)
		time.Sleep(sleepTime)
		sleepTime = sleepTime * 2
	}
}
