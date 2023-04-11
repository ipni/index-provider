package delegatedrouting

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-libipfs/routing/http/server"
	"github.com/ipfs/go-libipfs/routing/http/types"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/libp2p/go-libp2p/core/peer"

	logging "github.com/ipfs/go-log/v2"

	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("delegatedrouting/listener")
var bitswapMetadata = metadata.Default.New(metadata.Bitswap{})

const (
	// keeping this as "reframe" for backwards compatibility
	delegatedRoutingDSName      = "reframe"
	statsPrintFrequency         = time.Minute
	retryWithBackoffInterval    = 5 * time.Second
	retryWithBackoffMaxAttempts = 3
)

type Listener struct {
	dsWrapper    *dsWrapper
	engine       provider.Interface
	cidTtl       time.Duration
	chunkSize    int
	snapshotSize int
	// Listener maintains in memory indexes for fast key value lookups
	// as well as a rolling double-linked list of CIDs ordered by their timestamp.
	// Once a CID gets advertised, the respective linked list node gets moved to the
	// beginning of the list. To identify CIDs to expire, Listener would walk the list tail to head.
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

type MultihashLister struct {
	CidFetcher func(contextID []byte) (map[cid.Cid]struct{}, error)
}

func (lister *MultihashLister) MultihashLister(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
	contextIdStr := contextIDToStr(contextID)
	cids, err := lister.CidFetcher(contextID)

	if err != nil {
		return nil, err
	}

	mhs := make([]multihash.Multihash, 0, len(cids))
	for c := range cids {
		mhs = append(mhs, c.Hash())
	}

	sort.SliceStable(mhs, func(i, j int) bool {
		return mhs[i].String() < mhs[j].String()
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
		// We don't need to add chunk to the in memory index as old chunks must have been already processed by the engine
		now := time.Now()
		// some timestamps might be missing in the case if the latest snapshot hasn't been persisted due to an error
		// while some chunks containing those CIDs haven been persisted and sent out. In that case - backfilling the missing CIDs with the current timestamp.
		// That is safe to do. Even if those CIDs have expired, they will still expire from the index-provider just at a later date.
		for c := range chunk.Cids {
			if listener.cidQueue.getNodeByCid(c) != nil {
				continue
			}
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

func (listener *Listener) FindProviders(ctx context.Context, key cid.Cid) ([]types.ProviderResponse, error) {
	log.Warn("Received unsupported FindProviders request")
	return nil, errors.New("unsupported find providers request")
}

func (listener *Listener) Provide(ctx context.Context, req *server.WriteProvideRequest) (types.ProviderResponse, error) {
	log.Warn("Received unsupported Provide request")
	return nil, errors.New("unsupported provide request")
}

func (listener *Listener) ProvideBitswap(ctx context.Context, req *server.BitswapWriteProvideRequest) (time.Duration, error) {
	cids := req.Keys
	pid := req.ID
	paddrs := req.Addrs
	startTime := time.Now()
	printFrequency := 10_000
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

	timestamp := time.Now()
	for i, c := range cids {

		// persisting timestamp only if this is not a snapshot
		if len(cids) < listener.snapshotSize {
			err := listener.dsWrapper.recordCidTimestamp(ctx, c, timestamp)
			if err != nil {
				log.Errorw("Error persisting timestamp. Continuing.", "cid", c, "err", err)
				continue
			}
		}

		listElem := listener.cidQueue.getNodeByCid(c)
		if listElem == nil {
			listener.cidQueue.recordCidNode(&cidNode{
				C:         c,
				Timestamp: timestamp,
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
			node.Timestamp = timestamp
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
	lastElem := listener.cidQueue.nodesLl.Back()
	currentTime := time.Now()
	chunksToRemove := make(map[string]*cidsChunk)
	cidsToRemove := make(map[cid.Cid]struct{})
	removedSomeCids := false
	printFrequency := 100
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
		} else {
			chunksRemoved++
		}

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

	// mark the chunk as removed in the datastore. Removed chunks won't be re-loaded on the next initialisation
	chunk.Removed = true
	err = listener.dsWrapper.recordChunkByContextID(ctx, chunk)
	if err != nil {
		chunk.Removed = false
		return err
	}

	return nil
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
	var t *time.Timer

	flushFunc := func() {
		// we don't want flush to happen while re-provide is running
		listener.lock.Lock()
		defer listener.lock.Unlock()
		// flush only if the current chunk has some cids in it and the time since the current chunk has been created is greater than the flush frequency
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

	for {
		select {
		case <-ctx.Done():
			return
		default:
			flushFunc()
		}

		t = time.NewTimer(listener.adFlushFrequency)
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
