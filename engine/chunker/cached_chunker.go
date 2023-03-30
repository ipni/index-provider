package chunker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/golang/groupcache/lru"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	provider "github.com/ipni/index-provider"
)

var (
	_ EntriesChunker = (*CachedEntriesChunker)(nil)

	log               = logging.Logger("chunker/cached-entries-chunker")
	rootKeyPrefix     = datastore.NewKey("root")
	loverlapKeyPrefix = datastore.NewKey("overlap")
)

type (
	// CachedEntriesChunker is an EntriesChunker that caches the generated chunks using an LRU cache.
	// The chunks can be formatted as any DAG with two current implementations: HamtChunker and
	// ChainChunker.
	//
	// The DAGs are guaranteed to either be fully cached or not at all. If DAGs overlap, the smaller
	// overlapping portion is not evicted unless all the DAGs that link to it are evicted.
	//
	// The number of DAGs cached will be at most equal to the given capacity. The capacity is
	// immutable. DAGs are evicted as needed if the capacity is reached.
	//
	// See: NewCachedEntriesChunker.
	CachedEntriesChunker struct {
		// ds is the backing storage for the cached entry chunks and the caching metadata.
		ds datastore.Batching
		// lsys is used to store the IPLD representation of cached entry chunks.
		lsys ipld.LinkSystem
		// cache is the LRU cache used to determine the chains to keep and the chains to evict from the
		// backing datastore in order of least recently used.
		//
		// The cache uses link to root of a chain as key and a slice of links that make up the chain as
		// value. The rationale behind setting the list of chain links as value is to avoid having to
		// traverse the chain to learn what to delete should the chain be evicted. This makes eviction
		// faster in exchange for slightly larger memory footprint. Only cache keys are persisted in the
		// datastore. During restore, the chain is indeed traversed to populate cache values. See
		// CachedEntriesChunker.restoreCache.
		//
		// Note that all operations on cache must be performed via CachedEntriesChunker.performOnCache
		// to insure context is set in case of an eviction and any errors during eviction are returned
		// gracefully.
		cache *lru.Cache
		// onEvictedErr is used to signal any errors that occur during cache eviction by operations
		// performed via CachedEntriesChunker.performOnCache.
		onEvictedErr error
		// onEvictedCtx is used to set the context to be used during cache eviction by operations
		// performed via CachedEntriesChunker.performOnCache.
		onEvictedCtx context.Context
		// lock synchronizes the chunking, clearing the cache and reading the number of cached chains.
		// Any function that performs Store on the linksystem should also grab this lock. See inline
		// comments in Chunk.
		lock sync.Mutex
		// chunker is the underlying chunker that generates a DAG from a provider.MultihashIterator.
		chunker EntriesChunker
	}

	// NewChunkerFunc instantiates the core EntriesChunker to use for generating advertisement
	// entries DAG.
	NewChunkerFunc func(ls *ipld.LinkSystem) (EntriesChunker, error)
)

// NewCachedEntriesChunker instantiates a new CachedEntriesChunker backed by a given datastore.
//
// The DAGs are generated with the given newChunker and are stored in an LRU cache. Once
// stored, the individual DAGs that make up the entries chain are retrievable in their raw binary
//
//	form via CachedEntriesChunker.GetRawCachedChunk.
//
// The shape of the DAGs is dictated by the underlying chunking logic that is instantiated once via
// newChunker function. See: NewHamtChunkerFunc, NewChainChunkerFunc.
//
// The growth of LRU cache is limited by the given capacity. The capacity specifies the number of
// complete DAGs that are cached, not the DAGs within each chain. The actual storage consumed by
// the cache is a factor of: 1) the DAG shape determined by the underlying chunker, 2) multihash
// length and 3) capacity. For example, a fully populated cache with chunk size of 16384, for
// multihashes of length 128-bit and capacity of 1024 will consume 256MiB of space, i.e.
// (16384 * 1024 * 128b).
//
// This implementation guarantees that for any given chain of entries, either the entire chain is
// cached, or it is not cached at all. When chains overlap, the overlapping portion of the chain is
// not evicted until the larger chain is evicted.
//
// Unless purge is set to true, upon instantiation, the chunker will restore its state from the
// datastore, and prunes the datastore as needed. For example, if the given capacity is smaller than
// the number of chains present in the datastore it will evict chains to respect the given capacity
// in no particular order.
//
// The purge flag specifies whether any existing cache should be cleared on startup. If set, any
// existing cached chunks will be deleted from the datastore. Otherwise, the previously cached
// entries are restored.
//
// Note that a caching metadata with negligible size is persistent in addition to the chunks. The
// caching metadata is checked during restore to determine the root of cached chains, and the number
// of overlapping chunks.
//
// The context is only used cancel a call to this function while it is accessing the data store.
//
// See: CachedEntriesChunker.Chunk, CachedEntriesChunker.GetRawCachedChunk.
func NewCachedEntriesChunker(ctx context.Context, ds datastore.Batching, capacity int, newChunker NewChunkerFunc, purge bool) (*CachedEntriesChunker, error) {
	ls := &CachedEntriesChunker{
		ds:    ds,
		lsys:  cidlink.DefaultLinkSystem(),
		cache: lru.New(capacity),
	}

	ls.lsys.StorageReadOpener = ls.storageReadOpener
	ls.lsys.StorageWriteOpener = ls.storageWriteOpener
	ls.cache.OnEvicted = ls.onEvicted

	chunker, err := newChunker(&ls.lsys)
	if err != nil {
		return nil, err
	}
	ls.chunker = chunker

	// If cache is to be cleared don't bother restoring it.
	if purge {
		if err := ls.Clear(ctx); err != nil {
			log.Errorw("Failed to clear cache", "err", err)
			return nil, err
		}
		log.Info("Cleared cache successfully on start up.")
		return ls, nil
	}

	if err := ls.restoreCache(ctx); err != nil {
		log.Warnw("Failed to restore cache due to either corruption or format change. Falling back on clearing all cached chunks", "err", err)
		if err := ls.Clear(ctx); err != nil {
			log.Errorw("Failed to clear cache", "err", err)
			return nil, err
		}
		log.Info("Cleared all cached chunks successfully since restore failed.")
	}

	return ls, nil
}

func (ls *CachedEntriesChunker) storageWriteOpener(lctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
	buf := bytes.NewBuffer(nil)
	return buf, func(lnk ipld.Link) error {
		ctx := lctx.Ctx
		exists, err := ls.ds.Has(ctx, dsKey(lnk))
		if err != nil {
			log.Errorf("Could not check existence of cache entry for key %s", lnk)
			return err
		}
		if exists {
			return ls.incrementOverlap(ctx, lnk)
		}

		err = ls.ds.Put(ctx, dsKey(lnk), buf.Bytes())
		if err != nil {
			log.Errorf("Could not put cache entry for key %s", lnk)
		}
		return err
	}, nil
}

func (ls *CachedEntriesChunker) storageReadOpener(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
	val, err := ls.ds.Get(lctx.Ctx, dsKey(lnk))
	if err != nil {
		log.Errorf("Could not get cache entry for key %s", lnk)
		return nil, err
	}
	return bytes.NewBuffer(val), nil
}

func (ls *CachedEntriesChunker) onEvicted(k lru.Key, val interface{}) {
	log := log.With("key", k)
	log.Debug("Evicting cache key")
	chunkRoot, ok := k.(ipld.Link)
	if !ok {
		log.Errorw("Unexpected cache key type; expected ipld.Link", "key", k)
		ls.onEvictedErr = errors.New("invalid cache key")
		return
	}
	chunkLinks, ok := val.([]ipld.Link)
	if !ok {
		log.Errorw("Unexpected cache value type; expected []ipld.Link", "value", val)
		ls.onEvictedErr = errors.New("invalid cache value")
		return
	}
	for _, link := range chunkLinks {
		count, err := ls.countOverlap(ls.onEvictedCtx, link)
		if err != nil {
			ls.onEvictedErr = err
			return
		}

		if count == 0 {
			if err := ls.ds.Delete(ls.onEvictedCtx, dsKey(link)); err != nil {
				log.Errorw("failed to delete cache", "key", link, "err", err)
				ls.onEvictedErr = err
				return
			}
			continue
		}

		err = ls.decrementOverlap(ls.onEvictedCtx, link)
		if err != nil {
			ls.onEvictedErr = err
			return
		}
	}

	// Prune the persisted cache key
	err := ls.ds.Delete(ls.onEvictedCtx, ls.dsRootPrefixedKey(chunkRoot))
	if err != nil {
		log.Errorw("failed to prune persisted cache key after eviction", "err", err)
		ls.onEvictedErr = err
	}
}

func dsKey(l ipld.Link) datastore.Key {
	return datastore.NewKey(l.(cidlink.Link).Cid.String())
}

// Chunk chunks the multihashes supplied by the given mhi into a DAG and returns the link to root.
func (ls *CachedEntriesChunker) Chunk(ctx context.Context, mhi provider.MultihashIterator) (ipld.Link, error) {
	ls.lock.Lock()
	defer func() {
		ls.lsys.StorageWriteOpener = ls.storageWriteOpener
		ls.lock.Unlock()
	}()

	var links []ipld.Link
	var linksEnc []byte
	// Intercept the links that are being stored.
	// It is safe to swap the StorageWriteOpener, because:
	//  - Chunk is the only place we expect to write to the linksystem, and
	//  - calls to Chunk are synchronized using a lock.
	// It is also an efficient way to collecting all the links without having to traverse the dag
	// from the root link, or make the EntriesChunker interface more complex.
	ls.lsys.StorageWriteOpener = func(ctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		opener, committer, err := ls.storageWriteOpener(ctx)
		if err != nil {
			return nil, nil, err
		}
		return opener, func(link datamodel.Link) error {
			links = append(links, link)
			linksEnc = append(linksEnc, link.(cidlink.Link).Cid.Bytes()...)
			return committer(link)
		}, nil
	}

	// Store the multihashes in mhi as a DAG and get the root link.
	root, err := ls.chunker.Chunk(ctx, mhi)
	if err != nil {
		return nil, err
	} else if root == nil {
		log.Debugw("multihash iterator returned no elements")
		return nil, nil
	}

	// Store internal mappings for caching purposes.
	err = ls.performOnCache(ctx, func(cache *lru.Cache) { cache.Add(root, links) })
	if err != nil {
		return nil, err
	}
	err = ls.ds.Put(ctx, ls.dsRootPrefixedKey(root), linksEnc)
	if err != nil {
		return nil, err
	}
	return root, ls.sync(ctx)
}

func (ls *CachedEntriesChunker) sync(ctx context.Context) error {
	return ls.ds.Sync(ctx, datastore.NewKey("/"))
}

// GetRawCachedChunk gets the raw cached entry chunk for the given link, or nil if no such caching exists.
func (ls *CachedEntriesChunker) GetRawCachedChunk(ctx context.Context, l ipld.Link) ([]byte, error) {
	raw, err := ls.ds.Get(ctx, dsKey(l))
	if errors.Is(err, datastore.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// Clear purges all stored items from the CachedEntriesChunker.
func (ls *CachedEntriesChunker) Clear(ctx context.Context) error {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	// Clear loaded cache entries first, which calls OnEvict per entry.
	if err := ls.performOnCache(ctx, func(cache *lru.Cache) {
		cache.Clear()
	}); err != nil {
		return err
	}

	// Delete all datastore entries in case the cache was partially loaded.
	// Because, the lru.Clear() above only evicts the loaded cache entries.
	q := dsq.Query{
		KeysOnly: true,
	}
	results, err := ls.ds.Query(ctx, q)
	if err != nil {
		log.Errorw("Failed to query keys while clearing cache", "err", err)
		return err
	}
	defer results.Close()

	for r := range results.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.Error != nil {
			return fmt.Errorf("cannot list cache key to clear: %w", r.Error)
		}

		rawKey := datastore.RawKey(r.Key)
		err := ls.ds.Delete(ctx, rawKey)
		if err != nil {
			log.Errorw("Failed to delete key while clearing cache", "err", err)
			return err
		}
	}
	log.Info("Cleared the cache successfully")
	return nil
}

// Close syncs the backing datastore but does not close it.
// This is because cached entries chunker wraps an existing datastore and does
// not construct it, and the wrapped datastore may be in use elsewhere.
func (ls *CachedEntriesChunker) Close() error {
	return ls.sync(context.TODO())
}

// restoreCache restores the cached entries from the backing datastore and cleans up the datastore
// such that only chunks associated to the root of chains remain in the datastore.
func (ls *CachedEntriesChunker) restoreCache(ctx context.Context) error {
	// Query the root keys of entries chains.
	q := dsq.Query{
		Prefix: rootKeyPrefix.String(),
	}

	results, err := ls.ds.Query(ctx, q)
	if err != nil {
		return err
	}
	defer results.Close()

	// For each root key
	var count int
	for r := range results.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.Error != nil {
			return fmt.Errorf("cannot read cache key: %w", r.Error)
		}

		// The old cache format stored only root CID and traversed the DAG to collect its links
		// during restore. To improve speed in restoring the cache, the new format stores the list
		// of all links in the datastore. If no value is found for the root CID then we are most
		// likely dealing with old cache format.
		// Check this case and return an error. The error would mean the cache gets cleared anyway
		// and will be rebuilt with the new format.
		if len(r.Value) == 0 {
			return errors.New("no value found for root key; old cache format")
		}

		// List all of root's successive links by traversing the chain
		var links []ipld.Link
		vr := bytes.NewReader(r.Value)
		for {
			_, c, err := cid.CidFromReader(vr)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			links = append(links, cidlink.Link{Cid: c})
		}

		// Extract the root link from its datastore key
		rawKey := datastore.RawKey(r.Key)
		l, err := ls.linkFromDsCachePrefixedKey(rawKey)
		if err != nil {
			return err
		}

		// Update in memory cache with root link and its list of links
		err = ls.performOnCache(ctx, func(cache *lru.Cache) { cache.Add(l, links) })
		if err != nil {
			return err
		}
		count++
	}

	// If no root key is present in datastore, it means the cache should be empty
	// Therefore, clear all keys in the datastore.
	//
	// This also makes sure that data cached using previous implementation of caching is cleared.
	if count == 0 {
		// Query all keys in datastore.
		allKeys := dsq.Query{
			KeysOnly: true,
		}
		allKeysResult, err := ls.ds.Query(ctx, allKeys)
		if err != nil {
			return err
		}
		defer allKeysResult.Close()

		// For each key in datastore delete its corresponding value.
		var prunedCount int
		for r := range allKeysResult.Next() {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if r.Error != nil {
				return fmt.Errorf("cannot read cache key: %w", r.Error)
			}
			err := ls.ds.Delete(ctx, datastore.RawKey(r.Key))
			if err != nil {
				return err
			}
			prunedCount++
		}

		// If datastore was pruned, log informative message.
		if prunedCount != 0 {
			log.Infow("No caching metadata is persisted but datastore is non-empty; pruned lingering cache entries", "count", prunedCount)
		}
	} else if ls.Cap() < count {
		// If the cache capacity was too small to restore all entries present, it means cache was
		// evicted during restore and records were pruned as needed.
		//
		// Log an informative message to let the user know.
		log.Infow("Cache capacity is smaller than previously persisted cache; pruned persisted cache.", "persistedCacheCount", count, "capacity", ls.cache.MaxEntries)
	} else {
		log.Debugw("Cache restored successfully", "restoredCacheCount", ls.Len(), "capacity", ls.Cap())
	}

	return nil
}

// performOnCache is a utility to perform operations in CachedEntriesChunker.cache to safely set
// the context to be used during eviction and return errors that may occur as a result of
// eviction if performing the given action indeed causes it.
func (ls *CachedEntriesChunker) performOnCache(ctx context.Context, action func(*lru.Cache)) error {
	ls.onEvictedCtx = ctx
	defer func() {
		ls.onEvictedCtx = nil
		ls.onEvictedErr = nil
	}()
	action(ls.cache)
	err := ls.onEvictedErr
	return err
}

// Cap returns the maximum number of chained entries chunks this cache stores.
//
// Note, the maximum number refers to the number of chains as a unit and not the total sum of
// individual chunks across chains.
func (ls *CachedEntriesChunker) Cap() int {
	return ls.cache.MaxEntries
}

// Len returns the number of chained entries chunks thar are currently stored in cache.
//
// Note, the number refers to the number of chains as a unit and not the total sum of individual
// chunks across chains.
func (ls *CachedEntriesChunker) Len() int {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	return ls.cache.Len()
}

func (ls *CachedEntriesChunker) dsRootPrefixedKey(l ipld.Link) datastore.Key {
	return rootKeyPrefix.Child(dsKey(l))
}

func (ls *CachedEntriesChunker) linkFromDsCachePrefixedKey(ck datastore.Key) (ipld.Link, error) {

	if !rootKeyPrefix.IsAncestorOf(ck) {
		return nil, fmt.Errorf("key is not a prefixed cache key: %s", ck)
	}
	c, err := cid.Decode(ck.BaseNamespace())
	if err != nil {
		return nil, err
	}

	return cidlink.Link{Cid: c}, nil
}

func (ls *CachedEntriesChunker) incrementOverlap(ctx context.Context, lnk ipld.Link) error {
	oKey := ls.dsOverlapPrefixedKey(lnk)
	oVal, err := ls.ds.Get(ctx, oKey)
	var count uint64
	if err != nil {
		if !errors.Is(err, datastore.ErrNotFound) {
			return err
		}
		count = 1
		oVal = make([]byte, 8)
	} else {
		count = binary.LittleEndian.Uint64(oVal) + 1
	}
	binary.LittleEndian.PutUint64(oVal, count)
	return ls.ds.Put(ctx, oKey, oVal)
}

func (ls *CachedEntriesChunker) decrementOverlap(ctx context.Context, lnk ipld.Link) error {
	oKey := ls.dsOverlapPrefixedKey(lnk)
	oVal, err := ls.ds.Get(ctx, oKey)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return nil
		}
		return err
	}

	count := binary.LittleEndian.Uint64(oVal) - 1

	if count < 1 {
		return ls.ds.Delete(ctx, oKey)
	}

	binary.LittleEndian.PutUint64(oVal, count)
	return ls.ds.Put(ctx, oKey, oVal)
}

func (ls *CachedEntriesChunker) countOverlap(ctx context.Context, link ipld.Link) (uint64, error) {
	oKey := ls.dsOverlapPrefixedKey(link)
	oVal, err := ls.ds.Get(ctx, oKey)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return binary.LittleEndian.Uint64(oVal), nil
}

func (ls *CachedEntriesChunker) dsOverlapPrefixedKey(lnk ipld.Link) datastore.Key {
	return loverlapKeyPrefix.Child(dsKey(lnk))
}
