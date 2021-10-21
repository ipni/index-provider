package engine

import (
	"context"
	"fmt"
	"sync"

	lru "github.com/golang/groupcache/lru"
	ds "github.com/ipfs/go-datastore"
	dsn "github.com/ipfs/go-datastore/namespace"
	dsq "github.com/ipfs/go-datastore/query"
)

const linksCachePath = "/cache/links"

// dsCache is a LRU-cache that stores keys in memory and values in a datastore.
type dsCache struct {
	dstore ds.Datastore
	lru    *lru.Cache
	lock   sync.RWMutex
}

// newDsCache creates a new dsCache instance.  The context is only used cancel
// a call to this function while it is accessing the data store.
func newDsCache(ctx context.Context, dstore ds.Datastore, capacity int) (*dsCache, error) {
	dstore = dsn.Wrap(dstore, ds.NewKey(linksCachePath))

	// Create LRU cache that deletes value from datastore when key is eviceted
	// from cache.
	cache := lru.New(capacity)

	c := &dsCache{
		dstore: dstore,
		lru:    cache,
	}

	// Load all keys from datastore into lru cache.
	if err := c.loadKeys(ctx); err != nil {
		return nil, err
	}

	// Set the function to remove items from the datasotre when they are
	// evicted from lru.
	cache.OnEvicted = func(key lru.Key, val interface{}) {
		// Remove item from datastore that was evicted from LRU.
		err := dstore.Delete(key.(ds.Key))
		if err != nil {
			log.Errorf("Error removing link cache value from datastore: %s", err)
		}
	}

	return c, nil
}

// Get implements datastore interface.
func (c *dsCache) Get(key ds.Key) ([]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	_, ok := c.lru.Get(key)
	if !ok {
		return nil, ds.ErrNotFound
	}
	val, err := c.dstore.Get(key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Put implements datastore interface.
func (c *dsCache) Put(key ds.Key, val []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.lru.Add(key, nil)
	return c.dstore.Put(key, val)
}

// Delete implements datastore interface.
func (c *dsCache) Delete(key ds.Key) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.lru.Remove(key)
	return nil
}

// GetSize implements datastore interface.
func (c *dsCache) GetSize(key ds.Key) (int, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.dstore.GetSize(key)
}

// Has implements datastore interface.
func (c *dsCache) Has(key ds.Key) (bool, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.dstore.Has(key)
}

// Sync implements datastore interface.
func (c *dsCache) Sync(key ds.Key) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.dstore.Sync(key)
}

// Close implements datastore interface.
func (c *dsCache) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.dstore == nil {
		return nil
	}

	if err := c.dstore.Close(); err != nil {
		return err
	}
	c.dstore = nil
	return nil
}

// Query implements datastore interface.
func (c *dsCache) Query(q dsq.Query) (dsq.Results, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.dstore.Query(q)
}

// Cap returns the cache capacity.  Storing more than this number of items
// results in discarding oldest items.
func (c *dsCache) Cap() int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.lru.MaxEntries
}

// Len returns the number of items in the cache.
func (c *dsCache) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.lru.Len()
}

// Resize changes the cache capacity. If the capacity is decreased below the
// number of items in the cache, then oldest items are discarded until the
// cache is filled to the new lower capacity.  Returns the number of items
// evicted from cache.
func (c *dsCache) Resize(newSize int) int {
	c.lock.Lock()
	defer c.lock.Unlock()

	diff := c.lru.Len() - newSize
	if diff < 0 {
		diff = 0
	}
	for i := 0; i < diff; i++ {
		c.lru.RemoveOldest()
	}
	c.lru.MaxEntries = newSize
	return diff
}

// Clear purges all stored items from the cache.
func (c *dsCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.lru.Clear()
}

func (c *dsCache) loadKeys(ctx context.Context) error {
	q := dsq.Query{
		KeysOnly: true,
	}

	results, err := c.dstore.Query(q)
	if err != nil {
		return err
	}
	defer results.Close()

	origCap := c.lru.MaxEntries
	c.lru.MaxEntries = 0

	for r := range results.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.Error != nil {
			return fmt.Errorf("cannot read cache key: %s", r.Error)
		}

		c.lru.Add(ds.RawKey(r.Entry.Key), nil)
	}

	// If the cache was resized to expand beyond its original capacity, then
	// set its size to only as big as the number of keys read from datastore.
	// This will be the number of links in the largest list.
	if c.lru.Len() > origCap {
		c.lru.MaxEntries = c.lru.Len()
	} else {
		c.lru.MaxEntries = origCap
	}
	return nil
}
