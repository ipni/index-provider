package engine

import (
	"context"
	"fmt"

	"github.com/hashicorp/golang-lru"
	ds "github.com/ipfs/go-datastore"
	dsn "github.com/ipfs/go-datastore/namespace"
	dsq "github.com/ipfs/go-datastore/query"
)

const linksCachePath = "/cache/links"

// dsCache is a LRU-cache that stores keys in memory and values in a datastore.
type dsCache struct {
	capacity int
	dstore   ds.Datastore
	lru      *lru.Cache
}

// newDsCache creates a new dsCache instance.  The context is only used cancel
// a call to this function while it is accessing the data store.
func newDsCache(ctx context.Context, dstore ds.Datastore, capacity int, purge bool) (*dsCache, error) {
	dstore = dsn.Wrap(dstore, ds.NewKey(linksCachePath))

	// Create LRU cache that deletes value from datastore when key is eviceted
	// from cache.
	cache, err := lru.NewWithEvict(capacity, func(key, val interface{}) {
		// Remove item from datastore that was evicted from LRU.
		dstore.Delete(key.(ds.Key))
	})
	if err != nil {
		return nil, err
	}

	c := &dsCache{
		capacity: capacity,
		dstore:   dstore,
		lru:      cache,
	}

	if purge {
		// Remove all keys from datastore and start with empty cache.
		if err = c.purgeKeys(ctx); err != nil {
			return nil, err
		}
	} else {
		// Load all keys from datastore into lru cache.
		if err = c.loadKeys(ctx); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Get implements datastore interface.
func (c *dsCache) Get(key ds.Key) ([]byte, error) {
	_, ok := c.lru.Get(key)
	if !ok {
		return nil, ds.ErrNotFound
	}
	val, err := c.dstore.Get(key)
	if err != nil {
		if err == ds.ErrNotFound {
			c.lru.Remove(key)
		}
		return nil, err
	}
	return val, nil
}

// Put implements datastore interface.
func (c *dsCache) Put(key ds.Key, val []byte) error {
	c.lru.Add(key, nil)
	return c.dstore.Put(key, val)
}

// Delete implements datastore interface.
func (c *dsCache) Delete(key ds.Key) error {
	if c.lru.Remove(key) {
		return c.dstore.Delete(key)
	}
	return nil
}

// GetSize implements datastore interface.
func (c *dsCache) GetSize(key ds.Key) (int, error) {
	return c.dstore.GetSize(key)
}

// Has implements datastore interface.
func (c *dsCache) Has(key ds.Key) (bool, error) {
	return c.lru.Contains(key), nil
}

// Sync implements datastore interface.
func (c *dsCache) Sync(key ds.Key) error {
	return c.dstore.Sync(key)
}

// Close implements datastore interface.
func (c *dsCache) Close() error {
	return c.dstore.Close()
}

// Query implements datastore interface.
func (c *dsCache) Query(q dsq.Query) (dsq.Results, error) {
	return c.dstore.Query(q)
}

// Cap returns the cache capacity.  Storing more than this number of items
// results in discarding oldest items.
func (c *dsCache) Cap() int {
	return c.capacity
}

// Len returns the number of items in the cache.
func (c *dsCache) Len() int {
	return c.lru.Len()
}

// Resize changes the cache capacity. If the capacity is decreased below the
// number of items in the cache, then oldest items are discarded until the
// cache is filled to the new lower capacity.
func (c *dsCache) Resize(newSize int) {
	c.capacity = newSize
	c.lru.Resize(newSize)
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

	var resized bool
	for r := range results.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.Error != nil {
			return fmt.Errorf("cannot read cache key: %s", r.Error)
		}

		// Grow cache size as needed to hold items.
		if c.lru.Len() == c.capacity {
			c.Resize(c.capacity * 2)
			resized = true
		}

		c.lru.Add(ds.RawKey(r.Entry.Key), nil)
	}
	// If the cache was resized to expand beyond its original capacity, then
	// set its size to only as big as the number of keys read from datastore.
	// This should be the number of links in the largest list.
	if resized {
		c.Resize(c.Len())
	}
	return nil
}

func (c *dsCache) purgeKeys(ctx context.Context) error {
	q := dsq.Query{
		KeysOnly: true,
	}

	results, err := c.dstore.Query(q)
	if err != nil {
		return err
	}
	defer results.Close()

	for r := range results.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.Error != nil {
			return fmt.Errorf("cannot read cache key: %s", r.Error)
		}
		if err = c.dstore.Delete(ds.RawKey(r.Entry.Key)); err != nil {
			return err
		}
	}

	return nil
}
