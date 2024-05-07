// Package stores is copy pasted from go-fil-markets stores package -
// there is no novel code here.
package stores

import (
	"errors"
	"fmt"
	"io"
	"sync"

	bstore "github.com/ipfs/boxo/blockstore"
)

var ErrNotFound = errors.New("not found")

func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// ReadOnlyBlockstores tracks open read blockstores.
type ReadOnlyBlockstores struct {
	mu     sync.RWMutex
	stores map[string]bstore.Blockstore
}

func NewReadOnlyBlockstores() *ReadOnlyBlockstores {
	return &ReadOnlyBlockstores{
		stores: make(map[string]bstore.Blockstore),
	}
}

func (r *ReadOnlyBlockstores) Track(key string, bs bstore.Blockstore) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.stores[key]; ok {
		return false
	}

	r.stores[key] = bs
	return true
}

func (r *ReadOnlyBlockstores) Get(key string) (bstore.Blockstore, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if bs, ok := r.stores[key]; ok {
		return bs, nil
	}

	return nil, fmt.Errorf("could not get blockstore for key %s: %w", key, ErrNotFound)
}

func (r *ReadOnlyBlockstores) Untrack(key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if bs, ok := r.stores[key]; ok {
		delete(r.stores, key)
		if closer, ok := bs.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				return fmt.Errorf("failed to close read-only blockstore: %w", err)
			}
		}
	}

	return nil
}
