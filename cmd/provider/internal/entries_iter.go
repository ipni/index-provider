package internal

import (
	"context"
	"errors"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/ingest/schema"
	provider "github.com/ipni/index-provider"
	"github.com/multiformats/go-multihash"
)

var (
	_ provider.MultihashIterator = (*EntriesIterator)(nil)
	_ provider.MultihashIterator = (*sliceMhIterator)(nil)
)

type (
	EntriesIterator struct {
		store      *ProviderClientStore
		ctx        context.Context
		root       cid.Cid
		next       cid.Cid
		chunkIter  *sliceMhIterator
		chunkCount int
	}
	sliceMhIterator struct {
		mhs    []multihash.Multihash
		offset int
	}
)

func (d *EntriesIterator) Root() cid.Cid {
	return d.root
}

func (d *EntriesIterator) IsPresent() bool {
	return isPresent(d.root)
}

func isPresent(c cid.Cid) bool {
	return c != schema.NoEntries.Cid && c != cid.Undef
}

func (d *EntriesIterator) Next() (multihash.Multihash, error) {

	if !d.IsPresent() {
		return nil, io.EOF
	}

	if d.chunkIter != nil && d.chunkIter.hasNext() {
		return d.chunkIter.Next()
	}

	if !isPresent(d.next) {
		return nil, io.EOF
	}

	next, mhs, err := d.store.getEntriesChunk(d.ctx, d.next)
	if err != nil {
		return nil, err
	}
	d.next = next
	d.chunkIter = &sliceMhIterator{mhs: mhs}
	d.chunkCount++
	return d.chunkIter.Next()
}

func (d *EntriesIterator) Drain() ([]multihash.Multihash, error) {
	var mhs []multihash.Multihash
	for {
		mh, err := d.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Return what we have with error.
			// This is used when err is datastore.ErrNotFound when recursion limit stopped the remaining entries to be synced.
			return mhs, err
		}
		mhs = append(mhs, mh)
	}
	return mhs, nil
}

// ChunkCount returns the number of current chunk in iteration.
// This function returns the final count of entries chunk when iteration reaches its end, i.e.
// calling EntriesIterator.Next returns io.EOF error.
func (d *EntriesIterator) ChunkCount() int {
	return d.chunkCount
}

func (s *sliceMhIterator) Next() (multihash.Multihash, error) {
	if s.hasNext() {
		next := s.mhs[s.offset]
		s.offset++
		return next, nil
	}
	return nil, io.EOF
}

func (s *sliceMhIterator) hasNext() bool {
	return s.offset < len(s.mhs)
}
