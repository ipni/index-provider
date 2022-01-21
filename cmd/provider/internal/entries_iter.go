package internal

import (
	"context"
	provider "github.com/filecoin-project/index-provider"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"io"
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

func (d *EntriesIterator) Next() (multihash.Multihash, error) {
	if d.chunkIter != nil && d.chunkIter.hasNext() {
		return d.chunkIter.Next()
	}

	if d.next == cid.Undef {
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

func (d *EntriesIterator) Drain() ([]multihash.Multihash, int, error) {
	var mhs []multihash.Multihash
	for {
		mh, err := d.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, d.chunkCount, err
		}
		mhs = append(mhs, mh)
	}
	return mhs, d.chunkCount, nil
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
