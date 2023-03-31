package chunker

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/ingest/schema"
	provider "github.com/ipni/index-provider"
	"github.com/multiformats/go-multihash"
)

var _ EntriesChunker = (*ChainChunker)(nil)

// ChainChunker chunks advertisement entries as a chained series of schema.EntryChunk nodes.
// See: NewChainChunker
type ChainChunker struct {
	ls        *ipld.LinkSystem
	chunkSize int
}

// NewChainChunker instantiates a new chain chunker that given a provider.MultihashIterator it drains
// all its mulithashes and stores them in the given link system represented as a chain of
// schema.EntryChunk nodes where each chunk contains no more than chunkSize number of multihashes.
//
// See: schema.EntryChunk.
func NewChainChunker(ls *ipld.LinkSystem, chunkSize int) (*ChainChunker, error) {
	if chunkSize < 1 {
		return nil, fmt.Errorf("chunk size must be at least 1; got: %d", chunkSize)
	}
	return &ChainChunker{
		ls:        ls,
		chunkSize: chunkSize,
	}, nil
}

func NewChainChunkerFunc(chunkSize int) NewChunkerFunc {
	return func(ls *ipld.LinkSystem) (EntriesChunker, error) {
		return NewChainChunker(ls, chunkSize)
	}
}

// Chunk chunks all the mulithashes returned by the given iterator into a chain of schema.EntryChunk
// nodes where each chunk contains no more than chunkSize number of multihashes and returns the link
// the root chunk node.
//
// See: schema.EntryChunk.
func (ls *ChainChunker) Chunk(ctx context.Context, mhi provider.MultihashIterator) (ipld.Link, error) {
	mhs := make([]multihash.Multihash, 0, ls.chunkSize)
	var next ipld.Link
	var mhCount, chunkCount int
	for {
		mh, err := mhi.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		mhs = append(mhs, mh)
		mhCount++
		if len(mhs) >= ls.chunkSize {
			cNode, err := newEntriesChunkNode(mhs, next)
			if err != nil {
				return nil, err
			}
			next, err = ls.ls.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, cNode)
			if err != nil {
				return nil, err
			}
			chunkCount++
			// NewLinkedListOfMhs makes it own copy, so safe to reuse mhs
			mhs = mhs[:0]
		}
	}
	if len(mhs) != 0 {
		cNode, err := newEntriesChunkNode(mhs, next)
		if err != nil {
			return nil, err
		}
		next, err = ls.ls.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, cNode)
		if err != nil {
			return nil, err
		}
		chunkCount++
	}

	log.Infow("Generated linked chunks of multihashes", "totalMhCount", mhCount, "chunkCount", chunkCount)
	return next, nil
}

func newEntriesChunkNode(mhs []multihash.Multihash, next ipld.Link) (ipld.Node, error) {
	chunk := schema.EntryChunk{
		Entries: mhs,
	}
	if next != nil {
		chunk.Next = next
	}
	return chunk.ToNode()
}
