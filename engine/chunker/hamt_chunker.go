package chunker

import (
	"context"
	"errors"
	"fmt"
	"io"

	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/ingest/schema"
	provider "github.com/ipni/index-provider"
	"github.com/multiformats/go-multicodec"
)

var _ EntriesChunker = (*HamtChunker)(nil)

// HamtChunker chunks advertisement entries as an IPLD HAMT data structure.
// See: NewHamtChunker.
type HamtChunker struct {
	ls         *ipld.LinkSystem
	hashAlg    multicodec.Code
	bitWidth   int
	bucketSize int
}

// NewHamtChunker instantiates a new HAMT chunker that given a provider.MultihashIterator it drains
// all its mulithashes and stores them in the given link system represented as an IPLD HAMT ADL.
//
// Only multicodec.Identity, multicodec.Sha2_256 and multicodec.Murmur3X64_64 are supported as hash
// algorithm. The bit-width and bucket size must be at least 3 and 1 respectively.
//
// See:
//   - https://ipld.io/specs/advanced-data-layouts/hamt/spec
//   - https://github.com/ipld/go-ipld-adl-hamt
func NewHamtChunker(ls *ipld.LinkSystem, hashAlg multicodec.Code, bitWidth, bucketSize int) (*HamtChunker, error) {
	if bitWidth < 3 {
		return nil, fmt.Errorf("bit-width must be at least 3; got: %d", bitWidth)
	}
	if bucketSize < 1 {
		return nil, fmt.Errorf("bucket size must be at least 1; got: %d", bucketSize)
	}
	switch hashAlg {
	case multicodec.Identity, multicodec.Sha2_256, multicodec.Murmur3X64_64:
	default:
		return nil, fmt.Errorf("only %s, %s, and %s hash algorithms are supported; got: %s",
			multicodec.Identity, multicodec.Sha2_256, multicodec.Murmur3X64_64, hashAlg,
		)
	}
	return &HamtChunker{
		ls:         ls,
		hashAlg:    hashAlg,
		bitWidth:   bitWidth,
		bucketSize: bucketSize,
	}, nil
}

func NewHamtChunkerFunc(hashAlg multicodec.Code, bitWidth, bucketSize int) NewChunkerFunc {
	return func(ls *ipld.LinkSystem) (EntriesChunker, error) {
		return NewHamtChunker(ls, hashAlg, bitWidth, bucketSize)
	}
}

// Chunk drains all the multihashes in the given iterator, stores them as an IPLD HAMT ADL and
// returns the link to the root HAMT node.
//
// The HAMT is used as a set where the keys in the map represent the multihashes and values are
// simply set to true.
func (h *HamtChunker) Chunk(ctx context.Context, iterator provider.MultihashIterator) (ipld.Link, error) {
	builder := hamt.NewBuilder(hamt.Prototype{
		BitWidth:   h.bitWidth,
		BucketSize: h.bucketSize,
	}.WithHashAlg(h.hashAlg)).WithLinking(*h.ls, schema.Linkproto)
	ma, err := builder.BeginMap(0)
	if err != nil {
		return nil, err
	}
	var count int
	for {
		mh, err := iterator.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Iterator had no elements
				if count == 0 {
					return nil, nil
				}
				break
			}
			return nil, err
		}
		count++
		if err := ma.AssembleKey().AssignBytes(mh); err != nil {
			return nil, err
		}
		if err := ma.AssembleValue().AssignBool(true); err != nil {
			return nil, err
		}
	}
	log.Debugw("finished iterating over multihash lister", "mhCount", count)
	if err := ma.Finish(); err != nil {
		return nil, err
	}
	hamtRoot, ok := builder.Build().(ipld.ADL)
	if !ok {
		return nil, errors.New("HAMT must implement ipld.ADL")
	}
	return h.ls.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, hamtRoot.Substrate())
}
