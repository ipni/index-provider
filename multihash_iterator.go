package provider

import (
	"fmt"
	"io"
	"sort"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	carindex "github.com/ipld/go-car/v2/index"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	"github.com/multiformats/go-multihash"
)

var _ MultihashIterator = (*sliceMhIterator)(nil)

// sliceMhIterator is a simple MultihashIterator implementation that
// iterates a slice of multihash.Multihash.
type sliceMhIterator struct {
	mhs []multihash.Multihash
	pos int
}

type iteratorStep struct {
	mh     multihash.Multihash
	offset uint64
}

// CarMultihashIterator constructs a new MultihashIterator from a CAR index.
//
// This iterator supplies multihashes in deterministic order of their
// corresponding CAR offset. The order is maintained consistently regardless of
// the underlying IterableIndex implementation. Returns error if duplicate
// offsets detected.
func CarMultihashIterator(idx carindex.IterableIndex) (MultihashIterator, error) {
	var steps []iteratorStep
	if err := idx.ForEach(func(mh multihash.Multihash, offset uint64) error {
		steps = append(steps, iteratorStep{mh, offset})
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(steps, func(i, j int) bool {
		return steps[i].offset < steps[j].offset
	})

	var lastOffset uint64
	mhs := make([]multihash.Multihash, len(steps))
	for i := range steps {
		if steps[i].offset == lastOffset {
			return nil, fmt.Errorf("car multihash iterator has duplicate offset %d", steps[i].offset)
		}
		mhs[i] = steps[i].mh
	}
	return &sliceMhIterator{mhs: mhs}, nil
}

// SliceMultihashIterator constructs a new MultihashIterator from a slice of
// multihashes.
func SliceMultihashIterator(mhs []multihash.Multihash) MultihashIterator {
	return &sliceMhIterator{mhs: mhs}
}

// Next implements the MultihashIterator interface.
func (it *sliceMhIterator) Next() (multihash.Multihash, error) {
	if it.pos >= len(it.mhs) {
		return nil, io.EOF
	}
	mh := it.mhs[it.pos]
	it.pos++
	return mh, nil
}

var _ MultihashIterator = (*ipldMapMhIter)(nil)

type ipldMapMhIter struct {
	mi ipld.MapIterator
}

func (i *ipldMapMhIter) Next() (multihash.Multihash, error) {
	if i.mi.Done() {
		return nil, io.EOF
	}
	k, _, err := i.mi.Next()
	if err != nil {
		return nil, err
	}

	// Note the IPLD hamt implementation currently writes map keys as string
	ks, err := k.AsString()
	if err != nil {
		return nil, err
	}
	return []byte(ks), nil
}

func HamtMultihashIterator(root *hamt.HashMapRoot, ls ipld.LinkSystem) MultihashIterator {
	n := hamt.Node{
		HashMapRoot: *root,
	}.WithLinking(ls, schema.Linkproto)
	return &ipldMapMhIter{n.MapIterator()}
}
