package provider

import (
	"context"
	"fmt"
	"io"
	"sort"

	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
)

var _ MultihashIterator = (*indexMhIterator)(nil)

type indexMhIterator struct {
	steps []iteratorStep
	err   error

	curStep    int
	lastOffset uint64
}

type iteratorStep struct {
	mh     multihash.Multihash
	offset uint64
}

// CarMultihashIterator constructs a new MultihashIterator from a CAR index.
//
// A background goroutine is started to supply multihashes via Next method calls.
// Its lifecycle is controlled by the given context.
// If the context is canceled before Next reaches io.EOF,
// then the following Next call will return the context's error.
func CarMultihashIterator(ctx context.Context, idx carindex.IterableIndex) MultihashIterator {
	// TODO(mvdan): if we stick with this "upfront range and sort" approach,
	// we probably need to rethink this API a bit.
	// At the very least, to remove the context.

	var steps []iteratorStep
	err := idx.ForEach(func(mh multihash.Multihash, offset uint64) error {
		steps = append(steps, iteratorStep{mh, offset})
		return nil
	})
	sort.Slice(steps, func(i, j int) bool {
		return steps[i].offset < steps[j].offset
	})
	return &indexMhIterator{steps: steps, err: err}
}

func (i *indexMhIterator) Next() (multihash.Multihash, error) {
	// We have reached the end of stream.
	// There might also be an error available, which we should return.
	if i.curStep >= len(i.steps) {
		// Check if there is a error first, since returning error must take precedence.
		if i.err != nil {
			return nil, i.err
		}
		return nil, io.EOF
	}
	step := i.steps[i.curStep]
	i.curStep++
	if step.offset < i.lastOffset {
		return nil, fmt.Errorf("car multihash iterator out of order: %d then %d", i.lastOffset, step.offset)
	}
	if step.offset == i.lastOffset {
		return nil, fmt.Errorf("car multihash iterator has duplicate offset %d", step.offset)
	}
	i.lastOffset = step.offset
	return step.mh, nil
}
