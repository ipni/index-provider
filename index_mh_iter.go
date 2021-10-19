package provider

import (
	"fmt"
	"io"
	"sort"

	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
)

var _ MultihashIterator = (*indexMhIterator)(nil)

type indexMhIterator struct {
	steps []iteratorStep

	curStep    int
	lastOffset uint64
}

type iteratorStep struct {
	mh     multihash.Multihash
	offset uint64
}

// CarMultihashIterator constructs a new MultihashIterator from a CAR index.
//
// This iterator supplies multihashes in deterministic order of their corresponding CAR offset.
// The order is maintained consistently regardless of the underlying IterableIndex implementation.
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
	return &indexMhIterator{steps: steps}, nil
}

func (i *indexMhIterator) Next() (multihash.Multihash, error) {
	// We have reached the end of stream.
	if i.curStep >= len(i.steps) {
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
