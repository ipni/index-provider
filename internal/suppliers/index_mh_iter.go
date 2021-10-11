package suppliers

import (
	"context"
	"io"

	provider "github.com/filecoin-project/indexer-reference-provider"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
)

var _ provider.MultihashIterator = (*indexMhIterator)(nil)

type indexMhIterator struct {
	mhch chan multihash.Multihash
	err  error
}

func newIndexMhIterator(ctx context.Context, idx index.IterableIndex) *indexMhIterator {
	mhIterator := indexMhIterator{
		mhch: make(chan multihash.Multihash, 1),
	}
	go func() {
		if err := idx.ForEach(func(mh multihash.Multihash, _ uint64) error {
			select {
			case mhIterator.mhch <- mh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}); err != nil {
			mhIterator.err = err
		}
		close(mhIterator.mhch)
	}()
	return &mhIterator
}

func (i *indexMhIterator) Next() (multihash.Multihash, error) {
	mh, ok := <-i.mhch
	// If channel is closed we have reached the end of stream.
	// There might also be an error available, which we should return.
	if !ok {
		// Check if there is a error first, since returning error must take precedence.
		if i.err != nil {
			return nil, i.err
		}
		return nil, io.EOF
	}
	return mh, nil
}
