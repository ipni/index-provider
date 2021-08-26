package suppliers

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
)

type carCidIterator struct {
	closer   io.Closer
	cancel   context.CancelFunc
	cidsChan <-chan cid.Cid
	errChan  <-chan error
}

func newCarCidIterator(path string, opts ...car.ReadOption) (*carCidIterator, error) {
	robs, err := blockstore.OpenReadOnly(path, opts...)
	if err != nil {
		return nil, err
	}
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	ctx = blockstore.WithAsyncErrorHandler(ctx, func(err error) { errChan <- err })
	cidsChan, err := robs.AllKeysChan(ctx)
	if err != nil {
		cancel()
		_ = robs.Close()
		return nil, err
	}

	return &carCidIterator{
		closer:   robs,
		cancel:   cancel,
		cidsChan: cidsChan,
		errChan:  errChan,
	}, nil
}

func (c *carCidIterator) Next() (cid.Cid, error) {
	select {
	case err := <-c.errChan:
		return cid.Undef, err
	case nextCid, ok := <-c.cidsChan:
		if ok {
			return nextCid, nil
		}
		return cid.Undef, io.EOF
	}
}

func (c *carCidIterator) Close() error {
	c.cancel()
	return c.closer.Close()
}
