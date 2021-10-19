package suppliers

import (
	"context"
	"errors"
	"io"
	"path/filepath"

	provider "github.com/filecoin-project/indexer-reference-provider"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
)

const (
	carSupplierDatastorePrefix = "car_supplier://"
	carIdDatastoreKeyPrefix    = carSupplierDatastorePrefix + "car_id/"
)

// ErrNotFound signals that CidIteratorSupplier has no iterator corresponding to the given key.
var ErrNotFound = errors.New("no CID iterator found for given key")

type CarSupplier struct {
	eng  provider.Interface
	ds   datastore.Datastore
	opts []car.ReadOption
}

func NewCarSupplier(eng provider.Interface, ds datastore.Datastore, opts ...car.ReadOption) *CarSupplier {
	// We require a "full" index, including identity CIDs.
	// As such, we require StoreIdentityCIDs to be set.
	// Don't rely on all callers to remember to set it.
	// They can override it if they so wish, but that's unsupported.
	opts = append([]car.ReadOption{car.StoreIdentityCIDs(true)}, opts...)

	cs := &CarSupplier{
		eng:  eng,
		ds:   ds,
		opts: opts,
	}
	eng.RegisterCallback(cs.Callback)
	return cs
}

// Put makes the CAR at the given path, and identified by the given ID,
// suppliable by this supplier. The return CID can then be used via Supply to
// get an iterator over CIDs that belong to the CAR. When the CAR ID is not
// known, Put should be used instead.
//
// This function accepts both CARv1 and CARv2 formats.
func (cs *CarSupplier) Put(ctx context.Context, contextID []byte, path string, metadata stiapi.Metadata) (cid.Cid, error) {
	// Clean path to CAR.
	path = filepath.Clean(path)

	// Store mapping of CAR ID to path, used to instantiate CID iterator.
	carIdKey := toCarIdKey(contextID)
	err := cs.ds.Put(carIdKey, []byte(path))
	if err != nil {
		return cid.Undef, err
	}

	return cs.eng.NotifyPut(ctx, contextID, metadata)
}

func toCarIdKey(contextID []byte) datastore.Key {
	return datastore.NewKey(carIdDatastoreKeyPrefix + string(contextID))
}

// Remove removes the CAR at the given path from the list of suppliable CID
// iterators. If the CAR at given path is not known, this function will return
// an error.  This function accepts both CARv1 and CARv2 formats.
func (cs *CarSupplier) Remove(ctx context.Context, contextID []byte) (cid.Cid, error) {

	// Delete mapping of CAR ID to path.
	carIdKey := toCarIdKey(contextID)
	has, err := cs.ds.Has(carIdKey)
	if err != nil {
		return cid.Undef, err
	}
	if !has {
		return cid.Undef, ErrNotFound
	}
	if err := cs.ds.Delete(carIdKey); err != nil {
		// TODO improve error handling logic
		// we shouldn't typically get NotFound error here.
		// If we do then a put must have failed prematurely
		// See what we can do to opportunistically heal the datastore.
		return cid.Undef, err
	}

	return cs.eng.NotifyRemove(ctx, contextID)
}

// Callback supplies an iterator over CIDs of the CAR file that corresponds to
// the given key.  An error is returned if no CAR file is found for the key.
func (cs *CarSupplier) Callback(_ context.Context, contextID []byte) (provider.MultihashIterator, error) {
	idx, err := cs.lookupIterableIndex(contextID)
	if err != nil {
		return nil, err
	}
	return provider.CarMultihashIterator(idx)
}

// ClosableBlockstore is a blockstore that can be closed
type ClosableBlockstore interface {
	bstore.Blockstore
	io.Closer
}

// ReadOnlyBlockstore returns a CAR blockstore interface for the given blockstore key
func (cs *CarSupplier) ReadOnlyBlockstore(contextID []byte) (ClosableBlockstore, error) {
	path, err := cs.getPath(contextID)
	if err != nil {
		return nil, err
	}
	return blockstore.OpenReadOnly(path, cs.opts...)
}

func (cs *CarSupplier) getPath(contextID []byte) (path string, err error) {
	b, err := cs.ds.Get(toCarIdKey(contextID))
	if err != nil {
		if err == datastore.ErrNotFound {
			err = ErrNotFound
		}
		return "", err
	}
	return string(b), nil
}

func (cs *CarSupplier) lookupIterableIndex(contextID []byte) (index.IterableIndex, error) {
	path, err := cs.getPath(contextID)

	cr, err := car.OpenReader(path, cs.opts...)
	if err != nil {
		return nil, err
	}
	idxReader := cr.IndexReader()
	if err != nil {
		return nil, err
	}
	if idxReader == nil || !cr.Header.Characteristics.IsFullyIndexed() {
		// Missing or non-complete index; generate it.
		return cs.generateIterableIndex(cr)
	}
	idx, err := index.ReadFrom(idxReader)
	if err != nil {
		return nil, err
	}
	if idx.Codec() != multicodec.CarMultihashIndexSorted {
		// Index doesn't contain full multihashes; generate it.
		return cs.generateIterableIndex(cr)
	}
	return idx.(index.IterableIndex), nil
}

func (cs *CarSupplier) generateIterableIndex(cr *car.Reader) (index.IterableIndex, error) {
	idx := index.NewMultihashSorted()
	if err := car.LoadIndex(idx, cr.DataReader(), cs.opts...); err != nil {
		return nil, err
	}
	return idx, nil
}

// Close permanently closes this supplier.
// After calling Close this supplier is no longer usable.
func (cs *CarSupplier) Close() error {
	return cs.ds.Close()
}
