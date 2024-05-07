package supplier

import (
	"context"
	"errors"
	"io"
	"path/filepath"

	bstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

const (
	carSupplierDatastorePrefix = "car_supplier://"
	carIdDatastoreKeyPrefix    = carSupplierDatastorePrefix + "car_id/"
)

// ErrNotFound signals that CidIteratorSupplier has no iterator corresponding to the given key.
var ErrNotFound = errors.New("no CID iterator found for given key")
var log = logging.Logger("provider/carsupplier")

// CarSupplier supplies multihashes to an implementation of Provider.Interface via
// provider.MultihashLister. It allows the users to advertise addition and removal of multihashes
// within CAR files by simply calling CarSupplier.Put and CarSupplier.Remove.
//
// CarSupplier accepts both CARv1 and CARv2, and will automatically generate an index if one is not
// present or the index codec and characteristics are not sufficient for provider.Interface purposes.
//
// See: engine.New, CarSupplier.Put, CarSupplier.Remove.
type CarSupplier struct {
	eng  provider.Interface
	ds   datastore.Datastore
	opts []car.ReadOption
}

// NewCarSupplier instantiates a new CarSupplier and registers it as the provider.MultihashLister of the
// given provider.Interface.
func NewCarSupplier(eng provider.Interface, ds datastore.Datastore, opts ...car.ReadOption) *CarSupplier {
	cs := &CarSupplier{
		eng:  eng,
		ds:   ds,
		opts: opts,
	}
	eng.RegisterMultihashLister(cs.ListMultihashes)
	return cs
}

// Put makes the CAR at the given path, and identified by the given ID,
// suppliable by this supplier. The return CID can then be used via Supply to
// get an iterator over CIDs that belong to the CAR.
//
// This function accepts both CARv1 and CARv2 formats.
func (cs *CarSupplier) Put(ctx context.Context, contextID []byte, path string, metadata metadata.Metadata) (cid.Cid, error) {
	// Clean path to CAR.
	path = filepath.Clean(path)

	// Store mapping of CAR ID to path, used to instantiate CID iterator.
	carIdKey := toCarIdKey(contextID)
	err := cs.ds.Put(ctx, carIdKey, []byte(path))
	if err != nil {
		return cid.Undef, err
	}

	return cs.eng.NotifyPut(ctx, nil, contextID, metadata)
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
	has, err := cs.ds.Has(ctx, carIdKey)
	if err != nil {
		return cid.Undef, err
	}
	if !has {
		return cid.Undef, ErrNotFound
	}
	if err := cs.ds.Delete(ctx, carIdKey); err != nil {
		// TODO improve error handling logic
		// we shouldn't typically get NotFound error here.
		// If we do then a put must have failed prematurely
		// See what we can do to opportunistically heal the datastore.
		return cid.Undef, err
	}

	return cs.eng.NotifyRemove(ctx, "", contextID)
}

// List lists the CAR paths that are supplied by this supplier.
//
// See: CarSupplier.Put
func (cs *CarSupplier) List(ctx context.Context) ([]string, error) {
	q := query.Query{
		Prefix: carIdDatastoreKeyPrefix,
	}
	results, err := cs.ds.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer results.Close()

	var paths []string
	for r := range results.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		paths = append(paths, string(r.Value))
	}
	return paths, nil
}

// ListMultihashes supplies an iterator over CIDs of the CAR file that corresponds to
// the given key.  An error is returned if no CAR file is found for the key.
func (cs *CarSupplier) ListMultihashes(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
	idx, err := cs.lookupIterableIndex(ctx, contextID)
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
	path, err := cs.getPath(context.TODO(), contextID)
	if err != nil {
		return nil, err
	}
	return blockstore.OpenReadOnly(path, cs.opts...)
}

func (cs *CarSupplier) getPath(ctx context.Context, contextID []byte) (path string, err error) {
	b, err := cs.ds.Get(ctx, toCarIdKey(contextID))
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			err = ErrNotFound
		}
		return "", err
	}
	return string(b), nil
}

func (cs *CarSupplier) lookupIterableIndex(ctx context.Context, contextID []byte) (index.IterableIndex, error) {
	log := log.With("contextID", contextID)

	path, err := cs.getPath(ctx, contextID)
	if err != nil {
		return nil, err
	}

	cr, err := car.OpenReader(path, cs.opts...)
	if err != nil {
		return nil, err
	}
	idxReader, err := cr.IndexReader()
	if err != nil {
		return nil, err
	}
	if idxReader == nil {
		// Missing index; generate it.
		log.Debugw("CAR has no index; generating.")
		return cs.generateIterableIndex(cr)
	}
	idx, err := index.ReadFrom(idxReader)
	if err != nil {
		return nil, err
	}
	codec := idx.Codec()
	log = log.With("codec", codec)
	if codec != multicodec.CarMultihashIndexSorted {
		log.Debugw("CAR index not iterable; regenerating index.")
		return cs.generateIterableIndex(cr)
	}
	itIdx, ok := idx.(index.IterableIndex)
	if !ok {
		// Though technically possible, this should not happen, since the expectation is that
		// multicodec.CarMultihashIndexSorted implements the index.IterableIndex interface.
		// Regardless, defensively check this and re-generate as needed in case go-car library
		// changes this expectation.
		log.Warnw("expected CAR index to implement index.IterableIndex interface; regenerating index.")
		return cs.generateIterableIndex(cr)
	}
	return itIdx, nil
}

func (cs *CarSupplier) generateIterableIndex(cr *car.Reader) (index.IterableIndex, error) {
	idx := index.NewMultihashSorted()
	dr, err := cr.DataReader()
	if err != nil {
		return nil, err
	}
	if err := car.LoadIndex(idx, dr, cs.opts...); err != nil {
		return nil, err
	}
	return idx, nil
}

// Close permanently closes this supplier.
// After calling Close this supplier is no longer usable.
func (cs *CarSupplier) Close() error {
	return cs.ds.Close()
}
