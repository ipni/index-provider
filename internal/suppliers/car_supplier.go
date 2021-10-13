package suppliers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"path/filepath"

	"github.com/filecoin-project/indexer-reference-provider"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
)

const (
	carSupplierDatastorePrefix = "car_supplier://"
	carPathDatastoreKeyPrefix  = carSupplierDatastorePrefix + "car_path/"
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

// Put makes the CAR at given path suppliable by this supplier. The return CID
// can then be used via Supply to get an iterator over CIDs that belong to the
// CAR. The ID is generated based on the content of the CAR.  When the CAR ID
// is already known, PutWithID should be used instead.
//
// This function accepts both CARv1 and CARv2 formats.
func (cs *CarSupplier) Put(ctx context.Context, path string, metadata stiapi.Metadata) (provider.LookupKey, cid.Cid, error) {
	// Clean path to CAR.
	path = filepath.Clean(path)

	// Generate a CID for the CAR at given path.
	id, err := generateLookupKey(path)
	if err != nil {
		return nil, cid.Undef, err
	}

	c, err := cs.PutWithID(ctx, id, path, metadata)
	if err != nil {
		return nil, cid.Undef, err
	}
	return id, c, nil
}

// PutWithID makes the CAR at the given path, and identified by the given ID,
// suppliable by this supplier. The return CID can then be used via Supply to
// get an iterator over CIDs that belong to the CAR. When the CAR ID is not
// known, Put should be used instead.
//
// This function accepts both CARv1 and CARv2 formats.
func (cs *CarSupplier) PutWithID(ctx context.Context, key provider.LookupKey, path string, metadata stiapi.Metadata) (cid.Cid, error) {
	// Clean path to CAR.
	path = filepath.Clean(path)

	// Store mapping of CAR ID to path, used to instantiate CID iterator.
	carIdKey := toCarIdKey(key)
	err := cs.ds.Put(carIdKey, []byte(path))
	if err != nil {
		return cid.Undef, err
	}

	// Store mapping of path to CAR ID, used to lookup the CAR by path when it is removed.
	if err = cs.ds.Put(toPathKey(path), key); err != nil {
		return cid.Undef, err
	}

	return cs.eng.NotifyPut(ctx, key, metadata)
}

func toCarIdKey(key provider.LookupKey) datastore.Key {
	return datastore.NewKey(carIdDatastoreKeyPrefix + string(key))
}

// Remove removes the CAR at the given path from the list of suppliable CID
// iterators. If the CAR at given path is not known, this function will return
// an error.  This function accepts both CARv1 and CARv2 formats.
func (cs *CarSupplier) Remove(ctx context.Context, path string, metadata stiapi.Metadata) (cid.Cid, error) {
	// Clean path.
	path = filepath.Clean(path)

	// Find the CAR ID that corresponds to the given path
	pathKey := toPathKey(path)
	key, err := cs.getLookupKeyFromPathKey(pathKey)
	if err != nil {
		if err == datastore.ErrNotFound {
			err = ErrNotFound
		}
		return cid.Undef, err
	}

	// Delete mapping of CAR ID to path.
	carIdKey := toCarIdKey(key)
	if err := cs.ds.Delete(carIdKey); err != nil {
		// TODO improve error handling logic
		// we shouldn't typically get NotFound error here.
		// If we do then a put must have failed prematurely
		// See what we can do to opportunistically heal the datastore.
		return cid.Undef, err
	}

	// Delete mapping of path to CAR ID.
	if err := cs.ds.Delete(pathKey); err != nil {
		// TODO improve error handling logic
		// we shouldn't typically get NotFound error here.
		// If we do then a put must have failed prematurely
		// See what we can do to opportunistically heal the datastore.
		return cid.Undef, err
	}
	return cs.eng.NotifyRemove(ctx, key)
}

// Callback supplies an iterator over CIDs of the CAR file that corresponds to
// the given key.  An error is returned if no CAR file is found for the key.
func (cs *CarSupplier) Callback(ctx context.Context, key provider.LookupKey) (provider.MultihashIterator, error) {
	idx, err := cs.lookupIterableIndex(key)
	if err != nil {
		return nil, err
	}
	return newIndexMhIterator(ctx, idx), nil
}

func (cs *CarSupplier) lookupIterableIndex(key provider.LookupKey) (index.IterableIndex, error) {
	b, err := cs.ds.Get(toCarIdKey(key))
	if err != nil {
		if err == datastore.ErrNotFound {
			err = ErrNotFound
		}
		return nil, err
	}
	path := string(b)

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

func (cs *CarSupplier) getLookupKeyFromPathKey(pathKey datastore.Key) (provider.LookupKey, error) {
	return cs.ds.Get(pathKey)
}

func generateLookupKey(path string) (provider.LookupKey, error) {
	// Simply hash the path given as the lookup key.
	return sha256.New().Sum([]byte(path)), nil
}

func toPathKey(path string) datastore.Key {
	// Hash the path to get a fixed length string as key, regardless of how long the path is.
	pathHash := sha256.New().Sum([]byte(path))
	encPathHash := base64.StdEncoding.EncodeToString(pathHash)

	// Prefix the hash with some constant for namespacing and debug readability.
	return datastore.NewKey(carPathDatastoreKeyPrefix + encPathHash)
}
