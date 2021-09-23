package suppliers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"path/filepath"

	"github.com/filecoin-project/indexer-reference-provider/core"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-car/v2"
)

const (
	carSupplierDatastorePrefix = "car_supplier://"
	carPathDatastoreKeyPrefix  = carSupplierDatastorePrefix + "car_path/"
	carIdDatastoreKeyPrefix    = carSupplierDatastorePrefix + "car_id/"
)

var (
	_ CidIteratorSupplier = (*CarSupplier)(nil)
	_ io.Closer           = (*CarSupplier)(nil)
	_ io.Closer           = (*carCidIterator)(nil)
	_ CidIterator         = (*carCidIterator)(nil)
)

type CarSupplier struct {
	eng  core.Interface
	ds   datastore.Datastore
	opts []car.ReadOption
}

func NewCarSupplier(eng core.Interface, ds datastore.Datastore, opts ...car.ReadOption) *CarSupplier {
	cs := &CarSupplier{
		eng:  eng,
		ds:   ds,
		opts: opts,
	}
	eng.RegisterCidCallback(ToCidCallback(cs))
	return cs
}

// Put makes the CAR at given path suppliable by this supplier. The return CID
// can then be used via Supply to get an iterator over CIDs that belong to the
// CAR. The ID is generated based on the content of the CAR.  When the CAR ID
// is already known, PutWithID should be used instead.
//
// This function accepts both CARv1 and CARv2 formats.
func (cs *CarSupplier) Put(ctx context.Context, path string, metadata []byte) (core.LookupKey, cid.Cid, error) {
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
func (cs *CarSupplier) PutWithID(ctx context.Context, key core.LookupKey, path string, metadata []byte) (cid.Cid, error) {
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

func toCarIdKey(key core.LookupKey) datastore.Key {
	return datastore.NewKey(carIdDatastoreKeyPrefix + string(key))
}

// Remove removes the CAR at the given path from the list of suppliable CID
// iterators. If the CAR at given path is not known, this function will return
// an error.  This function accepts both CARv1 and CARv2 formats.
func (cs *CarSupplier) Remove(ctx context.Context, path string, metadata []byte) (cid.Cid, error) {
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
	return cs.eng.NotifyRemove(ctx, key, metadata)
}

// Supply supplies an iterator over CIDs of the CAR file that corresponds to
// the given key.  An error is returned if no CAR file is found for the key.
func (cs *CarSupplier) Supply(key core.LookupKey) (CidIterator, error) {
	b, err := cs.ds.Get(toCarIdKey(key))
	if err != nil {
		if err == datastore.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	path := string(b)
	return newCarCidIterator(path, cs.opts...)
}

// Close permanently closes this supplier.
// After calling Close this supplier is no longer usable.
func (cs *CarSupplier) Close() error {
	return cs.ds.Close()
}

func (cs *CarSupplier) getLookupKeyFromPathKey(pathKey datastore.Key) (core.LookupKey, error) {
	return cs.ds.Get(pathKey)
}

func generateLookupKey(path string) (core.LookupKey, error) {
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
