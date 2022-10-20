package reframe

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sort"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

const (
	chunkByContextIdIndexPrefix   = "ccid/"
	timestampByCidIndexPrefix     = "tc/"
	timestampsSnapshotIndexPrefix = "ts"
)

// dsWrapper encapsulates all functionality related top the datastore
type dsWrapper struct {
	ds                   datastore.Datastore
	snapshotChunkMaxSize int
}

func newDSWrapper(ds datastore.Datastore, snapshotChunkMaxSize int) *dsWrapper {
	return &dsWrapper{ds: ds, snapshotChunkMaxSize: snapshotChunkMaxSize}
}

// initialiseFromTheDatastore initialises in-memory data structures on first start
func (dsw *dsWrapper) initialiseFromTheDatastore(ctx context.Context, cidImporter func(n *cidNode), chunkImporter func(c *cidsChunk)) error {
	start := time.Now()
	// reading timestamps snapshot from the datastore
	cidNodes, err := dsw.readSnapshotFromDs(ctx)
	if err != nil {
		return fmt.Errorf("error reading timestamp snapshot from the datastore: %w", err)
	}

	// reading timestamp by cid index from the datastore, sorting the slice by the timestamp and providing it into in memory indexes
	q := dsq.Query{Prefix: timestampByCidIndexPrefix}
	tcResults, err := dsw.ds.Query(ctx, q)
	if err != nil {
		return fmt.Errorf("error reading timestamp by cid index from the datastore: %w", err)
	}
	defer tcResults.Close()
	for r := range tcResults.Next() {
		if r.Error != nil {
			return fmt.Errorf("error fetching datastore record: %w", r.Error)
		}

		timestamp := bytesToInt64(r.Value)
		cs := r.Key[len(timestampByCidIndexPrefix)+1:]
		c, err := cid.Parse(cs)
		if err != nil {
			return fmt.Errorf("error parsing cid datastore record: %w", err)
		}

		cidNodes = append(cidNodes, &cidNode{Timestamp: time.UnixMilli(timestamp), C: c})
	}

	sort.SliceStable(cidNodes, func(i, j int) bool {
		return cidNodes[i].Timestamp.Before(cidNodes[j].Timestamp)
	})

	for i := range cidNodes {
		n := cidNodes[i]
		cidImporter(n)
	}

	log.Infof("Loaded up all CIDs from the datastore in %v", time.Since(start))

	start = time.Now()
	// reading all cid chunks from the datastore and adding them up to the in-memory indexes
	q = dsq.Query{Prefix: chunkByContextIdIndexPrefix}
	ccResults, err := dsw.ds.Query(ctx, q)
	if err != nil {
		return fmt.Errorf("error reading from the datastore: %w", err)
	}
	defer ccResults.Close()

	for r := range ccResults.Next() {
		if r.Error != nil {
			return fmt.Errorf("error fetching datastore record: %w", r.Error)
		}

		chunk, err := deserialiseChunk(r.Value)
		if err != nil {
			return fmt.Errorf("error deserialising record from the datastore: %w", err)
		}
		// not importing removed chunks. They can be lazy loaded when needed.
		if chunk.Removed {
			continue
		}
		chunkImporter(chunk)
	}

	log.Infof("Loaded up all chunks from the datastore in %v", time.Since(start))

	return nil
}

func (dsw *dsWrapper) readSnapshotFromDs(ctx context.Context) ([]*cidNode, error) {
	cidNodes := make([]*cidNode, 0)
	keys, err := dsw.getSnapshotChunkKeys(ctx)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		snapshotChunk, err := dsw.ds.Get(ctx, datastore.NewKey(k))
		if err != nil && err != datastore.ErrNotFound {
			return nil, fmt.Errorf("error reading timestamps snapshot from the datastore: %w", err)
		}

		if len(snapshotChunk) == 0 {
			continue
		}

		var chunkNodes []*cidNode
		chunkNodes, err = parseSnapshot(snapshotChunk)
		if err != nil {
			return nil, fmt.Errorf("error parsing timestamps snapshot: %w", err)
		}
		cidNodes = append(cidNodes, chunkNodes...)
	}

	return cidNodes, nil
}

func (dsw *dsWrapper) getSnapshotChunkKeys(ctx context.Context) ([]string, error) {
	q := dsq.Query{Prefix: timestampsSnapshotIndexPrefix, KeysOnly: true}
	tcResults, err := dsw.ds.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("error reading timestamp snapshot keys from the datastore: %w", err)
	}
	defer tcResults.Close()
	keys, err := tcResults.Rest()
	if err != nil {
		return nil, fmt.Errorf("error reading timestamp snapshot keys from the datastore: %w", err)
	}

	keysStr := make([]string, len(keys))
	seenLegacyKey := false
	for i, k := range keys {
		keysStr[i] = k.Key
		seenLegacyKey = seenLegacyKey || k.Key == timestampsSnapshotIndexPrefix
	}

	// explicitly adding a legacy key if it exists in the datastore
	legacySnapshotExists, err := dsw.ds.Has(ctx, datastore.NewKey(timestampsSnapshotIndexPrefix))
	if err != nil {
		return nil, fmt.Errorf("error reading timestamp snapshot keys from the datastore: %w", err)
	}
	if legacySnapshotExists && !seenLegacyKey {
		keysStr = append(keysStr, timestampsSnapshotIndexPrefix)
	}

	return keysStr, nil
}

func (dsw *dsWrapper) recordTimestampsSnapshot(ctx context.Context, timestamps []*cidNode, cleanUpTimestamps bool) error {
	// get the existing snapshot chunks to clean up afterwards
	keys, err := dsw.getSnapshotChunkKeys(ctx)
	if err != nil {
		return err
	}
	keysMap := make(map[string]struct{})
	for _, k := range keys {
		keysMap[k] = struct{}{}
	}

	// split the snapshot into chunks and store in ds
	cnt := 0
	for startPos := 0; startPos < len(timestamps); startPos += dsw.snapshotChunkMaxSize {
		b := bytes.Buffer{}
		e := gob.NewEncoder(&b)
		endPos := min(startPos+dsw.snapshotChunkMaxSize, len(timestamps))
		err := e.Encode(timestamps[startPos:endPos])
		if err != nil {
			return err
		}

		key := datastore.NewKey(fmt.Sprintf("%s/%d", timestampsSnapshotIndexPrefix, cnt))
		delete(keysMap, key.String())
		err = dsw.ds.Put(ctx, key, b.Bytes())
		if err != nil {
			return err
		}
		cnt++
	}

	// delete old snapshot chunks
	for k := range keysMap {
		err = dsw.ds.Delete(ctx, datastore.NewKey(k))
		if err != nil {
			return fmt.Errorf("error cleaning up snapshot chunks from the datastore: %w", err)
		}
	}

	if !cleanUpTimestamps {
		return nil
	}

	q := dsq.Query{Prefix: timestampByCidIndexPrefix, KeysOnly: true}
	tcResults, err := dsw.ds.Query(ctx, q)
	if err != nil {
		return fmt.Errorf("error reading timestamp by cid index from the datastore: %w", err)
	}
	defer tcResults.Close()
	for r := range tcResults.Next() {
		err = dsw.ds.Delete(ctx, datastore.NewKey(r.Key))
		if err != nil {
			log.Warnf("Error cleaning up timestamp by cid index from datastore: %w. Continuing.", err)
		}
	}
	return nil
}

func (dsw *dsWrapper) recordCidTimestamp(ctx context.Context, c cid.Cid, t time.Time) error {
	return dsw.ds.Put(ctx, timestampByCidKey(c), int64ToBytes(t.UnixMilli()))
}

func (dsw *dsWrapper) deleteCidTimestamp(ctx context.Context, c cid.Cid) error {
	return dsw.ds.Delete(ctx, timestampByCidKey(c))
}

func (dsw *dsWrapper) getCidTimestamp(ctx context.Context, c cid.Cid) (time.Time, error) {
	timeBytes, err := dsw.ds.Get(ctx, timestampByCidKey(c))
	if err != nil {
		return time.Now(), err
	}
	return time.UnixMilli(bytesToInt64(timeBytes)), nil
}

func (dsw *dsWrapper) recordChunkByContextID(ctx context.Context, chunk *cidsChunk) error {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(chunk)
	if err != nil {
		return err
	}
	return dsw.ds.Put(ctx, chunkByContextIDKey(chunk.ContextID), b.Bytes())
}

func (dsw *dsWrapper) getChunkByContextID(ctx context.Context, contextID []byte) (*cidsChunk, error) {
	chunkBytes, err := dsw.ds.Get(ctx, chunkByContextIDKey(contextID))
	if err != nil {
		return nil, err
	}
	return deserialiseChunk(chunkBytes)
}

func deserialiseChunk(chunkBytes []byte) (*cidsChunk, error) {
	chunk := &cidsChunk{Cids: make(map[cid.Cid]struct{})}
	decoder := gob.NewDecoder(bytes.NewBuffer(chunkBytes))
	err := decoder.Decode(chunk)
	if err != nil {
		return nil, err
	}
	return chunk, nil
}

func timestampByCidKey(c cid.Cid) datastore.Key {
	return datastore.NewKey(timestampByCidIndexPrefix + c.String())
}

func chunkByContextIDKey(contextID []byte) datastore.Key {
	return datastore.NewKey(chunkByContextIdIndexPrefix + contextIDToStr(contextID))
}

func int64ToBytes(i int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func bytesToInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

func parseSnapshot(snapshot []byte) ([]*cidNode, error) {
	timestamps := make([]*cidNode, 0)
	decoder := gob.NewDecoder(bytes.NewBuffer(snapshot))
	err := decoder.Decode(&timestamps)
	if err != nil {
		return nil, err
	}
	return timestamps, nil
}

func min(l, r int) int {
	if l < r {
		return l
	}
	return r
}
