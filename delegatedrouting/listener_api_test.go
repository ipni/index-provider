package delegatedrouting

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
)

func ChunkExists(ctx context.Context, listener *Listener, cids []cid.Cid, nonceGen func() []byte) bool {
	cidsMap := cidsListToMap(cids)
	ctxID := listener.chunker.generateContextID(cidsMap)
	ctxIDStr := contextIDToStr(ctxID)
	chunkFromIndex := listener.chunker.getChunkByContextID(ctxIDStr)
	if chunkFromIndex == nil {
		return false
	}
	cidsRegistered := true
	// verifying that chunk has been assigned to nodes in the expiry queue
	for c := range chunkFromIndex.Cids {
		elem := listener.cidQueue.getNodeByCid(c)
		if elem == nil {
			cidsRegistered = false
			break
		}
		if elem.Value.(*cidNode).chunk != chunkFromIndex {
			cidsRegistered = false
			break
		}
	}
	if !cidsRegistered {
		return false
	}
	chunkFromDatastore, err := listener.dsWrapper.getChunkByContextID(ctx, ctxID)
	if err != nil {
		return false
	}
	if contextIDToStr(chunkFromDatastore.ContextID) != ctxIDStr {
		return false
	}

	return true
}

func HasSnapshot(ctx context.Context, listener *Listener) bool {
	return SnapshotsQty(ctx, listener) > 0
}

func SnapshotsQty(ctx context.Context, listener *Listener) int {
	keys, _ := listener.dsWrapper.getSnapshotChunkKeys(ctx)
	return len(keys)
}

func HasCidTimestamp(ctx context.Context, listener *Listener, c cid.Cid) bool {
	has, err := listener.dsWrapper.ds.Has(ctx, timestampByCidKey(c))
	return has && err == nil
}

func WrappedDatastore(listener *Listener) datastore.Datastore {
	return listener.dsWrapper.ds
}

func ChunkNotExist(ctx context.Context, listener *Listener, cids []cid.Cid, nonceGen func() []byte) bool {
	ctxID := listener.chunker.generateContextID(cidsListToMap(cids))
	ctxIDStr := contextIDToStr(ctxID)
	cidsRegistered := false
	for _, c := range cids {
		elem := listener.cidQueue.getNodeByCid(c)
		if elem == nil || elem.Value.(*cidNode).chunk == nil || !bytes.Equal(elem.Value.(*cidNode).chunk.ContextID, ctxID) {
			continue
		}
		cidsRegistered = true
		break
	}
	if cidsRegistered {
		return false
	}

	_, err := listener.dsWrapper.getChunkByContextID(ctx, ctxID)

	return err == datastore.ErrNotFound && listener.chunker.getChunkByContextID(ctxIDStr) == nil
}

func CidExist(ctx context.Context, listener *Listener, c cid.Cid, requireChunk bool) bool {
	elem := listener.cidQueue.getNodeByCid(c)
	return elem != nil && (!requireChunk || elem.Value.(*cidNode).chunk != nil)
}

func CidNotExist(ctx context.Context, listener *Listener, c cid.Cid) bool {
	return listener.cidQueue.getNodeByCid(c) == nil
}

func GetCidTimestampFromDatastore(ctx context.Context, listener *Listener, c cid.Cid) (time.Time, error) {
	return listener.dsWrapper.getCidTimestamp(ctx, c)
}

func GetCidTimestampFromCache(ctx context.Context, listener *Listener, c cid.Cid) (time.Time, error) {
	node := listener.cidQueue.getNodeByCid(c)
	if node == nil {
		return time.Unix(0, 0), fmt.Errorf("Timestamp not found")
	}
	return node.Value.(*cidNode).Timestamp, nil
}

func GetChunk(ctx context.Context, listener *Listener, contextID string) *cidsChunk {
	return listener.chunker.getChunkByContextID(contextID)
}

func GetCurrentChunk(ctx context.Context, listener *Listener) *cidsChunk {
	return listener.chunker.currentChunk
}

func GetExpiryQueue(ctx context.Context, listener *Listener) []cid.Cid {
	cids := make([]cid.Cid, listener.cidQueue.nodesLl.Len())
	node := listener.cidQueue.nodesLl.Front()
	cnt := 0
	for {
		if node == nil {
			break
		}
		cids[cnt] = node.Value.(*cidNode).C
		cnt++
		node = node.Next()
	}
	return cids
}

func cidsListToMap(cids []cid.Cid) map[cid.Cid]struct{} {
	cidsMap := make(map[cid.Cid]struct{})
	for _, c := range cids {
		cidsMap[c] = struct{}{}
	}
	return cidsMap
}

func StatsReporter() *statsReporter {
	return &statsReporter{}
}
