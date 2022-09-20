package reframe

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
)

func ChunkExists(ctx context.Context, listener *ReframeListener, cids []cid.Cid, nonceGen func() []byte) bool {
	cidsMap := cidsListToMap(cids)
	ctxID := listener.chunker.generateContextID(cidsMap)
	ctxIDStr := contextIDToStr(ctxID)
	chunkFromIndex := listener.chunker.getChunkByContextID(ctxIDStr)
	if chunkFromIndex == nil {
		return false
	}
	cidsRegistered := true
	for c := range chunkFromIndex.Cids {
		chunkFromCidIndex := listener.chunker.getChunkByCID(c)
		if chunkFromCidIndex == nil || contextIDToStr(chunkFromCidIndex.ContextID) != ctxIDStr {
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

	return !chunkFromDatastore.Removed
}

func HasSnapshot(ctx context.Context, listener *ReframeListener) bool {
	has, err := listener.dsWrapper.hasSanpshot(ctx)
	return has && err == nil
}

func HasCidTimestamp(ctx context.Context, listener *ReframeListener, c cid.Cid) bool {
	has, err := listener.dsWrapper.hasCidTimestamp(ctx, c)
	return has && err == nil
}

func ChunkNotExist(ctx context.Context, listener *ReframeListener, cids []cid.Cid, nonceGen func() []byte) bool {
	ctxID := listener.chunker.generateContextID(cidsListToMap(cids))
	ctxIDStr := contextIDToStr(ctxID)
	cidsRegistered := false
	for _, c := range cids {
		chunkFromIndex := listener.chunker.getChunkByCID(c)
		if chunkFromIndex == nil || contextIDToStr(chunkFromIndex.ContextID) != ctxIDStr {
			continue
		}
		cidsRegistered = true
		break
	}
	if cidsRegistered {
		return false
	}

	chunkFromDatastore, err := listener.dsWrapper.getChunkByContextID(ctx, ctxID)
	if err != nil {
		return false
	}
	return chunkFromDatastore.Removed && listener.chunker.getChunkByContextID(ctxIDStr) == nil

}

func CidExist(ctx context.Context, listener *ReframeListener, c cid.Cid, requireChunk bool) bool {
	return listener.cidQueue.getNodeByCid(c) != nil && (!requireChunk || listener.chunker.getChunkByCID(c) != nil)
}

func CidNotExist(ctx context.Context, listener *ReframeListener, c cid.Cid) bool {
	return listener.cidQueue.getNodeByCid(c) == nil && listener.chunker.getChunkByCID(c) == nil
}

func GetCidTimestamp(ctx context.Context, listener *ReframeListener, c cid.Cid) (time.Time, error) {
	return listener.dsWrapper.getCidTimestamp(ctx, c)
}

func GetChunk(ctx context.Context, listener *ReframeListener, contextID string) *cidsChunk {
	return listener.chunker.getChunkByContextID(contextID)
}

func GetCurrentChunk(ctx context.Context, listener *ReframeListener) *cidsChunk {
	return listener.chunker.currentChunk
}

func GetExpiryQueue(ctx context.Context, listener *ReframeListener) []cid.Cid {
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
