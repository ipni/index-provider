package delegatedrouting

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"sort"

	"github.com/ipfs/go-cid"
)

type chunker struct {
	chunkByContextId map[string]*cidsChunk
	currentChunk     *cidsChunk
	chunkSizeFunc    func() int
	nonceGen         func() []byte
}

type cidsChunk struct {
	ContextID []byte
	Cids      map[cid.Cid]struct{}
	Removed   bool
}

func defaultNonceGen() []byte {
	nonce := make([]byte, 8)
	rand.Read(nonce)
	return nonce
}

func newChunker(chunkSizeFunc func() int, nonceGenFunc func() []byte) *chunker {
	if nonceGenFunc == nil {
		nonceGenFunc = defaultNonceGen
	}
	ch := &chunker{
		chunkByContextId: make(map[string]*cidsChunk),
		chunkSizeFunc:    chunkSizeFunc,
		nonceGen:         nonceGenFunc,
	}
	ch.currentChunk = ch.newCidsChunk()
	return ch
}

func (ch *chunker) getChunkByContextID(ctxID string) *cidsChunk {
	if ctxID == contextIDToStr(ch.currentChunk.ContextID) {
		return ch.currentChunk
	}
	return ch.chunkByContextId[ctxID]
}

func (ch *chunker) addChunk(chunk *cidsChunk) {
	ch.chunkByContextId[contextIDToStr(chunk.ContextID)] = chunk
}

func (ch *chunker) removeChunk(chunk *cidsChunk) {
	delete(ch.chunkByContextId, contextIDToStr(chunk.ContextID))
}

func (ch *chunker) newCidsChunk() *cidsChunk {
	return &cidsChunk{Cids: make(map[cid.Cid]struct{}, ch.chunkSizeFunc()), Removed: false}
}

func (ch *chunker) addCidToCurrentChunk(ctx context.Context, c cid.Cid, chunkFullFunc func(*cidsChunk) error) error {
	// if the cid is already in the chunk - do nothing
	if _, ok := ch.currentChunk.Cids[c]; ok {
		return nil
	}

	// if the current chunk is full - publish it and create a new one
	if len(ch.currentChunk.Cids) >= ch.chunkSizeFunc() {
		ch.currentChunk.ContextID = ch.generateContextID(ch.currentChunk.Cids)
		err := chunkFullFunc(ch.currentChunk)
		if err != nil {
			return err
		}

		ch.currentChunk = ch.newCidsChunk()
	}

	ch.currentChunk.Cids[c] = struct{}{}

	return nil
}

func (ch *chunker) generateContextID(cidsMap map[cid.Cid]struct{}) []byte {
	cids := make([]string, len(cidsMap))
	i := 0
	for k := range cidsMap {
		cids[i] = k.String()
		i++
	}
	sort.Strings(cids)

	hasher := sha256.New()
	for _, c := range cids {
		hasher.Write([]byte(c))
	}
	hasher.Write(ch.nonceGen())
	return hasher.Sum(nil)
}
