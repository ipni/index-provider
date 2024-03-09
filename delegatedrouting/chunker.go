package delegatedrouting

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"slices"
	"time"

	"github.com/ipfs/go-cid"
)

type chunker struct {
	chunkByContextId map[string]*cidsChunk
	currentChunk     *cidsChunk
	currentChunkTime time.Time
	chunkSizeFunc    func() int
	nonceGen         func() []byte
}

type cidsChunk struct {
	ContextID []byte
	Cids      map[cid.Cid]struct{}
	// unused field left for backward compatibility purposes
	Removed bool
}

func defaultNonceGen() []byte {
	nonce := make([]byte, 8)
	_, err := rand.Read(nonce)
	if err != nil {
		// Do not allow non-random nonce to be created.
		panic(err.Error())
	}
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
	ch.setNewCurrentChunk()
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

func (ch *chunker) setNewCurrentChunk() {
	ch.currentChunk = &cidsChunk{Cids: make(map[cid.Cid]struct{}, ch.chunkSizeFunc()), Removed: false}
	ch.currentChunkTime = time.Now()
}

func (ch *chunker) addCidToCurrentChunk(ctx context.Context, c cid.Cid, chunkFullFunc func(*cidsChunk) error) error {
	// if the cid is already in the chunk - do nothing
	if _, ok := ch.currentChunk.Cids[c]; ok {
		return nil
	}

	// if the current chunk is full - publish it and create a new one
	if len(ch.currentChunk.Cids) >= ch.chunkSizeFunc() {
		err := ch.flushCurrentChunk(ctx, chunkFullFunc)
		if err != nil {
			return err
		}
	}

	ch.currentChunk.Cids[c] = struct{}{}

	return nil
}

func (ch *chunker) flushCurrentChunk(ctx context.Context, chunkFullFunc func(*cidsChunk) error) error {
	ch.currentChunk.ContextID = ch.generateContextID(ch.currentChunk.Cids)
	err := chunkFullFunc(ch.currentChunk)
	if err != nil {
		return err
	}

	ch.setNewCurrentChunk()
	return nil
}

func (ch *chunker) generateContextID(cidsMap map[cid.Cid]struct{}) []byte {
	cids := make([]string, len(cidsMap))
	i := 0
	for k := range cidsMap {
		cids[i] = k.String()
		i++
	}
	slices.Sort(cids)

	hasher := sha256.New()
	for _, c := range cids {
		hasher.Write([]byte(c))
	}
	hasher.Write(ch.nonceGen())
	return hasher.Sum(nil)
}
