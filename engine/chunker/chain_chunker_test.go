package chunker_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipni/go-libipni/ingest/schema"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine/chunker"
	"github.com/stretchr/testify/require"
)

func TestChainChunker_Chunk(t *testing.T) {
	ctx := context.TODO()
	ls := cidlink.DefaultLinkSystem()
	chunkHasExpectedMhs := func(t *testing.T, subject chunker.EntriesChunker) {
		mhs := random.Multihashes(100)
		l, err := subject.Chunk(ctx, provider.SliceMultihashIterator(mhs))
		require.NoError(t, err)

		ecn, err := ls.Load(ipld.LinkContext{Ctx: ctx}, l, schema.EntryChunkPrototype)
		require.NoError(t, err)

		chunk, err := schema.UnwrapEntryChunk(ecn)
		require.NoError(t, err)
		require.NotNil(t, chunk)

		gotMhs := requireDecodeAllMultihashes(t, l, ls)
		requireChunkEntriesMatch(t, gotMhs, mhs)
	}
	t.Run("NewChainChunker", func(t *testing.T) {
		store := &memstore.Store{}
		ls.SetReadStorage(store)
		ls.SetWriteStorage(store)
		subject, err := chunker.NewChainChunker(&ls, 7)
		require.NoError(t, err)
		chunkHasExpectedMhs(t, subject)
	})
	t.Run("NewChainChunkerFunc", func(t *testing.T) {
		store := &memstore.Store{}
		ls.SetReadStorage(store)
		ls.SetWriteStorage(store)
		subject, err := chunker.NewChainChunkerFunc(7)(&ls)
		require.NoError(t, err)
		chunkHasExpectedMhs(t, subject)
	})
}
