package engine_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/ipni/index-provider/engine/chunker"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

var testMetadata = metadata.Default.New(metadata.Bitswap{})

const testTimeout = 30 * time.Second

func Test_SchemaNoEntriesErr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()
	// Assert both add and remove advertisements can be loaded
	_, err = subject.LinkSystem().Load(ipld.LinkContext{Ctx: ctx}, schema.NoEntries, schema.AdvertisementPrototype)
	require.Equal(t, "no entries; see schema.NoEntries", err.Error())
}

func Test_RemovalAdvertisementWithNoEntriesIsRetrievable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	ctxID := []byte("added then removed content")
	mhs := random.Cids(12)
	require.NoError(t, err)

	// Register lister with removal handle
	var removed bool
	subject.RegisterMultihashLister(func(ctx context.Context, provider peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		strCtxID := string(contextID)
		if strCtxID == string(ctxID) && !removed {
			return getMhIterator(t, mhs), nil
		}
		return nil, errors.New("not found")
	})

	// Publish content added advertisement.
	adAddCid, err := subject.NotifyPut(ctx, nil, ctxID, testMetadata)
	require.NoError(t, err)
	adAdd, err := subject.GetAdv(ctx, adAddCid)
	require.NoError(t, err)
	require.NotEqual(t, schema.NoEntries, adAdd.Entries)

	// Assert entries chunk from content added is resolvable
	_, err = subject.LinkSystem().Load(ipld.LinkContext{}, adAdd.Entries, schema.EntryChunkPrototype)
	require.NoError(t, err)

	// Clear cached entries to force re-generation of chunks on traversal
	require.NoError(t, subject.Chunker().Clear(ctx))

	// Signal to lister that context ID must no longer be found
	removed = true

	// Publish content removed advertisement
	adRemoveCid, err := subject.NotifyRemove(ctx, "", ctxID)
	require.NoError(t, err)
	adRemove, err := subject.GetAdv(ctx, adRemoveCid)
	require.NoError(t, err)
	require.Equal(t, schema.NoEntries, adRemove.Entries)

	lCtx := ipld.LinkContext{Ctx: ctx}
	// Assert both add and remove advertisements can be loaded
	_, err = subject.LinkSystem().Load(lCtx, cidlink.Link{Cid: adAddCid}, schema.AdvertisementPrototype)
	require.NoError(t, err)
	_, err = subject.LinkSystem().Load(lCtx, cidlink.Link{Cid: adRemoveCid}, schema.AdvertisementPrototype)
	require.NoError(t, err)

	// Assert chunks from advertisement that added content are not found
	_, err = subject.LinkSystem().Load(lCtx, adAdd.Entries, schema.EntryChunkPrototype)
	require.Equal(t, ipld.ErrNotExists{}, err)
}

func Test_EvictedCachedEntriesChainIsRegeneratedGracefully(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	chunkSize := 2
	cacheCap := 1
	subject, err := engine.New(engine.WithEntriesCacheCapacity(cacheCap), engine.WithChainedEntries(chunkSize))
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	otherProviderId, _, _ := random.Identity()

	ad1CtxID := []byte("first")
	ad1MhCount := 12
	wantAd1EntriesChainLen := ad1MhCount / chunkSize
	ad1Mhs := random.Cids(ad1MhCount)
	require.NoError(t, err)

	ad2CtxID := []byte("second")
	ad2MhCount := 10
	wantAd2ChunkLen := ad2MhCount / chunkSize
	ad2Mhs := random.Cids(ad2MhCount)
	require.NoError(t, err)

	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		strCtxID := string(contextID)
		if strCtxID == string(ad1CtxID) && p == subject.ProviderID() {
			return getMhIterator(t, ad1Mhs), nil
		}
		if strCtxID == string(ad2CtxID) && p == otherProviderId {
			return getMhIterator(t, ad2Mhs), nil
		}
		return nil, errors.New("not found")
	})

	ad1Cid, err := subject.NotifyPut(ctx, nil, ad1CtxID, testMetadata)
	require.NoError(t, err)
	ad1, err := subject.GetAdv(ctx, ad1Cid)
	require.NoError(t, err)
	ad1EntriesChain := listEntriesChainFromCache(t, subject.Chunker(), ad1.Entries)
	require.Len(t, ad1EntriesChain, wantAd1EntriesChainLen)
	requireChunkIsCached(t, subject.Chunker(), ad1EntriesChain...)
	a1Chunks := requireLoadEntryChunkFromEngine(t, subject, ad1EntriesChain...)

	// deliberately generating an add with a different provider to verify that both default and postfixed entries
	// are correctly loaded from the datastore
	ad2Cid, err := subject.NotifyPut(ctx, &peer.AddrInfo{ID: otherProviderId}, ad2CtxID, testMetadata)
	require.NoError(t, err)
	ad2, err := subject.GetAdv(ctx, ad2Cid)
	require.NoError(t, err)
	ad2EntriesChain := listEntriesChainFromCache(t, subject.Chunker(), ad2.Entries)
	require.Len(t, ad2EntriesChain, wantAd2ChunkLen)
	requireChunkIsCached(t, subject.Chunker(), ad2EntriesChain...)
	a2Chunks := requireLoadEntryChunkFromEngine(t, subject, ad2EntriesChain...)

	// Assert ad1 entries chain is evicted since cache capacity is set to 1.
	requireChunkIsNotCached(t, subject.Chunker(), ad1EntriesChain...)
	a1ChunksAfterReGen := requireLoadEntryChunkFromEngine(t, subject, ad1EntriesChain...)
	require.Equal(t, a1Chunks, a1ChunksAfterReGen)

	// Assert ad2 entries are no longer cached since ad1 entries were re-generated and cached.
	requireChunkIsNotCached(t, subject.Chunker(), ad2EntriesChain...)
	a2ChunksAfterReGen := requireLoadEntryChunkFromEngine(t, subject, ad2EntriesChain...)
	require.Equal(t, a2Chunks, a2ChunksAfterReGen)
}

func getMhIterator(t *testing.T, cids []cid.Cid) provider.MultihashIterator {
	idx := index.NewMultihashSorted()
	var records []index.Record
	for i, c := range cids {
		records = append(records, index.Record{
			Cid:    c,
			Offset: uint64(i + 1),
		})
	}
	err := idx.Load(records)
	require.NoError(t, err)
	iterator, err := provider.CarMultihashIterator(idx)
	require.NoError(t, err)
	return iterator
}

func listEntriesChainFromCache(t *testing.T, e *chunker.CachedEntriesChunker, root ipld.Link) []ipld.Link {
	next := root
	var links []ipld.Link
	for {
		raw, err := e.GetRawCachedChunk(context.TODO(), next)
		require.NoError(t, err)
		chunk := requireDecodeAsEntryChunk(t, root, raw)
		links = append(links, next)
		if chunk.Next == nil {
			break
		}
		next = chunk.Next
	}
	return links
}

func requireLoadEntryChunkFromEngine(t *testing.T, e *engine.Engine, l ...ipld.Link) []*schema.EntryChunk {
	var chunks []*schema.EntryChunk
	for _, link := range l {
		n, err := e.LinkSystem().Load(ipld.LinkContext{}, link, schema.EntryChunkPrototype)
		require.NoError(t, err)
		chunk, err := schema.UnwrapEntryChunk(n)
		require.NoError(t, err)
		chunks = append(chunks, chunk)
	}
	return chunks
}

func requireDecodeAsEntryChunk(t *testing.T, l ipld.Link, value []byte) *schema.EntryChunk {
	c := l.(cidlink.Link).Cid
	nb := schema.EntryChunkPrototype.NewBuilder()
	decoder, err := multicodec.LookupDecoder(c.Prefix().Codec)
	require.NoError(t, err)

	err = decoder(nb, bytes.NewBuffer(value))
	require.NoError(t, err)
	n := nb.Build()

	ec, err := schema.UnwrapEntryChunk(n)
	require.NoError(t, err)
	return ec
}

func requireChunkIsCached(t *testing.T, e *chunker.CachedEntriesChunker, l ...ipld.Link) {
	for _, link := range l {
		chunk, err := e.GetRawCachedChunk(context.TODO(), link)
		require.NoError(t, err)
		require.NotEmpty(t, chunk)
	}
}

func requireChunkIsNotCached(t *testing.T, e *chunker.CachedEntriesChunker, l ...ipld.Link) {
	for _, link := range l {
		chunk, err := e.GetRawCachedChunk(context.TODO(), link)
		require.NoError(t, err)
		require.Empty(t, chunk)
	}
}
