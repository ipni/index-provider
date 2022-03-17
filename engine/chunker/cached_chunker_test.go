package chunker_test

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/engine/chunker"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/stretchr/testify/require"
)

func TestCachedEntriesChunker_OverlappingLinkCounter(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	capacity := 10
	chunkSize := 10
	subject, err := chunker.NewCachedEntriesChunker(context.Background(), datastore.NewMapDatastore(), chunkSize, capacity)
	require.NoError(t, err)
	defer subject.Close()

	// Cache a link with 2 full chunks
	c1Cids, err := testutil.RandomCids(rng, 20)
	require.NoError(t, err)
	c1Lnk, err := subject.Chunk(ctx, getMhIterator(t, c1Cids))
	require.NoError(t, err)
	c1Chain := listEntriesChain(t, subject, c1Lnk)
	require.Len(t, c1Chain, 2)
	requireChunkIsCached(t, subject, c1Chain...)

	for i := 1; i < capacity*2; i++ {
		// Generate a chunk worth of CIDs
		newCids, err := testutil.RandomCids(rng, 10*rng.Intn(4)+1)
		require.NoError(t, err)
		// Append to the previously generated CIDs
		newCids = append(c1Cids, newCids...)
		wantChainLen := math.Ceil(float64(len(newCids)) / float64(chunkSize))
		chunk, err := subject.Chunk(ctx, getMhIterator(t, newCids))
		require.NoError(t, err)
		chain := listEntriesChain(t, subject, chunk)
		require.Len(t, chain, int(wantChainLen))
		requireChunkIsCached(t, subject, chain...)

		// Assert that the count of overlap for c1 grows up to capacity-1= 9.
		// Because, since all cached chunks generated in this test overlap with c1 chain after
		// capacity is reached, oldest should get removed and the count should be decremented to
		// equal one less than the total number of cached chunks, i.e. the overlapping chain is
		// shared with 9 other chunks.
		var wantOverlapCount int
		if i < capacity {
			wantOverlapCount = i
		} else {
			wantOverlapCount = capacity - 1
		}
		requireOverlapCount(t, subject, uint64(wantOverlapCount), c1Chain...)
	}
}

func TestCachedEntriesChunker_CapAndLen(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wantCap := 42
	subject, err := chunker.NewCachedEntriesChunker(context.Background(), datastore.NewMapDatastore(), 10, wantCap)
	require.NoError(t, err)
	defer subject.Close()
	require.Equal(t, wantCap, subject.Cap())
	require.Equal(t, 0, subject.Len())

	var chunks []ipld.Link
	for i := 1; i < wantCap*2; i++ {
		chunk, err := subject.Chunk(ctx, getRandomMhIterator(t, rng, rand.Intn(50)+5))
		require.NoError(t, err)
		requireChunkIsCached(t, subject, chunk)
		// Assert that capacity remains fixed.
		require.Equal(t, wantCap, subject.Cap())
		// Assert that length grows with cached chunks until capacity is reached.
		// After that the length should remain constant and equal to capacity.
		if i < wantCap {
			require.Equal(t, i, subject.Len())
		} else {
			require.Equal(t, wantCap, subject.Len())
		}
		chunks = append(chunks, chunk)
	}

	require.NoError(t, subject.Clear(ctx))
	requireChunkIsNotCached(t, subject, chunks...)
	require.Equal(t, 0, subject.Len())
	require.Equal(t, wantCap, subject.Cap())
}

func TestNewCachedEntriesChunker_FailsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ds := datastore.NewMapDatastore()
	// Prepare subject by caching something so that restore has data to recover
	subject, err := chunker.NewCachedEntriesChunker(ctx, ds, 10, 1)
	require.NoError(t, err)
	_, err = subject.Chunk(ctx, getRandomMhIterator(t, rand.New(rand.NewSource(1413)), 45))
	require.NoError(t, err)
	require.NoError(t, subject.Close())

	cancel()
	// Assert context is checked for error
	_, err = chunker.NewCachedEntriesChunker(ctx, ds, 10, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCachedEntriesChunker_NonOverlappingDagIsEvicted(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, 10, 1)
	require.NoError(t, err)
	defer subject.Close()

	// Cache a chain of length 5 and assert it is cached.
	c1Lnk, err := subject.Chunk(ctx, getRandomMhIterator(t, rng, 45))
	require.NoError(t, err)
	require.Equal(t, 1, subject.Len())
	c1Chain := listEntriesChain(t, subject, c1Lnk)
	require.Len(t, c1Chain, 5)
	requireChunkIsCached(t, subject, c1Chain...)

	// Cache a new, non-overlapping chain of length 2 and assert it is cached
	c2Lnk, err := subject.Chunk(ctx, getRandomMhIterator(t, rng, 15))
	require.NoError(t, err)
	c2Chain := listEntriesChain(t, subject, c2Lnk)
	require.Len(t, c2Chain, 2)
	requireChunkIsCached(t, subject, c2Chain...)
	require.Equal(t, 1, subject.Len())

	// Assert the first chain is fully evicted
	requireChunkIsNotCached(t, subject, c1Chain...)
}

func TestCachedEntriesChunker_PreviouslyCachedChunksAreRestored(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	chunkSize := 10
	capacity := 5
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, chunkSize, capacity)
	require.NoError(t, err)
	defer subject.Close()

	// Chunk and cache a multihash iterator.
	c1Lnk, err := subject.Chunk(ctx, getRandomMhIterator(t, rng, 50))
	require.NoError(t, err)

	// Assert the entire chain is cached.
	c1Chain := listEntriesChain(t, subject, c1Lnk)
	require.Len(t, c1Chain, 5)
	requireChunkIsCached(t, subject, c1Chain...)

	// Chunk another iterators with overlapping section.
	c2Cids, err := testutil.RandomCids(rng, 12)
	require.NoError(t, err)
	c2Lnk, err := subject.Chunk(ctx, getMhIterator(t, c2Cids))
	require.NoError(t, err)

	// Assert the entire c2 chain is cached.
	c2Chain := listEntriesChain(t, subject, c2Lnk)
	require.Len(t, c2Chain, 2)
	requireChunkIsCached(t, subject, c2Chain...)

	// Chunk and cache another multihash iterators with overlapping section.
	c3Cids, err := testutil.RandomCids(rng, 13)
	require.NoError(t, err)
	c3Cids = append(c2Cids, c3Cids...)
	c3Lnk, err := subject.Chunk(ctx, getMhIterator(t, c3Cids))
	require.NoError(t, err)

	// Assert the entire c2 chain is cached.
	// Expect 3 chunks: 13 new cids plus 12 from overlap, total 25
	c3Chain := listEntriesChain(t, subject, c3Lnk)
	require.Len(t, c3Chain, 3)
	requireChunkIsCached(t, subject, c3Chain...)

	// Close the cache.
	require.NoError(t, subject.Close())

	// Re-create cache from the same datastore
	subject, err = chunker.NewCachedEntriesChunker(ctx, store, chunkSize, capacity)
	require.NoError(t, err)

	// Assert previously cached chain is still cached after subject is recreated from the same
	// datastore.
	requireChunkIsCached(t, subject, c1Chain...)
	requireChunkIsCached(t, subject, c2Chain...)
	requireChunkIsCached(t, subject, c3Chain...)
}

func TestCachedEntriesChunker_OverlappingDagIsNotEvicted(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, 10, 1)
	require.NoError(t, err)
	defer subject.Close()

	// Chunk 10 random CIDs as c1 and assert that:
	//  1. It is cached as a valid EntryChunk
	//  2. Its entries match the original CIDs
	//  3. The length of chain is 1, i.e. the chunk has no next since chunkSize = 10
	//  4. The cache length is 1.
	c1Cids, err := testutil.RandomCids(rng, 10)
	require.NoError(t, err)
	c1Lnk, err := subject.Chunk(ctx, getMhIterator(t, c1Cids))
	require.NoError(t, err)
	c1Raw, err := subject.GetRawCachedChunk(ctx, c1Lnk)
	require.NoError(t, err)
	c1 := requireDecodeAsEntryChunk(t, c1Lnk, c1Raw)
	requireChunkEntriesMatch(t, c1, c1Cids)
	require.Nil(t, c1.Next)
	require.Equal(t, 1, subject.Len())

	// Chunk a total of 20 random CIDs as c2, where the CIDs are made up of 10 newly generated CIDs
	// and 10 CIDs from c1, i.e. overlapping entries chain, and assert that:
	//  1. It is cached as a valid chain of EntryChunk
	//  2. The length of chain is 2.
	//  3. The first entry in chain has all the newly generated CIDs
	//  4. The second entry in chain is identical to c1.
	//  5. The length of cache is still 1.
	extraCids, err := testutil.RandomCids(rng, 10)
	require.NoError(t, err)
	c2Cids := append(c1Cids, extraCids...)
	c2Lnk, err := subject.Chunk(ctx, getMhIterator(t, c2Cids))
	require.NoError(t, err)

	c2Raw, err := subject.GetRawCachedChunk(ctx, c2Lnk)
	require.NoError(t, err)
	c2 := requireDecodeAsEntryChunk(t, c2Lnk, c2Raw)
	requireChunkEntriesMatch(t, c2, extraCids)
	require.NotNil(t, c2.Next)
	c2NextLnk := *c2.Next
	require.Equal(t, c1Lnk, c2NextLnk)

	c2NextRaw, err := subject.GetRawCachedChunk(ctx, c2NextLnk)
	require.NoError(t, err)
	c2Next := requireDecodeAsEntryChunk(t, c2NextLnk, c2NextRaw)
	requireChunkEntriesMatch(t, c2Next, c1Cids)
	require.Nil(t, c2Next.Next)
	require.Equal(t, 1, subject.Len())
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

func requireChunkEntriesMatch(t *testing.T, chunk *schema.EntryChunk, want []cid.Cid) {
	for i, gotMh := range chunk.Entries {
		wantMh := want[i].Hash()
		require.Equal(t, wantMh, gotMh)
	}
}

func listEntriesChain(t *testing.T, e *chunker.CachedEntriesChunker, root ipld.Link) []ipld.Link {
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
		next = *chunk.Next
		require.NoError(t, err)
	}
	return links
}

func requireDecodeAsEntryChunk(t *testing.T, l ipld.Link, value []byte) *schema.EntryChunk {
	c := l.(cidlink.Link).Cid
	nb := schema.EntryChunkPrototype.NewBuilder()
	decoder, err := multicodec.LookupDecoder(c.Prefix().Codec)
	require.NoError(t, err)

	err = decoder(nb, bytes.NewBuffer(value))
	require.NoError(t, err)
	n := nb.Build()
	chunk, err := schema.UnwrapEntryChunk(n)
	require.NoError(t, err)
	return chunk
}

func getRandomMhIterator(t *testing.T, rng *rand.Rand, mhCount int) provider.MultihashIterator {
	cids, err := testutil.RandomCids(rng, mhCount)
	require.NoError(t, err)
	return getMhIterator(t, cids)
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

func requireOverlapCount(t *testing.T, s *chunker.CachedEntriesChunker, want uint64, l ...ipld.Link) {
	for _, link := range l {
		got, err := s.CountOverlap(context.TODO(), link)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}
