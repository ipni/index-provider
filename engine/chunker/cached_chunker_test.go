package chunker_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	ipldcodec "github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipni/go-libipni/ingest/schema"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine/chunker"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestCachedEntriesChunker_Chain_OverlappingLinkCounter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	capacity := 10
	chunkSize := 10
	subject, err := chunker.NewCachedEntriesChunker(ctx, datastore.NewMapDatastore(), capacity, chunker.NewChainChunkerFunc(chunkSize), false)
	require.NoError(t, err)
	defer subject.Close()

	// Cache a link with 2 full chunks
	c1Mhs := random.Multihashes(20)
	c1Lnk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(c1Mhs))
	require.NoError(t, err)
	c1Chain := listEntriesChain(t, subject, c1Lnk)
	require.Len(t, c1Chain, 2)
	requireChunkIsCached(t, subject, c1Chain...)

	for i := 1; i < capacity*2; i++ {
		// Generate a chunk worth of CIDs
		newMhs := random.Multihashes(i * 2)
		// Append to the previously generated CIDs
		newMhs = append(c1Mhs, newMhs...)
		wantChainLen := math.Ceil(float64(len(newMhs)) / float64(chunkSize))
		chunk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(newMhs))
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

func TestCachedEntriesChunker(t *testing.T) {
	tests := []struct {
		capacity int
		c        chunker.NewChunkerFunc
	}{
		{42, chunker.NewChainChunkerFunc(10)},
		{42, chunker.NewHamtChunkerFunc(multicodec.Murmur3X64_64, 3, 1)},
	}
	for _, test := range tests {
		t.Run("CapAndLen", func(t *testing.T) {
			testCachedEntriesChunker_CapAndLen(t, test.capacity, test.c)
		})
		t.Run("FailsWhenContextIsCancelled", func(t *testing.T) {
			testNewCachedEntriesChunker_FailsWhenContextIsCancelled(t, test.capacity, test.c)
		})
		t.Run("NonOverlappingDagIsEvicted", func(t *testing.T) {
			testCachedEntriesChunker_NonOverlappingDagIsEvicted(t, test.c)
		})
		t.Run("PreviouslyCachedChunksAreRestored", func(t *testing.T) {
			testCachedEntriesChunker_PreviouslyCachedChunksAreRestored(t, test.capacity, test.c)
		})
		t.Run("RecoversFromCorruptCacheGracefully", func(t *testing.T) {
			testCachedEntriesChunker_RecoversFromCorruptCacheGracefully(t, test.capacity, test.c)
		})
		t.Run("OldFormatIsHandledGracefully", func(t *testing.T) {
			testCachedEntriesChunker_OldFormatIsHandledGracefully(t, test.capacity, test.c)
		})
		t.Run("PurgesCacheSuccessfully", func(t *testing.T) {
			testCachedEntriesChunker_PurgesCacheSuccessfully(t, test.capacity, test.c)
		})
		t.Run("PurgesCacheSuccessfullyEvenIfCorrupted", func(t *testing.T) {
			testCachedEntriesChunker_PurgesCacheSuccessfullyEvenIfCorrupted(t, test.capacity, test.c)
		})
	}
}

func testCachedEntriesChunker_CapAndLen(t *testing.T, capacity int, c chunker.NewChunkerFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	subject, err := chunker.NewCachedEntriesChunker(context.Background(), datastore.NewMapDatastore(), capacity, c, false)
	require.NoError(t, err)
	defer subject.Close()
	require.Equal(t, capacity, subject.Cap())
	require.Equal(t, 0, subject.Len())

	var chunks []ipld.Link
	for i := 1; i < capacity*2; i++ {
		mhs := random.Multihashes(5 * i)
		chunk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(mhs))
		require.NoError(t, err)
		requireChunkIsCached(t, subject, chunk)
		// Assert that capacity remains fixed.
		require.Equal(t, capacity, subject.Cap())
		// Assert that length grows with cached chunks until capacity is reached.
		// After that the length should remain constant and equal to capacity.
		if i < capacity {
			require.Equal(t, i, subject.Len())
		} else {
			require.Equal(t, capacity, subject.Len())
		}
		chunks = append(chunks, chunk)
	}

	require.NoError(t, subject.Clear(ctx))
	requireChunkIsNotCached(t, subject, chunks...)
	require.Equal(t, 0, subject.Len())
	require.Equal(t, capacity, subject.Cap())
}

func testNewCachedEntriesChunker_FailsWhenContextIsCancelled(t *testing.T, capacity int, c chunker.NewChunkerFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ds := datastore.NewMapDatastore()
	// Prepare subject by caching something so that restore has data to recover
	subject, err := chunker.NewCachedEntriesChunker(ctx, ds, capacity, c, false)
	require.NoError(t, err)
	mhs := random.Multihashes(45)
	_, err = subject.Chunk(ctx, provider.SliceMultihashIterator(mhs))
	require.NoError(t, err)
	require.NoError(t, subject.Close())

	cancel()
	// Assert context is checked for error
	_, err = chunker.NewCachedEntriesChunker(ctx, ds, 1, chunker.NewChainChunkerFunc(10), false)
	require.ErrorIs(t, err, context.Canceled)
}

func testCachedEntriesChunker_NonOverlappingDagIsEvicted(t *testing.T, c chunker.NewChunkerFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, 1, c, false)
	require.NoError(t, err)
	defer subject.Close()

	// Cache a chain of length 5 and assert it is cached.
	c1Mhs := random.Multihashes(45)
	c1Lnk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(c1Mhs))
	require.NoError(t, err)
	require.Equal(t, 1, subject.Len())
	gotC1Mhs := requireDecodeAllMultihashes(t, c1Lnk, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotC1Mhs, c1Mhs)

	// Cache a new, non-overlapping chain of length 2 and assert it is cached
	c2Mhs := random.Multihashes(15)
	c2Lnk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(c2Mhs))
	require.NoError(t, err)
	gotC2Mhs := requireDecodeAllMultihashes(t, c2Lnk, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotC2Mhs, c2Mhs)
	require.Equal(t, 1, subject.Len())

	// Assert the first chain is fully evicted
	requireChunkIsNotCached(t, subject, c1Lnk)
}

func testCachedEntriesChunker_PreviouslyCachedChunksAreRestored(t *testing.T, capacity int, c chunker.NewChunkerFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, capacity, c, false)
	require.NoError(t, err)
	defer subject.Close()

	// Chunk and cache a multihash iterator.
	c1Mhs := random.Multihashes(50)
	c1Lnk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(c1Mhs))
	require.NoError(t, err)

	// Assert the entire chain is cached.
	gotC1Mhs := requireDecodeAllMultihashes(t, c1Lnk, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotC1Mhs, c1Mhs)

	// Chunk another iterator.
	c2Mhs := random.Multihashes(12)
	c2Lnk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(c2Mhs))
	require.NoError(t, err)

	// Assert the entire c2 chain is cached.
	gotC2Mhs := requireDecodeAllMultihashes(t, c2Lnk, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotC2Mhs, c2Mhs)

	// Chunk and cache another multihash iterators with overlapping section.
	c3Mhs := random.Multihashes(13)
	require.NoError(t, err)
	c3Mhs = append(c2Mhs, c3Mhs...)
	c3Lnk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(c3Mhs))
	require.NoError(t, err)

	// Assert the entire c2 chain is cached.
	// Expect 3 chunks: 13 new cids plus 12 from overlap, total 25
	gotC3Mhs := requireDecodeAllMultihashes(t, c3Lnk, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotC3Mhs, c3Mhs)

	// Close the cache.
	require.NoError(t, subject.Close())

	// Re-create cache from the same datastore
	subject, err = chunker.NewCachedEntriesChunker(ctx, store, capacity, c, false)
	require.NoError(t, err)

	// Assert previously cached chain is still cached after subject is recreated from the same
	// datastore.
	gotC1Mhs = requireDecodeAllMultihashes(t, c1Lnk, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotC1Mhs, c1Mhs)

	gotC2Mhs = requireDecodeAllMultihashes(t, c2Lnk, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotC2Mhs, c2Mhs)

	gotC3Mhs = requireDecodeAllMultihashes(t, c3Lnk, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotC3Mhs, c3Mhs)
}

func TestCachedEntriesChunker_OverlappingDagIsNotEvicted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, 1, chunker.NewChainChunkerFunc(10), false)
	require.NoError(t, err)
	defer subject.Close()

	// Chunk 10 random CIDs as c1 and assert that:
	//  1. It is cached as a valid EntryChunk
	//  2. Its entries match the original CIDs
	//  3. The length of chain is 1, i.e. the chunk has no next since chunkSize = 10
	//  4. The cache length is 1.
	c1Mhs := random.Multihashes(10)
	require.NoError(t, err)
	c1Lnk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(c1Mhs))
	require.NoError(t, err)
	c1Raw, err := subject.GetRawCachedChunk(ctx, c1Lnk)
	require.NoError(t, err)
	c1 := requireDecodeAsEntryChunk(t, c1Lnk, c1Raw)
	requireChunkEntriesMatch(t, c1.Entries, c1Mhs)
	require.Nil(t, c1.Next)
	require.Equal(t, 1, subject.Len())

	// Chunk a total of 20 random CIDs as c2, where the CIDs are made up of 10 newly generated CIDs
	// and 10 CIDs from c1, i.e. overlapping entries chain, and assert that:
	//  1. It is cached as a valid chain of EntryChunk
	//  2. The length of chain is 2.
	//  3. The first entry in chain has all the newly generated CIDs
	//  4. The second entry in chain is identical to c1.
	//  5. The length of cache is still 1.
	extraMhs := random.Multihashes(10)
	require.NoError(t, err)
	c2Mhs := append(c1Mhs, extraMhs...)
	c2Lnk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(c2Mhs))
	require.NoError(t, err)

	c2Raw, err := subject.GetRawCachedChunk(ctx, c2Lnk)
	require.NoError(t, err)
	c2 := requireDecodeAsEntryChunk(t, c2Lnk, c2Raw)
	requireChunkEntriesMatch(t, c2.Entries, extraMhs)
	require.NotNil(t, c2.Next)
	c2NextLnk := c2.Next
	require.Equal(t, c1Lnk, c2NextLnk)

	c2NextRaw, err := subject.GetRawCachedChunk(ctx, c2NextLnk)
	require.NoError(t, err)
	c2Next := requireDecodeAsEntryChunk(t, c2NextLnk, c2NextRaw)
	requireChunkEntriesMatch(t, c2Next.Entries, c1Mhs)
	require.Nil(t, c2Next.Next)
	require.Equal(t, 1, subject.Len())
}

func testCachedEntriesChunker_RecoversFromCorruptCacheGracefully(t *testing.T, capacity int, c chunker.NewChunkerFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, capacity, c, false)
	require.NoError(t, err)
	defer subject.Close()

	// Chunk some data first which we won't corrupt to test partial corruption.
	wantMhs1 := random.Multihashes(10)
	require.NoError(t, err)
	chunkLink1, err := subject.Chunk(ctx, provider.SliceMultihashIterator(wantMhs1))
	require.NoError(t, err)
	raw1, err := subject.GetRawCachedChunk(ctx, chunkLink1)
	require.NoError(t, err)
	gotMhs1 := requireDecodeAllMultihashes(t, chunkLink1, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotMhs1, wantMhs1)
	require.Equal(t, 1, subject.Len())

	// Chunk some more data which we will corrupt.
	wantMhs2 := random.Multihashes(10)
	require.NoError(t, err)
	chunkLink2, err := subject.Chunk(ctx, provider.SliceMultihashIterator(wantMhs2))
	require.NoError(t, err)
	raw2, err := subject.GetRawCachedChunk(ctx, chunkLink2)
	require.NoError(t, err)
	gotMhs2 := requireDecodeAllMultihashes(t, chunkLink2, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotMhs2, wantMhs2)
	require.Equal(t, 2, subject.Len())

	// Corrupt the second cached data.
	require.NoError(t, subject.Close())
	key2 := chunker.RootPrefixedDSKey(chunkLink2)
	err = store.Put(ctx, key2, []byte("fish"))
	require.NoError(t, err)

	// Assert that chunker can be re-instantiated when cached entries are corrupt.
	subject, err = chunker.NewCachedEntriesChunker(ctx, store, capacity, c, false)
	require.NoError(t, err)

	// Assert that data is cleared.
	raw1Again, err := subject.GetRawCachedChunk(ctx, chunkLink1)
	require.NoError(t, err)
	require.Nil(t, raw1Again)
	raw2Again, err := subject.GetRawCachedChunk(ctx, chunkLink2)
	require.NoError(t, err)
	require.Nil(t, raw2Again)

	// Cache the same data again and assert we get the same bytes back for both chunks.
	chunkLink1After, err := subject.Chunk(ctx, provider.SliceMultihashIterator(wantMhs1))
	require.NoError(t, err)
	require.Equal(t, chunkLink1, chunkLink1After)
	raw1After, err := subject.GetRawCachedChunk(ctx, chunkLink1After)
	require.NoError(t, err)
	require.Equal(t, raw1, raw1After)

	chunkLink2After, err := subject.Chunk(ctx, provider.SliceMultihashIterator(wantMhs2))
	require.NoError(t, err)
	require.Equal(t, chunkLink2, chunkLink2After)
	raw2After, err := subject.GetRawCachedChunk(ctx, chunkLink2After)
	require.NoError(t, err)
	require.Equal(t, raw2, raw2After)
}

func testCachedEntriesChunker_PurgesCacheSuccessfullyEvenIfCorrupted(t *testing.T, capacity int, c chunker.NewChunkerFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, capacity, c, false)
	require.NoError(t, err)
	defer subject.Close()

	// Chunk some data
	wantMhs1 := random.Multihashes(10)
	require.NoError(t, err)
	chunkLink1, err := subject.Chunk(ctx, provider.SliceMultihashIterator(wantMhs1))
	require.NoError(t, err)
	gotMhs1 := requireDecodeAllMultihashes(t, chunkLink1, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotMhs1, wantMhs1)
	require.Equal(t, 1, subject.Len())

	// Close off the test subject
	require.NoError(t, subject.Close())

	// Corrupt the cached data.
	key2 := chunker.RootPrefixedDSKey(chunkLink1)
	err = store.Put(ctx, key2, []byte("fish"))
	require.NoError(t, err)

	// Instantiate a new test subject with purge true
	subject, err = chunker.NewCachedEntriesChunker(ctx, store, capacity, c, true)
	require.NoError(t, err)

	// Assert that data is cleared.
	raw1Again, err := subject.GetRawCachedChunk(ctx, chunkLink1)
	require.NoError(t, err)
	require.Nil(t, raw1Again)
	require.Equal(t, 0, subject.Len())
}

func testCachedEntriesChunker_PurgesCacheSuccessfully(t *testing.T, capacity int, c chunker.NewChunkerFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, capacity, c, false)
	require.NoError(t, err)
	defer subject.Close()

	// Chunk some data
	wantMhs1 := random.Multihashes(10)
	require.NoError(t, err)
	chunkLink1, err := subject.Chunk(ctx, provider.SliceMultihashIterator(wantMhs1))
	require.NoError(t, err)
	gotEntryChunk1 := requireDecodeAllMultihashes(t, chunkLink1, subject.LinkSystem())
	requireChunkEntriesMatch(t, gotEntryChunk1, wantMhs1)
	require.Equal(t, 1, subject.Len())

	// Close off the test subject
	require.NoError(t, subject.Close())

	// Instantiate a new test subject with purge true
	subject, err = chunker.NewCachedEntriesChunker(ctx, store, capacity, c, true)
	require.NoError(t, err)

	// Assert that data is cleared.
	raw1Again, err := subject.GetRawCachedChunk(ctx, chunkLink1)
	require.NoError(t, err)
	require.Nil(t, raw1Again)
	require.Equal(t, 0, subject.Len())
}

func testCachedEntriesChunker_OldFormatIsHandledGracefully(t *testing.T, capacity int, c chunker.NewChunkerFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, capacity, c, false)
	require.NoError(t, err)
	defer subject.Close()

	// Chunk and cache a multihash iterator.
	root, err := subject.Chunk(ctx, provider.SliceMultihashIterator(random.Multihashes(50)))
	require.NoError(t, err)

	// Close off the test subject
	require.NoError(t, subject.Close())

	// Clear the value associated to root key to mimic the old format.
	rootKey := chunker.RootPrefixedDSKey(root)
	err = store.Put(ctx, rootKey, nil)
	require.NoError(t, err)

	subject, err = chunker.NewCachedEntriesChunker(ctx, store, capacity, c, false)
	require.NoError(t, err)
	require.Equal(t, 0, subject.Len())
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

func requireChunkEntriesMatch(t *testing.T, got, want []multihash.Multihash) {
	wantLen := len(want)
	require.Len(t, got, wantLen)
	diff := make(map[string]int, wantLen)
	for _, w := range want {
		diff[string(w)]++
	}
	for _, g := range got {
		gk := string(g)
		_, ok := diff[gk]
		require.True(t, ok)
		diff[gk] -= 1
		if diff[gk] == 0 {
			delete(diff, gk)
		}
	}
	require.Len(t, diff, 0)
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
		next = chunk.Next
		require.NoError(t, err)
	}
	return links
}

func requireDecodeAllMultihashes(t *testing.T, l ipld.Link, ls ipld.LinkSystem) []multihash.Multihash {
	var hamtRoot bool
	lctx := ipld.LinkContext{Ctx: context.TODO()}
	n, err := ls.Load(lctx, l, schema.EntryChunkPrototype)
	if err != nil {
		n, err = ls.Load(lctx, l, hamt.HashMapRootPrototype)
		hamtRoot = true
	}
	require.NoError(t, err)

	if !hamtRoot {
		chunk, err := schema.UnwrapEntryChunk(n)
		require.NoError(t, err)
		mhs := chunk.Entries
		if chunk.Next != nil {
			mhs = append(mhs, requireDecodeAllMultihashes(t, chunk.Next, ls)...)
		}
		return mhs
	}

	root := bindnode.Unwrap(n).(*hamt.HashMapRoot)
	require.NotNil(t, root)

	mhi := provider.HamtMultihashIterator(root, ls)
	var mhs []multihash.Multihash
	for {
		mh, err := mhi.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		mhs = append(mhs, mh)
	}
	return mhs
}

func requireDecodeAsEntryChunk(t *testing.T, l ipld.Link, value []byte) *schema.EntryChunk {
	c := l.(cidlink.Link).Cid
	nb := schema.EntryChunkPrototype.NewBuilder()
	decoder, err := ipldcodec.LookupDecoder(c.Prefix().Codec)
	require.NoError(t, err)

	err = decoder(nb, bytes.NewBuffer(value))
	require.NoError(t, err)
	n := nb.Build()
	chunk, err := schema.UnwrapEntryChunk(n)
	require.NoError(t, err)
	return chunk
}

func requireOverlapCount(t *testing.T, s *chunker.CachedEntriesChunker, want uint64, l ...ipld.Link) {
	for _, link := range l {
		got, err := s.CountOverlap(context.TODO(), link)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}
