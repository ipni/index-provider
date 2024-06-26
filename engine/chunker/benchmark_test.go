package chunker_test

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine/chunker"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func BenchmarkCachedChunker(b *testing.B) {
	const capacity = 100
	const mhCount = 1000
	const byteSize = capacity * mhCount * 256 / 8 // multicodec.Sha2_256

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var mhis [][]multihash.Multihash
	for i := 0; i < capacity; i++ {
		mhis = append(mhis, random.Multihashes(mhCount))
	}

	b.Run("ChainedEntryChunk/ChunkSize_1", benchmarkCachedChunker(ctx, byteSize, capacity, mhis, chunker.NewChainChunkerFunc(1)))
	b.Run("ChainedEntryChunk/ChunkSize_2", benchmarkCachedChunker(ctx, byteSize, capacity, mhis, chunker.NewChainChunkerFunc(2)))
	b.Run("ChainedEntryChunk/ChunkSize_8", benchmarkCachedChunker(ctx, byteSize, capacity, mhis, chunker.NewChainChunkerFunc(8)))
	b.Run("ChainedEntryChunk/ChunkSize_16", benchmarkCachedChunker(ctx, byteSize, capacity, mhis, chunker.NewChainChunkerFunc(16)))

	b.Run("HamtEntryChunk/Murmur_BitWidth_3_BucketSize_1", benchmarkCachedChunker(ctx, byteSize, capacity, mhis, chunker.NewHamtChunkerFunc(multicodec.Murmur3X64_64, 3, 1)))
	b.Run("HamtEntryChunk/Murmur_BitWidth_4_BucketSize_2", benchmarkCachedChunker(ctx, byteSize, capacity, mhis, chunker.NewHamtChunkerFunc(multicodec.Murmur3X64_64, 4, 2)))
	b.Run("HamtEntryChunk/Murmur_BitWidth_8_BucketSize_3", benchmarkCachedChunker(ctx, byteSize, capacity, mhis, chunker.NewHamtChunkerFunc(multicodec.Murmur3X64_64, 8, 3)))
	b.Run("HamtEntryChunk/Murmur_BitWidth_16_BucketSize_4", benchmarkCachedChunker(ctx, byteSize, capacity, mhis, chunker.NewHamtChunkerFunc(multicodec.Murmur3X64_64, 16, 4)))
}

func benchmarkCachedChunker(ctx context.Context, byteSize int64, capacity int, mhis [][]multihash.Multihash, c chunker.NewChunkerFunc) func(b *testing.B) {
	return func(b *testing.B) {
		b.SetBytes(byteSize)
		b.ReportAllocs()

		store := dssync.MutexWrap(datastore.NewMapDatastore())
		subject, err := chunker.NewCachedEntriesChunker(ctx, store, capacity, c, false)

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				require.NoError(b, err)
				for _, mhs := range mhis {
					mhi := provider.SliceMultihashIterator(mhs)
					chunk, err := subject.Chunk(ctx, mhi)
					require.NoError(b, err)
					require.NotNil(b, chunk)
				}
				require.NoError(b, subject.Close())
			}
		})
	}
}

func BenchmarkRestoreCache_ChainChunker(b *testing.B) {
	const chunkSize = 1
	const capacity = 100
	const mhCount = 500
	const byteSize = capacity * mhCount * 256 / 8 // multicodec.Sha2_256

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := dssync.MutexWrap(datastore.NewMapDatastore())

	// Populate the datastore with data.
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, capacity, chunker.NewChainChunkerFunc(chunkSize), false)
	require.NoError(b, err)
	for i := 0; i < capacity; i++ {
		mhi := random.Multihashes(mhCount)
		chunk, err := subject.Chunk(ctx, provider.SliceMultihashIterator(mhi))
		require.NoError(b, err)
		require.NotNil(b, chunk)
	}
	require.NoError(b, subject.Close())

	// Instantiate a cache which will restore data on start up.
	b.Run("RestoreOnConstruction", func(b *testing.B) {
		b.SetBytes(byteSize)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			subject, err := chunker.NewCachedEntriesChunker(ctx, store, capacity, chunker.NewChainChunkerFunc(chunkSize), false)
			require.NoError(b, err)
			require.NoError(b, subject.Close())
		}
	})
}
