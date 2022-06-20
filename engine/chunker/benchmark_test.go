package chunker_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/index-provider/engine/chunker"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func BenchmarkRestoreCache(b *testing.B) {
	const chunkSize = 1
	const capacity = 100
	const mhCount = 500
	const byteSize = capacity * mhCount * 256 / 8 // multicodec.Sha2_256

	rng := rand.New(rand.NewSource(1413))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := dssync.MutexWrap(datastore.NewMapDatastore())

	// Populate the datastore with data.
	subject, err := chunker.NewCachedEntriesChunker(ctx, store, chunkSize, capacity, false)
	require.NoError(b, err)
	for i := 0; i < capacity; i++ {
		mhi := getRandomMhIterator(b, rng, mhCount)
		chunk, err := subject.Chunk(ctx, mhi)
		require.NoError(b, err)
		require.NotNil(b, chunk)
	}
	require.NoError(b, subject.Close())

	// Instantiate a cache which will restore data on start up.
	b.Run("RestoreOnConstruction", func(b *testing.B) {
		b.SetBytes(byteSize)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			subject, err := chunker.NewCachedEntriesChunker(ctx, store, chunkSize, capacity, false)
			require.NoError(b, err)
			require.NoError(b, subject.Close())
		}
	})
}
