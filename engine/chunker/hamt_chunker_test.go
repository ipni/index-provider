package chunker_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-test/random"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine/chunker"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestNewHamtChunker_ValidatesHamtConfig(t *testing.T) {
	t.Parallel()

	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetReadStorage(store)
	ls.SetWriteStorage(store)

	tests := []struct {
		name           string
		giveHashAlg    multicodec.Code
		giveBitWidth   int
		giveBucketSize int
		wantErr        bool
	}{
		{
			name:           "Identity accepted",
			giveHashAlg:    multicodec.Identity,
			giveBitWidth:   3,
			giveBucketSize: 1,
		},
		{
			name:           "Sha2_256 accepted",
			giveHashAlg:    multicodec.Sha2_256,
			giveBitWidth:   3,
			giveBucketSize: 1,
		},
		{
			name:           "Murmur3X64_64 accepted",
			giveHashAlg:    multicodec.Murmur3X64_64,
			giveBitWidth:   3,
			giveBucketSize: 1,
		},
		{
			name:           "zero bit-width errors",
			giveHashAlg:    multicodec.Murmur3X64_64,
			giveBucketSize: 3,
			wantErr:        true,
		},

		{
			name:         "zero bucket size errors",
			giveHashAlg:  multicodec.Murmur3X64_64,
			giveBitWidth: 3,
			wantErr:      true,
		},
		{
			name:    "zero bucket size  or bit-width errors",
			wantErr: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			subject, err := chunker.NewHamtChunker(&ls, test.giveHashAlg, test.giveBitWidth, test.giveBucketSize)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, subject)
			}
		})
	}
}

func TestHamtChunker_Chunk(t *testing.T) {
	ctx := context.TODO()
	ls := cidlink.DefaultLinkSystem()
	chunkHasExpectedMhs := func(t *testing.T, subject chunker.EntriesChunker) {
		mhs := random.Multihashes(100)
		l, err := subject.Chunk(ctx, provider.SliceMultihashIterator(mhs))
		require.NoError(t, err)

		rn, err := ls.Load(ipld.LinkContext{Ctx: ctx}, l, hamt.HashMapRootPrototype)
		require.NoError(t, err)

		root := bindnode.Unwrap(rn).(*hamt.HashMapRoot)
		require.NotNil(t, root)
		gotMhs := requireDecodeAllMultihashes(t, l, ls)
		requireChunkEntriesMatch(t, gotMhs, mhs)
	}
	t.Run("NewHamtChunker", func(t *testing.T) {
		store := &memstore.Store{}
		ls.SetReadStorage(store)
		ls.SetWriteStorage(store)
		subject, err := chunker.NewHamtChunker(&ls, multicodec.Murmur3X64_64, 3, 5)
		require.NoError(t, err)
		chunkHasExpectedMhs(t, subject)
	})
	t.Run("NewHamtChunkerFunc", func(t *testing.T) {
		store := &memstore.Store{}
		ls.SetReadStorage(store)
		ls.SetWriteStorage(store)
		subject, err := chunker.NewHamtChunkerFunc(multicodec.Murmur3X64_64, 3, 5)(&ls)
		require.NoError(t, err)
		chunkHasExpectedMhs(t, subject)
	})
}
