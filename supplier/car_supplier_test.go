package supplier

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-car/v2"
	"github.com/ipni/go-libipni/metadata"
	mock_provider "github.com/ipni/index-provider/mock"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestPutCarReturnsExpectedIterator(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))

	tests := []struct {
		name    string
		carPath string
		opts    []car.Option
	}{
		{
			name:    "CARv1ReturnsExpectedCIDs",
			carPath: "../testdata/sample-v1.car",
			opts:    []car.Option{car.StoreIdentityCIDs(true)},
		},
		{
			name:    "CARv2ReturnsExpectedCIDs",
			carPath: "../testdata/sample-wrapped-v2.car",
			opts:    []car.Option{car.StoreIdentityCIDs(true)},
		},
		{
			name:    "CARv1ReturnsExpectedCIDsWithoutIdentityCids",
			carPath: "../testdata/sample-v1.car",
		},
		{
			name:    "CARv2ReturnsExpectedCIDsWithoutIdentityCids",
			carPath: "../testdata/sample-wrapped-v2.car",
		},
	}
	md := metadata.Default.New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := gomock.NewController(t)
			t.Cleanup(mc.Finish)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mockEng := mock_provider.NewMockInterface(mc)
			ds := datastore.NewMapDatastore()
			mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
			subject := NewCarSupplier(mockEng, ds, tt.opts...)
			t.Cleanup(func() { require.NoError(t, subject.Close()) })

			options := car.ApplyOptions(tt.opts...)

			seenMultihashes := make(map[string]bool)
			wantMultihashes := 0
			{
				f, err := os.Open(tt.carPath)
				require.NoError(t, err)
				t.Cleanup(func() { f.Close() })

				br, err := car.NewBlockReader(f)
				require.NoError(t, err)
				for {
					bl, err := br.Next()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)

					if bl.Cid().Prefix().MhType == multihash.IDENTITY && !options.StoreIdentityCIDs {
						continue
					}

					mh := bl.Cid().Hash()
					seenMultihashes[mh.HexString()] = false
					wantMultihashes++
				}
			}

			wantCid := generateCidV1(t, rng)
			mockEng.
				EXPECT().
				NotifyPut(ctx, gomock.Any(), gomock.Any(), md).
				Return(wantCid, nil)

			hash := sha256.New()
			_, err := hash.Write([]byte(tt.carPath))
			require.NoError(t, err)
			gotContextID := hash.Sum(nil)

			gotCid, err := subject.Put(ctx, gotContextID, tt.carPath, md)
			require.NoError(t, err)
			require.Equal(t, wantCid, gotCid)

			paths, err := subject.List(ctx)
			require.NoError(t, err)
			require.Len(t, paths, 1)
			require.Equal(t, filepath.Clean(tt.carPath), filepath.Clean(paths[0]))

			gotIterator, err := subject.ListMultihashes(ctx, "", gotContextID)
			require.NoError(t, err)

			gotMultihashes := 0
			for {
				gotMh, err := gotIterator.Next()
				if errors.Is(err, io.EOF) {
					break // done
				}
				require.NoError(t, err)
				seen, known := seenMultihashes[gotMh.HexString()]
				require.False(t, seen)
				require.True(t, known)
				seenMultihashes[gotMh.HexString()] = true
				gotMultihashes++
			}

			for mhStr, seen := range seenMultihashes {
				if !seen {
					t.Errorf("multihash %s was not seen", mhStr)
				}
			}
			require.Equal(t, gotMultihashes, wantMultihashes)
		})
	}
}

func TestRemovedPathIsNoLongerSupplied(t *testing.T) {
	path := "../testdata/sample-wrapped-v2.car"
	rng := rand.New(rand.NewSource(1413))

	ctx := context.Background()
	mc := gomock.NewController(t)
	t.Cleanup(mc.Finish)
	ds := datastore.NewMapDatastore()

	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	subject := NewCarSupplier(mockEng, ds)
	t.Cleanup(func() { require.NoError(t, subject.Close()) })

	md := metadata.Default.New(metadata.Bitswap{})

	wantCid := generateCidV1(t, rng)
	mockEng.
		EXPECT().
		NotifyPut(ctx, gomock.Any(), gomock.Any(), md).
		Return(wantCid, nil)

	hash := sha256.New()
	_, err := hash.Write([]byte(path))
	require.NoError(t, err)
	gotContextID := hash.Sum(nil)
	id, err := subject.Put(ctx, gotContextID, path, md)
	require.NoError(t, err)
	require.Equal(t, wantCid, id)

	paths, err := subject.List(ctx)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.Equal(t, filepath.Clean(path), filepath.Clean(paths[0]))

	wantCid = generateCidV1(t, rng)
	mockEng.
		EXPECT().
		NotifyRemove(ctx, peer.ID(""), gotContextID).
		Return(wantCid, nil)

	removedId, err := subject.Remove(ctx, gotContextID)
	require.NoError(t, err)
	require.Equal(t, wantCid, removedId)

	_, err = subject.Remove(ctx, gotContextID)
	require.EqualError(t, err, "no CID iterator found for given key")

	pathsAfterRm, err := subject.List(ctx)
	require.NoError(t, err)
	require.Len(t, pathsAfterRm, 0)
}

func generateCidV1(t *testing.T, rng *rand.Rand) cid.Cid {
	data := []byte(fmt.Sprintf("🌊d-%d", rng.Uint64()))
	mh, err := multihash.Sum(data, multihash.SHA3_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}
