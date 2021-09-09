package suppliers

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	mock_core "github.com/filecoin-project/indexer-reference-provider/core/mock"
	"github.com/golang/mock/gomock"
	"github.com/multiformats/go-multihash"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"
)

func TestPutCarReturnsExpectedCidIterator(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	tests := []struct {
		name    string
		carPath string
	}{
		{
			"CARv1ReturnsExpectedCIDs",
			"../../testdata/sample-v1.car",
		},
		{
			"CARv2ReturnsExpectedCIDs",
			"../../testdata/sample-wrapped-v2.car",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := gomock.NewController(t)
			t.Cleanup(mc.Finish)

			ctx := context.Background()
			mockEng := mock_core.NewMockInterface(mc)
			ds := datastore.NewMapDatastore()
			mockEng.EXPECT().RegisterCidCallback(gomock.Any())
			subject := NewCarSupplier(mockEng, ds)
			t.Cleanup(func() { require.NoError(t, subject.Close()) })

			wantIterator, err := newCarCidIterator(tt.carPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wantIterator.Close()) })

			wantCid := generateCidV1(t, rng)
			mockEng.
				EXPECT().
				NotifyPut(ctx, gomock.Any(), gomock.Nil()).
				Return(wantCid, nil)

			gotKey, gotCid, err := subject.Put(ctx, tt.carPath, nil)
			require.NoError(t, err)
			require.Equal(t, wantCid, gotCid)
			require.NotNil(t, gotKey)

			gotIterator, err := subject.Supply(gotKey)
			require.NoError(t, err)

			for {
				wantNext, wantErr := wantIterator.Next()
				gotNext, gotErr := gotIterator.Next()
				require.Equal(t, wantErr, gotErr)
				require.Equal(t, wantNext, gotNext)

				if wantErr == io.EOF {
					break
				}
			}
		})
	}
}

func TestRemovedPathIsNoLongerSupplied(t *testing.T) {
	path := "../../testdata/sample-wrapped-v2.car"
	rng := rand.New(rand.NewSource(1413))

	ctx := context.Background()
	mc := gomock.NewController(t)
	t.Cleanup(mc.Finish)
	ds := datastore.NewMapDatastore()

	mockEng := mock_core.NewMockInterface(mc)
	mockEng.EXPECT().RegisterCidCallback(gomock.Any())
	subject := NewCarSupplier(mockEng, ds)
	t.Cleanup(func() { require.NoError(t, subject.Close()) })

	wantCid := generateCidV1(t, rng)
	mockEng.
		EXPECT().
		NotifyPut(ctx, gomock.Any(), gomock.Nil()).
		Return(wantCid, nil)

	gotKey, id, err := subject.Put(ctx, path, nil)
	require.NoError(t, err)
	require.Equal(t, wantCid, id)
	require.NotNil(t, gotKey)

	wantCid = generateCidV1(t, rng)
	mockEng.
		EXPECT().
		NotifyRemove(ctx, gotKey, gomock.Nil()).
		Return(wantCid, nil)

	removedId, err := subject.Remove(ctx, path, nil)
	require.NoError(t, err)
	require.Equal(t, wantCid, removedId)

	_, err = subject.Remove(ctx, path, nil)
	require.EqualError(t, err, "no CID iterator found for given key")
}

func generateCidV1(t *testing.T, rng *rand.Rand) cid.Cid {
	data := []byte(fmt.Sprintf("🌊d-%d", rng.Uint64()))
	mh, err := multihash.Sum(data, multihash.SHA3_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}
