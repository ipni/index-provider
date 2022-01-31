package supplier

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	mock_provider "github.com/filecoin-project/index-provider/mock"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-car/v2"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const testProtocolID = 0x300000

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
	metadata := stiapi.Metadata{
		ProtocolID: testProtocolID,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := gomock.NewController(t)
			t.Cleanup(mc.Finish)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mockEng := mock_provider.NewMockInterface(mc)
			ds := datastore.NewMapDatastore()
			mockEng.EXPECT().RegisterCallback(gomock.Any())
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
					if err == io.EOF {
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
				NotifyPut(ctx, gomock.Any(), metadata).
				Return(wantCid, nil)

			gotContextID := sha256.New().Sum([]byte(tt.carPath))
			gotCid, err := subject.Put(ctx, gotContextID, tt.carPath, metadata)
			require.NoError(t, err)
			require.Equal(t, wantCid, gotCid)

			gotIterator, err := subject.Callback(ctx, gotContextID)
			require.NoError(t, err)

			gotMultihashes := 0
			for {
				gotMh, err := gotIterator.Next()
				if err == io.EOF {
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
	mockEng.EXPECT().RegisterCallback(gomock.Any())
	subject := NewCarSupplier(mockEng, ds)
	t.Cleanup(func() { require.NoError(t, subject.Close()) })

	metadata := stiapi.Metadata{
		ProtocolID: testProtocolID,
	}

	wantCid := generateCidV1(t, rng)
	mockEng.
		EXPECT().
		NotifyPut(ctx, gomock.Any(), metadata).
		Return(wantCid, nil)

	gotContextID := sha256.New().Sum([]byte(path))
	id, err := subject.Put(ctx, gotContextID, path, metadata)
	require.NoError(t, err)
	require.Equal(t, wantCid, id)

	wantCid = generateCidV1(t, rng)
	mockEng.
		EXPECT().
		NotifyRemove(ctx, gotContextID).
		Return(wantCid, nil)

	removedId, err := subject.Remove(ctx, gotContextID)
	require.NoError(t, err)
	require.Equal(t, wantCid, removedId)

	_, err = subject.Remove(ctx, gotContextID)
	require.EqualError(t, err, "no CID iterator found for given key")
}

func generateCidV1(t *testing.T, rng *rand.Rand) cid.Cid {
	data := []byte(fmt.Sprintf("🌊d-%d", rng.Uint64()))
	mh, err := multihash.Sum(data, multihash.SHA3_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}
