package suppliers

import (
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"
)

func TestPutCarReturnsExpectedCidIterator(t *testing.T) {
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
			ds := datastore.NewMapDatastore()
			subject := NewCarSupplier(ds)
			t.Cleanup(func() { require.NoError(t, subject.Close()) })

			wantIterator, err := newCarCidIterator(tt.carPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wantIterator.Close()) })

			gotCid, err := subject.Put(tt.carPath)
			require.NoError(t, err)

			gotIterator, err := subject.Supply(gotCid)
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
	ds := datastore.NewMapDatastore()
	subject := NewCarSupplier(ds)
	t.Cleanup(func() { require.NoError(t, subject.Close()) })

	id, err := subject.Put(path)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, id)

	removedId, err := subject.Remove(path)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, id)
	require.Equal(t, id, removedId)

	_, err = subject.Remove(path)
	require.EqualError(t, err, "no CID iterator found for given key")
}

func TestCARsWithSameCidsHaveSameID(t *testing.T) {
	oneId, err := generateID("../../testdata/sample-v1.car")
	require.NoError(t, err)
	anotherId, err := generateID("../../testdata/sample-wrapped-v2.car")
	require.NoError(t, err)
	require.Equal(t, oneId, anotherId)
}
