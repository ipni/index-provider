package suppliers

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/stretchr/testify/require"
)

func Test_toChan(t *testing.T) {
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
			cidIter, err := newCarCidIterator(tt.carPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cidIter.Close()) })

			// Open ReadOnly blockstore used to provide wanted case for testing
			want, err := blockstore.OpenReadOnly(tt.carPath)
			require.NoError(t, err)

			// Wait at most 3 seconds for iteration over wanted CIDs.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)

			// Fail immediately if error is encountered while iterating over CIDs.
			ctx = blockstore.WithAsyncErrorHandler(ctx, func(err error) { require.Fail(t, "expected no error", "%v", err) })
			t.Cleanup(cancel)

			// Instantiate wanted CIDs channel
			keysChan, err := want.AllKeysChan(ctx)
			require.NoError(t, err)

			gotCidChan, gotErrChan := toChan(cidIter)

			// Assert CIDs are consistent with the drained iterator
			for wantCid := range keysChan {
				gotCid, ok := <-gotCidChan
				require.True(t, ok)
				require.Equal(t, wantCid.Hash(), gotCid)

				select {
				case gotErr := <-gotErrChan:
					require.Nil(t, gotErr)
				default:
				}
			}

			// Assert channels are closed
			_, ok := <-gotCidChan
			require.False(t, ok)
			_, ok = <-gotErrChan
			require.False(t, ok)

			// Assert drain has fully drained the iterator
			gotCid, gotErr := cidIter.Next()
			require.Equal(t, io.EOF, gotErr)
			require.Equal(t, cid.Undef, gotCid)
		})
	}
}
