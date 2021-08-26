package suppliers

import (
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestCidIteratorReadCloserReturnsCidsConsistentWithIterator(t *testing.T) {
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
			iter, err := newCarCidIterator(tt.carPath)
			require.NoError(t, err)
			subject := NewCidIteratorReadCloser(iter, func(cid cid.Cid) ([]byte, error) { return cid.Bytes(), nil })
			t.Cleanup(func() { require.NoError(t, subject.Close()) })

			control, err := newCarCidIterator(tt.carPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, control.Close()) })

			// Assert CIDs are consistent with the iterator
			for {
				wantNext, wantErr := control.Next()
				if wantErr == io.EOF {
					gotBytes := make([]byte, 1)
					_, err := subject.Read(gotBytes)
					require.Equal(t, io.EOF, err)
					break
				}
				require.NoError(t, wantErr)
				wantBytes := wantNext.Bytes()
				wantBytesLen := len(wantBytes)
				gotBytes := make([]byte, wantBytesLen)
				read, err := subject.Read(gotBytes)
				require.NoError(t, err)
				require.Equal(t, wantBytesLen, read)
				require.Equal(t, wantBytes, gotBytes)
			}
		})
	}
}
