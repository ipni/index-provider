package metadata_test

import (
	"bytes"
	"testing"

	"github.com/ipni/index-provider/metadata"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/require"
)

func TestNewBitswapTransport(t *testing.T) {
	wantBytes := varint.ToUvarint(uint64(multicodec.TransportBitswap))

	var subject metadata.Bitswap
	require.Equal(t, multicodec.TransportBitswap, subject.ID())

	gotBytes, err := subject.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, gotBytes, wantBytes)

	err = subject.UnmarshalBinary(gotBytes)
	require.NoError(t, err)
	require.Equal(t, multicodec.TransportBitswap, subject.ID())

	reader := bytes.NewReader(gotBytes)
	read, err := subject.ReadFrom(reader)
	require.NoError(t, err)
	require.Equal(t, multicodec.TransportBitswap, subject.ID())
	require.Equal(t, int64(len(wantBytes)), read)
}

func TestBitswapTransport_UnmarshalBinaryIsErrorForMismatchingID(t *testing.T) {
	tests := []struct {
		name       string
		givenBytes []byte
		wantErr    string
	}{
		{
			name:       "Invalid Code",
			givenBytes: varint.ToUvarint(uint64(multicodec.TransportGraphsyncFilecoinv1)),
			wantErr:    "transport ID does not match transport-bitswap",
		},
		{
			name:       "Invalid Unknwon Code",
			givenBytes: varint.ToUvarint(uint64(789456)),
			wantErr:    "transport ID does not match transport-bitswap",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var subject metadata.Bitswap
			err := subject.UnmarshalBinary(test.givenBytes)
			if test.wantErr != "" {
				require.EqualError(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
