package metadata_test

import (
	"math/rand"
	"testing"

	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/require"
)

func TestMetadata(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	cids := testutil.RandomCids(t, rng, 4)
	tests := []struct {
		name            string
		givenTransports []metadata.Protocol
		wantValidateErr string
	}{
		{
			name: "Out of order transports",
			givenTransports: []metadata.Protocol{
				&metadata.GraphsyncFilecoinV1{
					PieceCID:      cids[0],
					VerifiedDeal:  false,
					FastRetrieval: false,
				},
				&metadata.Bitswap{},
			},
		},
		{
			name:            "No transports is invalid",
			wantValidateErr: "at least one transport must be specified",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subject := metadata.Default.New(test.givenTransports...)
			require.Equal(t, len(test.givenTransports), subject.Len())

			err := subject.Validate()
			if test.wantValidateErr != "" {
				require.EqualError(t, err, test.wantValidateErr)
				return
			}
			require.NoError(t, err)

			tps := test.givenTransports
			rand.Shuffle(len(tps), func(i, j int) { tps[i], tps[j] = tps[j], tps[i] })

			// Assert transports are sorted
			anotherSubject := metadata.Default.New(tps...)
			require.Equal(t, subject, anotherSubject)

			gotBytes, err := subject.MarshalBinary()
			require.NoError(t, err)

			var subjectFromBytes metadata.Metadata
			err = subjectFromBytes.UnmarshalBinary(gotBytes)
			require.NoError(t, err)
			require.Equal(t, subject, subjectFromBytes)
			require.True(t, subject.Equal(subjectFromBytes))

			err = subject.Validate()
			if test.wantValidateErr != "" {
				require.EqualError(t, err, test.wantValidateErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMetadata_UnmarshalBinary(t *testing.T) {
	tests := []struct {
		name         string
		givenBytes   []byte
		wantMetadata metadata.Metadata
		wantErr      string
	}{
		{
			name:    "Empty bytes is error",
			wantErr: "at least one transport must be specified",
		},
		{
			name:       "Unknown transport ID is error",
			givenBytes: varint.ToUvarint(uint64(multicodec.Libp2pRelayRsvp)),
			wantErr:    "unknwon transport id: libp2p-relay-rsvp",
		},
		{
			name:         "Known transport ID is not error",
			givenBytes:   varint.ToUvarint(uint64(multicodec.TransportBitswap)),
			wantMetadata: metadata.Default.New(&metadata.Bitswap{}),
		},

		{
			name:       "Known transport ID mixed with unknown ID is not error",
			givenBytes: append(varint.ToUvarint(uint64(123456)), varint.ToUvarint(uint64(multicodec.TransportBitswap))...),
			wantErr:    "unknwon transport id: Code(123456)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var subject metadata.Metadata
			err := subject.UnmarshalBinary(test.givenBytes)
			if test.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, subject, test.wantMetadata)
				require.NoError(t, subject.Validate())
			} else {
				require.EqualError(t, err, test.wantErr)
			}
		})
	}
}
