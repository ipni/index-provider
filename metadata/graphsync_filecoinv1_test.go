package metadata_test

import (
	"testing"

	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/testutil"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/require"
)

func TestRoundTripDataTransferFilecoin(t *testing.T) {
	cids := testutil.GenerateCids(4)
	filecoinV1Datas := []*metadata.GraphsyncFilecoinV1{
		{
			PieceCID:      cids[0],
			VerifiedDeal:  false,
			FastRetrieval: false,
		},
		{
			PieceCID:      cids[1],
			VerifiedDeal:  false,
			FastRetrieval: true,
		},
		{
			PieceCID:      cids[2],
			VerifiedDeal:  true,
			FastRetrieval: true,
		},
		{
			PieceCID:      cids[3],
			VerifiedDeal:  true,
			FastRetrieval: true,
		},
	}
	for _, src := range filecoinV1Datas {
		require.Equal(t, multicodec.TransportGraphsyncFilecoinv1, src.ID())

		asBytes, err := src.MarshalBinary()
		require.NoError(t, err)

		dst := &metadata.GraphsyncFilecoinV1{}
		err = dst.UnmarshalBinary(asBytes)
		require.NoError(t, err)
		require.Equal(t, src, dst)

		//TODO: remove this once sti.metadata is removed; for now add tests to assert backwards
		//      compatibility.
		var stimd stiapi.Metadata
		err = stimd.UnmarshalBinary(asBytes)
		require.NoError(t, err)
		require.Equal(t, multicodec.TransportGraphsyncFilecoinv1, stimd.ProtocolID)
		require.Equal(t, asBytes[varint.UvarintSize(uint64(multicodec.TransportGraphsyncFilecoinv1)):], stimd.Data)
	}
}

func TestGraphsyncFilecoinV1Metadata_FromIndexerMetadataErr(t *testing.T) {
	imd := stiapi.Metadata{
		ProtocolID: multicodec.TransportBitswap,
	}
	dst := &metadata.GraphsyncFilecoinV1{}
	bytes, err := imd.MarshalBinary()
	require.NoError(t, err)
	err = dst.UnmarshalBinary(bytes)
	require.Errorf(t, err, "invalid transport ID: transport-bitswap")
}
