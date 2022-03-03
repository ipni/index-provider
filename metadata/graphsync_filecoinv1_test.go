package metadata_test

import (
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"testing"

	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestRoundTripDataTransferFilecoin(t *testing.T) {
	cids := testutil.GenerateCids(4)
	filecoinV1Datas := []*metadata.GraphsyncFilecoinV1Metadata{
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
		imd, err := src.ToIndexerMetadata()
		require.NoError(t, err)
		require.Equal(t, multicodec.TransportGraphsyncFilecoinv1, imd.ProtocolID)

		dst := &metadata.GraphsyncFilecoinV1Metadata{}
		err = dst.FromIndexerMetadata(imd)
		require.NoError(t, err)
		require.Equal(t, src, dst)
	}
}

func TestGraphsyncFilecoinV1Metadata_FromIndexerMetadataErr(t *testing.T) {
	imd := stiapi.Metadata{
		ProtocolID: multicodec.TransportBitswap,
	}
	dst := &metadata.GraphsyncFilecoinV1Metadata{}
	err := dst.FromIndexerMetadata(imd)
	require.IsType(t, metadata.ErrNotGraphsyncFilecoinV1{}, err)
	require.Equal(t, "protocol 0x900 does not use the FilecoinV1 exchange format", err.Error())
}
