package metadata_test

import (
	"errors"
	"testing"

	"github.com/filecoin-project/indexer-reference-provider/metadata"
	"github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestRoundTripDataTransferFilecoin(t *testing.T) {
	cids := generateCids(4)
	filecoinV1Datas := []*metadata.FilecoinV1Data{
		{
			PieceCID:      cidlink.Link{Cid: cids[0]},
			IsFree:        false,
			FastRetrieval: false,
		},
		{
			PieceCID:      cidlink.Link{Cid: cids[1]},
			IsFree:        false,
			FastRetrieval: true,
		},
		{
			PieceCID:      cidlink.Link{Cid: cids[2]},
			IsFree:        true,
			FastRetrieval: true,
		},
		{
			PieceCID:      cidlink.Link{Cid: cids[3]},
			IsFree:        true,
			FastRetrieval: true,
		},
	}
	for _, srcFilecoinV1Data := range filecoinV1Datas {
		srcDataTransferMetadata, err := srcFilecoinV1Data.Encode(metadata.GraphSyncV1)
		require.NoError(t, err)
		require.Equal(t, srcDataTransferMetadata.ExchangeFormat(), metadata.FilecoinV1)
		require.Equal(t, srcDataTransferMetadata.TransportProtocol(), metadata.GraphSyncV1)
		indexerMetadata := srcDataTransferMetadata.ToIndexerMetadata()
		require.Equal(t, multicodec.Code(0x3F0000), indexerMetadata.ProtocolID)
		dstDataTransferMetadata, err := metadata.FromIndexerMetadata(indexerMetadata)
		require.NoError(t, err)
		require.Equal(t, srcDataTransferMetadata, dstDataTransferMetadata)
		dstFilecoinV1Data, err := metadata.DecodeFilecoinV1Data(dstDataTransferMetadata)
		require.NoError(t, err)
		require.Equal(t, srcFilecoinV1Data, dstFilecoinV1Data)
	}
}

func TestFormatDetection(t *testing.T) {
	cids := generateCids(1)
	filecoinV1Data := &metadata.FilecoinV1Data{
		PieceCID:      cidlink.Link{Cid: cids[0]},
		IsFree:        false,
		FastRetrieval: false,
	}
	dataTransferMetadata, err := filecoinV1Data.Encode(metadata.GraphSyncV1)
	require.NoError(t, err)
	indexerMetadata := dataTransferMetadata.ToIndexerMetadata()
	testCases := []struct {
		protocol    multicodec.Code
		expectedErr error
	}{
		{
			protocol:    0x13F0000,
			expectedErr: errors.New("protocol 0x13F0000 is not a data transfer protocol"),
		},
		{
			protocol:    0x310000,
			expectedErr: errors.New("protocol 0x310000 is not a data transfer protocol"),
		},
		{
			protocol:    0x3F0100,
			expectedErr: errors.New("protocol 0x3F0100 does not use the FilecoinV1 exchange format"),
		},
		{
			protocol:    0x3F0001,
			expectedErr: nil,
		},
	}
	for _, testCase := range testCases {
		indexerMetadata.ProtocolID = testCase.protocol
		var err error
		dataTransferMetadata, err = metadata.FromIndexerMetadata(indexerMetadata)
		if err == nil {
			_, err = metadata.DecodeFilecoinV1Data(dataTransferMetadata)
		}
		if testCase.expectedErr == nil {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, testCase.expectedErr.Error())
		}
	}
}

var blockGenerator = blocksutil.NewBlockGenerator()

func generateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		cids = append(cids, c)
	}
	return cids
}
