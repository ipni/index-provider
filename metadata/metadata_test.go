package metadata_test

import (
	"testing"

	"github.com/filecoin-project/indexer-reference-provider/metadata"
	"github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
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
		filecoinV1Bytes, err := metadata.EncodeFilecoinV1DataToBytes(srcFilecoinV1Data)
		require.NoError(t, err)
		srcDataTransferMetadata := &metadata.DataTransferMetadata{
			ExchangeFormat:    metadata.FilecoinV1,
			TransportProtocol: metadata.GraphSync,
			FormatData:        filecoinV1Bytes,
		}
		dataTransferBytes, err := metadata.EncodeDataTransferMetadataToBytes(srcDataTransferMetadata)
		require.NoError(t, err)
		dstDataTransferMetadata, err := metadata.DecodeDataTransferMetadataFromBytes(dataTransferBytes)
		require.NoError(t, err)
		require.Equal(t, srcDataTransferMetadata, dstDataTransferMetadata)
		dstFilecoinV1Data, err := metadata.DecodeFilecoinV1DataFromBytes(dstDataTransferMetadata.FormatData)
		require.NoError(t, err)
		require.Equal(t, srcFilecoinV1Data, dstFilecoinV1Data)
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
