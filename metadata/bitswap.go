package metadata

import (
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
)

const (
	// BitswapMetadataV1 is the 2021-era ipfs-bitswap transport protocol
	BitswapMetadataV1 = 0x3E0000
)

// BitswapMetadata is a signalling indicator for the bitswap transport
var BitswapMetadata = stiapi.Metadata{
	ProtocolID: BitswapMetadataV1,
	Data:       []byte{},
}
