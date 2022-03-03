package metadata

import (
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/multiformats/go-multicodec"
)

// BitswapMetadata represents the indexing metadata that uses multicodec.TransportBitswap.
var BitswapMetadata = stiapi.Metadata{
	ProtocolID: multicodec.TransportBitswap,
}
