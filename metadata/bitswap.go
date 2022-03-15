package metadata

import (
	"errors"

	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

// BitswapMetadata represents the indexing metadata that uses multicodec.TransportBitswap.
type BitswapMetadata struct {
}

func init() {
	stiapi.RegisterMetadataProtocol(func() stiapi.ProtocolMetadata {
		return &BitswapMetadata{}
	})
}

func (m *BitswapMetadata) Protocol() multicodec.Code {
	return multicodec.TransportBitswap
}

func (m *BitswapMetadata) PayloadLength() int {
	// No data, so return size of Uvarint.
	return varint.UvarintSize(0)
}

func (m *BitswapMetadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, varint.UvarintSize(0))
	varint.PutUvarint(buf, 0)
	return buf, nil
}

func (m *BitswapMetadata) UnmarshalBinary(data []byte) error {
	l, _, err := varint.FromUvarint(data)
	if err != nil {
		return err
	}
	if l != 0 {
		return errors.New("bitswap data lenght not zero")
	}
	return nil
}

func (m *BitswapMetadata) Equal(other stiapi.ProtocolMetadata) bool {
	return other.Protocol() == m.Protocol()
}
