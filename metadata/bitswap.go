package metadata

import (
	"bytes"
	"fmt"
	"io"

	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

var (
	bitswapBytes          = varint.ToUvarint(uint64(multicodec.TransportBitswap))
	_            Protocol = (*Bitswap)(nil)
)

// Bitswap represents the indexing metadata that uses multicodec.TransportBitswap.
type Bitswap struct {
}

func (b Bitswap) ID() multicodec.Code {
	return multicodec.TransportBitswap
}

func (b Bitswap) MarshalBinary() ([]byte, error) {
	return bitswapBytes, nil
}

func (b Bitswap) UnmarshalBinary(data []byte) error {
	if !bytes.Equal(data, bitswapBytes) {
		return fmt.Errorf("transport ID does not match %s", multicodec.TransportBitswap)
	}
	return nil
}

func (b Bitswap) ReadFrom(r io.Reader) (n int64, err error) {
	wantLen := len(bitswapBytes)
	buf := make([]byte, wantLen)
	read, err := r.Read(buf)
	bRead := int64(read)
	if err != nil {
		return bRead, err
	}
	if wantLen != read {
		return bRead, fmt.Errorf("expected %d readable bytes but read %d", wantLen, read)
	}

	if !bytes.Equal(bitswapBytes, buf) {
		return bRead, fmt.Errorf("transport ID does not match %s", multicodec.TransportBitswap)
	}
	return bRead, nil
}
