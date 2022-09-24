package metadata

import (
	"errors"
	"fmt"
	"io"

	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

const MaxMetadataSize = 1024

var (
	ErrTooLong = errors.New("too long")
)

// Unknown represents an unparsed metadata payload
type Unknown struct {
	Code    multicodec.Code
	Payload []byte
}

func (u *Unknown) ID() multicodec.Code {
	return u.Code
}

func (u *Unknown) MarshalBinary() ([]byte, error) {
	return u.Payload, nil
}

func (u *Unknown) UnmarshalBinary(data []byte) error {
	u.Payload = data
	return nil
}

func (u *Unknown) ReadFrom(r io.Reader) (n int64, err error) {
	// see if it starts with a reasonable looking uvarint.
	size, err := varint.ReadUvarint(rbr{r})
	if err != nil {
		return 0, err
	}

	rl := varint.ToUvarint(size)
	if size > MaxMetadataSize {
		return int64(len(rl)), ErrTooLong
	}
	buf := make([]byte, size)
	read, err := r.Read(buf)
	bRead := int64(read)
	if err != nil {
		return int64(len(rl)) + bRead, err
	}
	if size != uint64(read) {
		return int64(len(rl)) + bRead, fmt.Errorf("expected %d readable bytes but read %d", size, read)
	}

	// put the varint back in.
	u.Payload = append(rl, buf...)

	return int64(len(rl)) + bRead, nil
}

type rbr struct{ io.Reader }

func (r rbr) ReadByte() (byte, error) {
	var buf [1]byte
	n, err := r.Read(buf[:])
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, io.ErrNoProgress
	}
	return buf[0], nil
}
