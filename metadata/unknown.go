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
	init := varint.ToUvarint(uint64(u.Code))
	return append(init, u.Payload...), nil
}

func (u *Unknown) UnmarshalBinary(data []byte) error {
	init := varint.ToUvarint(uint64(u.Code))
	if len(data) < len(init) {
		return fmt.Errorf("doesn't start as expected")
	}
	u.Payload = data[len(init):]
	return nil
}

func (u *Unknown) ReadFrom(r io.Reader) (n int64, err error) {
	code, err := varint.ReadUvarint(rbr{r, [1]byte{0}})
	if err != nil {
		return 0, err
	}
	if code != uint64(u.Code) {
		return int64(varint.UvarintSize(code)), fmt.Errorf("unexpected code")
	}

	// see if it starts with a reasonable looking uvarint.
	size, err := varint.ReadUvarint(rbr{r, [1]byte{0}})
	if err == io.EOF {
		return int64(varint.UvarintSize(code)), nil
	}
	if err != nil {
		return int64(varint.UvarintSize(code)), err
	}

	rl := varint.ToUvarint(size)
	preSize := int64(len(rl))
	if size > MaxMetadataSize {
		return preSize, ErrTooLong
	}
	buf := make([]byte, size+uint64(preSize))
	copy(buf, rl)
	read, err := r.Read(buf[preSize:])
	bRead := int64(read)
	if err != nil {
		return preSize + bRead, err
	}
	if size != uint64(read) {
		return preSize + bRead, fmt.Errorf("expected %d readable bytes but read %d", size, read)
	}
	u.Payload = buf

	return int64(varint.UvarintSize(code)) + preSize + bRead, nil
}

type rbr struct {
	io.Reader
	b [1]byte // avoid alloc in ReadByte
}

func (r rbr) ReadByte() (byte, error) {
	n, err := r.Read(r.b[:])
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, io.ErrNoProgress
	}
	return r.b[0], nil
}
