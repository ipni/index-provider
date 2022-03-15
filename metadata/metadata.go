package metadata

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"

	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

type ErrInvalidMetadata struct {
	Message string
}

func (e ErrInvalidMetadata) Error() string {
	return fmt.Sprintf("storetheindex: invalid metadata: %v", e.Message)
}

var (
	_ sort.Interface             = (*Metadata)(nil)
	_ encoding.BinaryMarshaler   = (*Metadata)(nil)
	_ encoding.BinaryUnmarshaler = (*Metadata)(nil)
)

type (
	// Metadata is data that provides information about how to retrieve the advertised content.
	// Note that the content may be avaiable for retrieval over multiple protocols.
	Metadata struct {
		protocols []Protocol
	}

	// Protocol represents the retrieval transport protocol of an advertisement.
	Protocol interface {
		encoding.BinaryMarshaler
		encoding.BinaryUnmarshaler
		io.ReaderFrom // Implement io.ReaderFrom so that transport can be incrementally decoded.

		// ID is the multicodec of the transport protocol represented by this Protocol.
		ID() multicodec.Code
	}
)

// New instantiates a new Metadata with the given transports.
func New(t ...Protocol) Metadata {
	metadata := Metadata{
		protocols: t,
	}
	sort.Sort(&metadata)
	return metadata
}

func (m *Metadata) Len() int {
	return len(m.protocols)
}

func (m *Metadata) Less(one, other int) bool {
	return m.protocols[one].ID() < m.protocols[other].ID()
}

func (m *Metadata) Swap(one, other int) {
	m.protocols[one], m.protocols[other] = m.protocols[other], m.protocols[one]
}

// Validate checks whether this Metadata is valid.
func (m *Metadata) Validate() error {
	if len(m.protocols) == 0 {
		return errors.New("at least one transport must be specified")
	}

	var lastID multicodec.Code
	for _, transport := range m.protocols {
		if lastID > transport.ID() {
			return errors.New("metadata transports must be sorted by ID")
		}
	}

	//TODO: Assert all IDs are known and transports are unique once we discuss what's techincally valid?
	//      For example, is it valid to have repeated graphsync metadata with different values?
	return nil
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (m *Metadata) MarshalBinary() ([]byte, error) {
	sort.Sort(m)
	var buf bytes.Buffer
	for _, meta := range m.protocols {
		binary, err := meta.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if _, err := buf.Write(binary); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (m *Metadata) UnmarshalBinary(data []byte) error {
	var read int64
	for read < int64(len(data)) {
		data = data[read:]
		v, _, err := varint.FromUvarint(data)
		if err != nil {
			return err
		}
		id := multicodec.Code(v)
		t, err := newTransport(id)
		if err != nil {
			return err
		}

		buf := bytes.NewBuffer(data)
		tLen, err := t.ReadFrom(buf)
		if err != nil {
			return err
		}
		m.protocols = append(m.protocols, t)
		read += tLen
	}
	return m.Validate()
}

// Equals checks whether this Metadata is equal with the other Metadata.
// The two are considered equal if they have the same transports with the same order.
func (m *Metadata) Equals(other *Metadata) bool {
	if len(m.protocols) != len(other.protocols) {
		return false
	}
	for i, this := range m.protocols {
		that := other.protocols[i]
		// TODO: Is it more efficient to check fields individually?
		if !reflect.DeepEqual(this, that) {
			return false
		}
	}
	return true
}

func newTransport(id multicodec.Code) (Protocol, error) {
	switch id {
	case multicodec.TransportBitswap:
		return &Bitswap{}, nil
	case multicodec.TransportGraphsyncFilecoinv1:
		return &GraphsyncFilecoinV1{}, nil
	default:
		return nil, fmt.Errorf("unknwon transport id: %s", id.String())
	}
}
