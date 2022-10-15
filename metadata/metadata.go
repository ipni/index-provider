package metadata

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"io"
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
		mc        *metadataContext
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

// metadataContext holds context for metadata serialization and deserialization.
type metadataContext struct {
	protocols map[multicodec.Code]func() Protocol
}

var Default metadataContext

func init() {
	Default = metadataContext{
		protocols: make(map[multicodec.Code]func() Protocol),
	}
	Default.protocols[multicodec.TransportBitswap] = func() Protocol { return &Bitswap{} }
	Default.protocols[multicodec.TransportGraphsyncFilecoinv1] = func() Protocol { return &GraphsyncFilecoinV1{} }
}

// WithProtocol dervies a new MetadataContext including the additional protocol mapping.
func (mc *metadataContext) WithProtocol(id multicodec.Code, factory func() Protocol) metadataContext {
	derived := metadataContext{
		protocols: make(map[multicodec.Code]func() Protocol),
	}
	for k, v := range mc.protocols {
		derived.protocols[k] = v
	}
	derived.protocols[id] = factory
	return derived
}

// New instantiates a new Metadata with the given transports.
func (mc *metadataContext) New(t ...Protocol) Metadata {
	metadata := Metadata{
		mc:        mc,
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

// Get determines if a given protocol is included in metadata, and if so returned the parsed protocol.
func (m *Metadata) Get(protocol multicodec.Code) Protocol {
	for _, p := range m.protocols {
		if p.ID() == protocol {
			return p
		}
	}
	return nil
}

// Protocols returns the transport protocols advertised in this metadata
func (m *Metadata) Protocols() []multicodec.Code {
	var out []multicodec.Code
	for _, p := range m.protocols {
		out = append(out, p.ID())
	}
	return out
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
		t, err := m.mc.newTransport(id)
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

// Equal checks whether this Metadata is equal with the other Metadata.
// The two are considered equal if they have the same transports with the same order.
func (m Metadata) Equal(other Metadata) bool {
	if len(m.protocols) != len(other.protocols) {
		return false
	}
	for i, this := range m.protocols {
		that := other.protocols[i]
		if !protocolEqual(this, that) {
			return false
		}
	}
	return true
}

func protocolEqual(one, other Protocol) bool {
	if one.ID() != other.ID() {
		return false
	}

	oneBytes, err := one.MarshalBinary()
	if err != nil {
		return false
	}
	otherBytes, err := other.MarshalBinary()
	if err != nil {
		return false
	}
	return bytes.Equal(oneBytes, otherBytes)
}

func (mc *metadataContext) newTransport(id multicodec.Code) (Protocol, error) {
	if factory, ok := mc.protocols[id]; ok {
		return factory(), nil
	}

	return nil, fmt.Errorf("unknown transport id: %s", id.String())
}
