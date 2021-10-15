package metadata

import (
	"bytes"
	"fmt"

	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-multicodec"
)

// IsDataTransferProtocol indicates whether or not a multicodec is
// in the reserved range for a data transfer protocols
// Data transfer protocols reserve the range 0x3F0000 to 0x3FFFFF
// After the 2 largest order bytes (3F)
// the first two remaining bytes indicate the exchange format and
// the next indicate the transport
// so for 0x3F6704, 67 would indicate the exchange format and 04 would be the
// transfport
func IsDataTransferProtocol(c multicodec.Code) bool {
	return c&0xFFFFFFFFFF3F0000 == 0x3F0000
}

// ErrNotDataTransfer indicates a protocol is not a data transfer protocol
type ErrNotDataTransfer struct {
	incorrectProtocol multicodec.Code
}

func (e ErrNotDataTransfer) Error() string {
	return fmt.Sprintf("protocol 0x%X is not a data transfer protocol", uint64(e.incorrectProtocol))
}

// ExchangeFormat identifies the type of vouchers taht will be exchanged
type ExchangeFormat byte

// TransportProtocol indicates the underlying protocol (for now, always graphsync)
type TransportProtocol byte

// DataTransferMetadata is simply a structured form of Metadata that deciphers
// ExchangeFormat and TransportProtocol
type DataTransferMetadata struct {
	c multicodec.Code
	// FormatData is opaque bytes that can be decoded once you know the format type
	Data []byte
}

// ExchangeFormat indicates the type of exchange format used for sending vouchers
// in this data transfer
func (dtm DataTransferMetadata) ExchangeFormat() ExchangeFormat {
	return ExchangeFormat((dtm.c & 0xFF00) >> 8)
}

// TransportProtocol indicates the type of transport protocol used for this data transfer
func (dtm DataTransferMetadata) TransportProtocol() TransportProtocol {
	return TransportProtocol(dtm.c & 0xFF)
}

const (
	// FilecoinV1 is the current filecoin retrieval protocol
	FilecoinV1 ExchangeFormat = 0x00

	// GraphSyncV1 is the graphsync libp2p protocol
	GraphSyncV1 TransportProtocol = 0x00
)

// FilecoinV1Data is the information encoded in Data for FilecoinGraphsyncV1
type FilecoinV1Data struct {
	// PieceCID identifies the piece this data can be found in
	PieceCID datamodel.Link
	// Free indicates if the retrieval is free
	IsFree bool
	// FastRetrieval indicates whether the provider claims there is an unsealed copy
	FastRetrieval bool
}

var filecoinvV1SchemaType schema.Type

func init() {
	ts := schema.TypeSystem{}
	ts.Init()
	ts.Accumulate(schema.SpawnLink("Link"))
	ts.Accumulate(schema.SpawnBool("Bool"))
	ts.Accumulate(schema.SpawnStruct("FilecoinV1Data",
		[]schema.StructField{
			schema.SpawnStructField("PieceCID", "Link", false, false),
			schema.SpawnStructField("IsFree", "Bool", false, false),
			schema.SpawnStructField("FastRetrieval", "Bool", false, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))
	filecoinvV1SchemaType = ts.TypeByName("FilecoinV1Data")
}

// DataTransferMulticodec writes a new data transfer protocol multicodec
// from the given exchange format and transport protocol
func DataTransferMulticodec(ef ExchangeFormat, tp TransportProtocol) multicodec.Code {
	return multicodec.Code(uint64(0x3F0000) | uint64(ef)<<8 | uint64(tp))
}

// ToIndexerMetadata converts DataTransferMetadata information into
// indexer Metadata format
func (dtm DataTransferMetadata) ToIndexerMetadata() stiapi.Metadata {
	return stiapi.Metadata{
		ProtocolID: dtm.c,
		Data:       dtm.Data,
	}
}

// FromIndexerMetadata converts indexer Metadata format to DataTransferMetadata
// if the multicodec falls in the DataTransferMetadata range
func FromIndexerMetadata(m stiapi.Metadata) (DataTransferMetadata, error) {
	if !IsDataTransferProtocol(m.ProtocolID) {
		return DataTransferMetadata{}, ErrNotDataTransfer{m.ProtocolID}
	}
	return DataTransferMetadata{
		c:    m.ProtocolID,
		Data: m.Data,
	}, nil
}

// Encode serialized FilecoinV1 data and then wraps it into a
// DataTransferMetadata struct
func (fd *FilecoinV1Data) Encode(transportProtocol TransportProtocol) (DataTransferMetadata, error) {
	nd := bindnode.Wrap(fd, filecoinvV1SchemaType)
	buf := new(bytes.Buffer)
	err := dagcbor.Encode(nd, buf)
	if err != nil {
		return DataTransferMetadata{}, err
	}
	return DataTransferMetadata{
		c:    DataTransferMulticodec(FilecoinV1, transportProtocol),
		Data: buf.Bytes(),
	}, nil
}

// ErrNotFilecoinV1 indicates a protocol does not use filecoin v1 exchange
type ErrNotFilecoinV1 struct {
	incorrectProtocol multicodec.Code
}

func (e ErrNotFilecoinV1) Error() string {
	return fmt.Sprintf("protocol 0x%X does not use the FilecoinV1 exchange format", uint64(e.incorrectProtocol))
}

// DecodeFilecoinV1Data decodes a new FilecoinV1Data instance from
// DataTransferMetadata if the format and protocol match
func DecodeFilecoinV1Data(dtm DataTransferMetadata) (*FilecoinV1Data, error) {
	if dtm.ExchangeFormat() != FilecoinV1 {
		return nil, ErrNotFilecoinV1{dtm.c}
	}
	r := bytes.NewBuffer(dtm.Data)
	proto := bindnode.Prototype((*FilecoinV1Data)(nil), filecoinvV1SchemaType)
	nb := proto.NewBuilder()
	err := dagcbor.Decode(nb, r)
	if err != nil {
		return nil, err
	}
	nd := nb.Build()
	return bindnode.Unwrap(nd).(*FilecoinV1Data), nil
}
