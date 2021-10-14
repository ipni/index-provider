package metadata

import (
	"bytes"
	"io"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

// DataTransferProtocolID is the protocol id that indicates an piece of content
// is served over the data transport protocol
// TBD: choose a number -- should Protocol be a multicodec? It's not really
// and encoding format -- it's a protocol -- or maybe we're ultimately just
// describing a metadata encoding format?
const DataTransferProtocolID = 0x300001

// ExchangeFormat identifies a protocol for exchanging vouchers over data transfer
type ExchangeFormat string

// TransportProtocol indicates the underlying transport data transfer will use
// Note: while currently go-data-transfer supports only graphsync, this is part
// of the metadata to future proof for multiple transport protocols
type TransportProtocol string

// DataTransferMetadata is the CBOR serialized data encoded in the
// metadata bytes when the multicodec is DataTransferProtocolID
type DataTransferMetadata struct {
	// ExchangeFormat identifies the type of vouchers taht will be exchanged
	ExchangeFormat ExchangeFormat
	// Transport indicates the underlying protocol (for now, always graphsync)
	TransportProtocol TransportProtocol
	// FormatData is opaque bytes that can be decoded once you know the format type
	FormatData []byte
}

const (
	// FilecoinV1 is the current filecoin retrieval protocol
	FilecoinV1 ExchangeFormat = "/filecoin-exchange/1.0.0"

	// GraphSync is the name of actual graphsync libp2p protocol
	GraphSync TransportProtocol = "/ipfs/graphsync/1.0.0"
)

// FilecoinV1Data is the information encoded in FormatDat for FilecoinV1
type FilecoinV1Data struct {
	// PieceCID identifies the piece this data can be found in
	PieceCID datamodel.Link
	// Free indicates if the retrieval is free
	IsFree bool
	// FastRetrieval indicates whether the provider claims there is an unsealed copy
	FastRetrieval bool
}

var dataTransferSchemaType schema.Type
var filecoinvV1SchemaType schema.Type

func init() {
	ts := schema.TypeSystem{}
	ts.Init()
	ts.Accumulate(schema.SpawnString("String"))
	ts.Accumulate(schema.SpawnBytes("Bytes"))
	ts.Accumulate(schema.SpawnLink("Link"))
	ts.Accumulate(schema.SpawnBool("Bool"))
	ts.Accumulate(schema.SpawnStruct("DataTransferMetadata",
		[]schema.StructField{
			schema.SpawnStructField("ExchangeFormat", "String", false, false),
			schema.SpawnStructField("TransportProtocol", "String", false, false),
			schema.SpawnStructField("FormatData", "Bytes", false, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))
	ts.Accumulate(schema.SpawnStruct("FilecoinV1Data",
		[]schema.StructField{
			schema.SpawnStructField("PieceCID", "Link", false, false),
			schema.SpawnStructField("IsFree", "Bool", false, false),
			schema.SpawnStructField("FastRetrieval", "Bool", false, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))
	dataTransferSchemaType = ts.TypeByName("DataTransferMetadata")
	filecoinvV1SchemaType = ts.TypeByName("FilecoinV1Data")
}

// EncodeDataTransferMetadata writes DataTransferMetadata to a byte stream
func EncodeDataTransferMetadata(dtm *DataTransferMetadata, w io.Writer) error {
	nd := bindnode.Wrap(dtm, dataTransferSchemaType)
	return dagcbor.Encode(nd, w)
}

// EncodeDataTransferMetadataToBytes writes a serialized byte array for the given
// DataTransferMetadata
func EncodeDataTransferMetadataToBytes(dtm *DataTransferMetadata) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := EncodeDataTransferMetadata(dtm, buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeDataTransferMetadata reads a new DataTransferMetadata instance from
// a byte stream
func DecodeDataTransferMetadata(r io.Reader) (*DataTransferMetadata, error) {
	proto := bindnode.Prototype((*DataTransferMetadata)(nil), dataTransferSchemaType)
	nb := proto.NewBuilder()
	err := dagcbor.Decode(nb, r)
	if err != nil {
		return nil, err
	}
	nd := nb.Build()
	return bindnode.Unwrap(nd).(*DataTransferMetadata), nil
}

// DecodeDataTransferMetadataFromBytes reads a new DataTransferMetadata instance from
// serialized byte array
func DecodeDataTransferMetadataFromBytes(serialized []byte) (*DataTransferMetadata, error) {
	r := bytes.NewBuffer(serialized)
	return DecodeDataTransferMetadata(r)
}

// EncodeFilecoinV1Data writes FilecoinV1Data to a byte stream
func EncodeFilecoinV1Data(fd *FilecoinV1Data, w io.Writer) error {
	nd := bindnode.Wrap(fd, filecoinvV1SchemaType)
	return dagcbor.Encode(nd, w)
}

// EncodeFilecoinV1DataToBytes writes a serialized byte array for the given
// FilecoinV1Data
func EncodeFilecoinV1DataToBytes(fd *FilecoinV1Data) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := EncodeFilecoinV1Data(fd, buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeFilecoinV1Data reads a new FilecoinV1Data instance from
// a byte stream
func DecodeFilecoinV1Data(r io.Reader) (*FilecoinV1Data, error) {
	proto := bindnode.Prototype((*FilecoinV1Data)(nil), filecoinvV1SchemaType)
	nb := proto.NewBuilder()
	err := dagcbor.Decode(nb, r)
	if err != nil {
		return nil, err
	}
	nd := nb.Build()
	return bindnode.Unwrap(nd).(*FilecoinV1Data), nil
}

// DecodeFilecoinV1DataFromBytes reads a new FilecoinV1Data instance from
// serialized byte array
func DecodeFilecoinV1DataFromBytes(serialized []byte) (*FilecoinV1Data, error) {
	r := bytes.NewBuffer(serialized)
	return DecodeFilecoinV1Data(r)
}
