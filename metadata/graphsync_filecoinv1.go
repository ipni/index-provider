package metadata

import (
	"bytes"
	"errors"
	"fmt"

	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

// GraphsyncFilecoinV1Metadata represents the indexing metadata for multicodec.TransportGraphsyncFilecoinv1.
type GraphsyncFilecoinV1Metadata struct {
	// PieceCID identifies the piece this data can be found in
	PieceCID cid.Cid
	// VerifiedDeal indicates if the deal is verified
	VerifiedDeal bool
	// FastRetrieval indicates whether the provider claims there is an unsealed copy
	FastRetrieval bool
}

var ParsedGraphsyncFilecoinV1MetadataMetadata = stiapi.ParsedMetadata{Protocols: []stiapi.ProtocolMetadata{&GraphsyncFilecoinV1Metadata{}}}

func init() {
	stiapi.RegisterMetadataProtocol(func() stiapi.ProtocolMetadata {
		return &GraphsyncFilecoinV1Metadata{}
	})
}

func (m *GraphsyncFilecoinV1Metadata) Protocol() multicodec.Code {
	return multicodec.TransportGraphsyncFilecoinv1
}

func (dtm *GraphsyncFilecoinV1Metadata) PayloadLength() int {
	nd := bindnode.Wrap(dtm, graphSyncfilecoinV1SchemaType)
	encLen, err := dagcbor.EncodedLength(nd)
	if err != nil {
		panic(err)
	}
	return varint.UvarintSize(uint64(encLen)) + int(encLen)
}

func (dtm *GraphsyncFilecoinV1Metadata) MarshalBinary() ([]byte, error) {
	nd := bindnode.Wrap(dtm, graphSyncfilecoinV1SchemaType)
	buf := new(bytes.Buffer)
	err := dagcbor.Encode(nd, buf)
	if err != nil {
		return stiapi.Metadata{}, err
	}

	uviBuf := varint.ToUvarint(uint64(buf.Len()))

	var b bytes.Buffer
	b.Grow(len(uviBuf) + buf.Len())
	b.Write(uviBuf)
	b.Write(buf.Bytes())

	return b.Bytes(), nil
}

func (dtm *GraphsyncFilecoinV1Metadata) UnmarshalBinary(data []byte) error {
	l, sl, err := varint.FromUvarint(data)
	if err != nil {
		return err
	}
	if int(l)+sl < len(data) {
		return errors.New("payload too short")
	}
	data = data[sl:]

	r := bytes.NewBuffer(data)
	proto := bindnode.Prototype((*GraphsyncFilecoinV1Metadata)(nil), graphSyncfilecoinV1SchemaType)
	nb := proto.NewBuilder()
	err = dagcbor.Decode(nb, r)
	if err != nil {
		return err
	}
	nd := nb.Build()
	gm := bindnode.Unwrap(nd).(*GraphsyncFilecoinV1Metadata)
	dtm.VerifiedDeal = gm.VerifiedDeal
	dtm.FastRetrieval = gm.FastRetrieval
	dtm.PieceCID = gm.PieceCID
	return nil
}

func (m GraphsyncFilecoinV1Metadata) Equal(other stiapi.ProtocolMetadata) bool {
	if other.Protocol() != m.Protocol() {
		return false
	}
	ogfm := other.(*GraphsyncFilecoinV1Metadata)
	return ogfm.PieceCID == m.PieceCID && ogfm.VerifiedDeal == m.VerifiedDeal && ogfm.FastRetrieval == m.FastRetrieval
}

var graphSyncfilecoinV1SchemaType schema.Type

func init() {
	ts := schema.TypeSystem{}
	ts.Init()
	ts.Accumulate(schema.SpawnLink("Link"))
	ts.Accumulate(schema.SpawnBool("Bool"))
	ts.Accumulate(schema.SpawnStruct("GraphsyncFilecoinV1Metadata",
		[]schema.StructField{
			schema.SpawnStructField("PieceCID", "Link", false, false),
			schema.SpawnStructField("VerifiedDeal", "Bool", false, false),
			schema.SpawnStructField("FastRetrieval", "Bool", false, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))
	graphSyncfilecoinV1SchemaType = ts.TypeByName("GraphsyncFilecoinV1Metadata")
}

// ToIndexerMetadata converts GraphsyncFilecoinV1Metadata into indexer Metadata format.
func (dtm *GraphsyncFilecoinV1Metadata) ToIndexerMetadata() (stiapi.ParsedMetadata, error) {
	return stiapi.ParsedMetadata{Protocols: []stiapi.ProtocolMetadata{dtm}}, nil
}

// FromIndexerMetadata decodes the indexer metadata format into GraphsyncFilecoinV1Metadata
// if the protocol ID of the given metadata matches multicodec.TransportGraphsyncFilecoinv1.
// Otherwise, ErrNotGraphsyncFilecoinV1 error is returned.
func (dtm *GraphsyncFilecoinV1Metadata) FromIndexerMetadata(m stiapi.ParsedMetadata) error {
	var pm stiapi.ProtocolMetadata
	var incorrectProtocol multicodec.Code
	for _, p := range m.Protocols {
		if p.Protocol() == multicodec.TransportGraphsyncFilecoinv1 {
			pm = p
			break
		}
		incorrectProtocol = p.Protocol()
	}
	if pm == nil {
		return ErrNotGraphsyncFilecoinV1{incorrectProtocol}
	}
	*dtm = *(pm.(*GraphsyncFilecoinV1Metadata))
	return nil
}

// ErrNotGraphsyncFilecoinV1 indicates a protocol does not use graphsync filecoin v1 transport.
// See: multicodec.TransportGraphsyncFilecoinv1.
type ErrNotGraphsyncFilecoinV1 struct {
	incorrectProtocol multicodec.Code
}

func (e ErrNotGraphsyncFilecoinV1) Error() string {
	return fmt.Sprintf("protocol 0x%X does not use the FilecoinV1 exchange format", uint64(e.incorrectProtocol))
}
