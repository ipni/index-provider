package metadata

import (
	"bytes"
	"fmt"

	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-multicodec"
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
func (dtm *GraphsyncFilecoinV1Metadata) ToIndexerMetadata() (stiapi.Metadata, error) {
	nd := bindnode.Wrap(dtm, graphSyncfilecoinV1SchemaType)
	buf := new(bytes.Buffer)
	err := dagcbor.Encode(nd, buf)
	if err != nil {
		return stiapi.Metadata{}, err
	}
	return stiapi.Metadata{
		ProtocolID: multicodec.TransportGraphsyncFilecoinv1,
		Data:       buf.Bytes(),
	}, nil
}

// FromIndexerMetadata decodes the indexer metadata format into GraphsyncFilecoinV1Metadata
// if the protocol ID of the given metadata matches multicodec.TransportGraphsyncFilecoinv1.
// Otherwise, ErrNotGraphsyncFilecoinV1 error is returned.
func (dtm *GraphsyncFilecoinV1Metadata) FromIndexerMetadata(m stiapi.Metadata) error {
	if m.ProtocolID != multicodec.TransportGraphsyncFilecoinv1 {
		return ErrNotGraphsyncFilecoinV1{m.ProtocolID}
	}
	r := bytes.NewBuffer(m.Data)
	proto := bindnode.Prototype((*GraphsyncFilecoinV1Metadata)(nil), graphSyncfilecoinV1SchemaType)
	nb := proto.NewBuilder()
	err := dagcbor.Decode(nb, r)
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

// ErrNotGraphsyncFilecoinV1 indicates a protocol does not use graphsync filecoin v1 transport.
// See: multicodec.TransportGraphsyncFilecoinv1.
type ErrNotGraphsyncFilecoinV1 struct {
	incorrectProtocol multicodec.Code
}

func (e ErrNotGraphsyncFilecoinV1) Error() string {
	return fmt.Sprintf("protocol 0x%X does not use the FilecoinV1 exchange format", uint64(e.incorrectProtocol))
}
