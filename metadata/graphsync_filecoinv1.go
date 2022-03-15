package metadata

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

var (
	_ Protocol = (*GraphsyncFilecoinV1)(nil)

	graphSyncfilecoinV1SchemaType schema.Type
	graphSyncfilecoinV1Prototype  schema.TypedPrototype
)

// GraphsyncFilecoinV1 represents the indexing metadata for multicodec.TransportGraphsyncFilecoinv1.
type GraphsyncFilecoinV1 struct {
	// PieceCID identifies the piece this data can be found in
	PieceCID cid.Cid
	// VerifiedDeal indicates if the deal is verified
	VerifiedDeal bool
	// FastRetrieval indicates whether the provider claims there is an unsealed copy
	FastRetrieval bool
}

func (dtm *GraphsyncFilecoinV1) ID() multicodec.Code {
	return multicodec.TransportGraphsyncFilecoinv1
}

func init() {
	ts := schema.TypeSystem{}
	ts.Init()
	ts.Accumulate(schema.SpawnLink("Link"))
	ts.Accumulate(schema.SpawnBool("Bool"))
	ts.Accumulate(schema.SpawnStruct("GraphsyncFilecoinV1",
		[]schema.StructField{
			schema.SpawnStructField("PieceCID", "Link", false, false),
			schema.SpawnStructField("VerifiedDeal", "Bool", false, false),
			schema.SpawnStructField("FastRetrieval", "Bool", false, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))
	graphSyncfilecoinV1SchemaType = ts.TypeByName("GraphsyncFilecoinV1")
	graphSyncfilecoinV1Prototype = bindnode.Prototype((*GraphsyncFilecoinV1)(nil), graphSyncfilecoinV1SchemaType)
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (dtm *GraphsyncFilecoinV1) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(varint.ToUvarint(uint64(dtm.ID())))
	nd := bindnode.Wrap(dtm, graphSyncfilecoinV1SchemaType)
	if err := dagcbor.Encode(nd, buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (dtm *GraphsyncFilecoinV1) UnmarshalBinary(data []byte) error {
	r := bytes.NewReader(data)
	_, err := dtm.ReadFrom(r)
	return err
}

func (dtm *GraphsyncFilecoinV1) ReadFrom(r io.Reader) (n int64, err error) {
	cr := &countingReader{r: r}
	v, err := varint.ReadUvarint(cr)
	if err != nil {
		return cr.readCount, err
	}
	id := multicodec.Code(v)
	if id != multicodec.TransportGraphsyncFilecoinv1 {
		return cr.readCount, fmt.Errorf("transport id does not match %s: %s", multicodec.TransportGraphsyncFilecoinv1, id)
	}

	nb := graphSyncfilecoinV1Prototype.NewBuilder()
	err = dagcbor.Decode(nb, cr)
	if err != nil {
		return cr.readCount, err
	}
	nd := nb.Build()
	gm := bindnode.Unwrap(nd).(*GraphsyncFilecoinV1)
	dtm.VerifiedDeal = gm.VerifiedDeal
	dtm.FastRetrieval = gm.FastRetrieval
	dtm.PieceCID = gm.PieceCID
	return cr.readCount, nil
}
