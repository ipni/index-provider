package models

import (
	"bytes"
	"encoding/json"

	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
)

// AdRequest requests for a specific Advertisement by CID
type AdRequest struct {
	ID cid.Cid
}

// AdResponse with the advertisement for a CID.
type AdResponse struct {
	ID cid.Cid
	Ad schema.Advertisement
}

// Auxiliary struct used to encapsulate advertisement encoding.
type wrap struct {
	ID cid.Cid
	Ad []byte
}

// MarshalReq serializes the request.
func MarshalReq(r *AdRequest) ([]byte, error) {
	return json.Marshal(r)
}

// MarshalResp serializes the response.
func MarshalResp(r *AdResponse) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := dagjson.Encode(r.Ad.Representation(), buf)
	if err != nil {
		return nil, err
	}
	w := &wrap{ID: r.ID, Ad: buf.Bytes()}
	return json.Marshal(w)
}

// UnmarshalReq de-serializes the request.
// We currently JSON, we could use any other format.
func UnmarshalReq(b []byte) (*AdRequest, error) {
	r := &AdRequest{}
	err := json.Unmarshal(b, r)
	return r, err
}

// UnmarshalResp de-serializes the response.
func UnmarshalResp(b []byte) (*AdResponse, error) {
	w := &wrap{}
	err := json.Unmarshal(b, w)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(w.Ad)
	nb := schema.Type.Advertisement.NewBuilder()
	err = dagjson.Decode(nb, buf)
	if err != nil {
		return nil, err
	}
	return &AdResponse{ID: w.ID, Ad: nb.Build().(schema.Advertisement)}, err
}
