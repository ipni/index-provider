package adminserver

import "github.com/ipfs/go-cid"

// ConnectReq request
type ConnectReq struct {
	Maddr string
}

// ImportCarReq represents a request for importing a CAR file.
type ImportCarReq struct {
	// The path to the CAR file
	Path string `json:"path"`
	// The optional lookup key associated to the CAR. If not provided, one will be generated.
	Key []byte `json:"key"`
	// The optional metadata.
	Metadata []byte `json:"metadata"`
}

// ImportCarRes represents the response to an ImportCarReq.
type ImportCarRes struct {
	// The lookup Key associated to the imported CAR.
	Key []byte `json:"key"`
	// The CID of the advertisement generated as a result of import.
	AdvId cid.Cid `json:"adv_id"`
}
