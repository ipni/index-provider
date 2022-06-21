package adminserver

import (
	"github.com/ipfs/go-cid"
)

type (
	// ConnectReq request to connect to a given multiaddr.
	ConnectReq struct {
		Maddr string `json:"maddr"`
	}
	// ConnectRes represents successful response to ConnectReq request.
	ConnectRes struct { // Empty placeholder used to return an empty JSON object in body.
	}
)

type (
	// ImportCarReq represents a request for importing a CAR file.
	ImportCarReq struct {
		// The path to the CAR file
		Path string `json:"path"`
		// The optional key associated to the CAR. If not provided, one will be generated.
		Key []byte `json:"key"`
		// The optional metadata.
		Metadata []byte `json:"metadata"`
	}
	// ImportCarRes represents the response to an ImportCarReq.
	ImportCarRes struct {
		// The lookup Key associated to the imported CAR.
		Key []byte `json:"key"`
		// The CID of the advertisement generated as a result of import.
		AdvId cid.Cid `json:"adv_id"`
	}
)

type (
	// RemoveCarReq represents a request for removing a CAR file.
	RemoveCarReq struct {
		// The key associated to the CAR.
		Key []byte `json:"key"`
	}
	// RemoveCarRes represents the response to a RemoveCarReq
	RemoveCarRes struct {
		// The CID of the advertisement generated as a result of removal.
		AdvId cid.Cid `json:"adv_id"`
	}
)

type (
	// ListCarRes represents the response to list cars.
	ListCarRes struct {
		// The path of CARs imported.
		Paths []string `json:"paths"`
	}
)

type (
	AnnounceRes struct {
		// The CID of the advertisement announced as latest.
		AdvId cid.Cid `json:"adv_id"`
	}
)
