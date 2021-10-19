package adminserver

import (
	"context"
	"crypto/sha256"
	"net/http"

	"github.com/filecoin-project/indexer-reference-provider/internal/cardatatransfer"
	"github.com/filecoin-project/indexer-reference-provider/internal/suppliers"
	"github.com/ipfs/go-cid"
)

type importCarHandler struct {
	cs *suppliers.CarSupplier
}

func (h *importCarHandler) handle(w http.ResponseWriter, r *http.Request) {
	log.Info("Received import CAR request")

	// Decode request.
	var req ImportCarReq
	if _, err := req.ReadFrom(r.Body); err != nil {
		log.Errorw("cannot unmarshal request", "err", err)
		errRes := newErrorResponse("failed to unmarshal request. %v", err)
		respond(w, http.StatusBadRequest, errRes)
		return
	}

	// Supply CAR.
	var contextID []byte
	var advID cid.Cid
	var err error
	ctx := context.Background()
	if req.hasId() {
		contextID = req.Key
	} else {
		contextID = sha256.New().Sum([]byte(req.Path))
	}

	metadata, err := cardatatransfer.MetadataFromContextID(contextID)
	if err != nil {
		log.Errorw("could not generate storetheindex metadata", "err", err)
		errRes := newErrorResponse("failed to generate metadata. %v", err)
		respond(w, http.StatusInternalServerError, errRes)
		return
	}

	log.Info("Storing CAR and generating key")
	advID, err = h.cs.Put(ctx, contextID, req.Path, metadata)

	// Respond with cause of failure.
	if err != nil {
		log.Errorw("failed to put CAR", "err", err, "path", req.Path)
		errRes := newErrorResponse("failed to supply CAR. %v", err)
		respond(w, http.StatusInternalServerError, errRes)
		return
	}

	log.Infow("Stored CAR", "path", req.Path, "contextID", contextID)

	// Respond with successful import results.
	resp := &ImportCarRes{contextID, advID}
	respond(w, http.StatusOK, resp)
}

func (req *ImportCarReq) hasId() bool {
	return len(req.Key) != 0
}
