package adminserver

import (
	"context"
	"net/http"

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
	var advID cid.Cid
	var err error
	ctx := context.Background()

	log.Info("Storing CAR and generating key")
	advID, err = h.cs.Put(ctx, req.Key, req.Path, req.Metadata)

	// Respond with cause of failure.
	if err != nil {
		log.Errorw("failed to put CAR", "err", err, "path", req.Path)
		errRes := newErrorResponse("failed to supply CAR. %v", err)
		respond(w, http.StatusInternalServerError, errRes)
		return
	}

	log.Infow("Stored CAR", "path", req.Path, "contextID", req.Key)

	// Respond with successful import results.
	resp := &ImportCarRes{req.Key, advID}
	respond(w, http.StatusOK, resp)
}
