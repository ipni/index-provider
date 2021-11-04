package adminserver

import (
	"context"
	"net/http"

	"github.com/filecoin-project/indexer-reference-provider/internal/suppliers"
)

type removeCarHandler struct {
	cs *suppliers.CarSupplier
}

func (h *removeCarHandler) handle(w http.ResponseWriter, r *http.Request) {
	log.Info("Received remove CAR request")

	// Decode request.
	var req RemoveCarReq
	if _, err := req.ReadFrom(r.Body); err != nil {
		log.Errorw("cannot unmarshal request", "err", err)
		errRes := newErrorResponse("failed to unmarshal request. %v", err)
		respond(w, http.StatusBadRequest, errRes)
		return
	}
	if len(req.Key) == 0 {
		errRes := newErrorResponse("key must be specified")
		respond(w, http.StatusBadRequest, errRes)
		return
	}

	// Remove CAR.
	ctx := context.Background()
	log.Info("Removing CAR by key", "key", req.Key)
	advID, err := h.cs.Remove(ctx, req.Key)

	// Respond with cause of failure.
	if err != nil {
		if err == suppliers.ErrNotFound {
			log.Errorw("CAR not found for given context ID", "contextID", req.Key)
			errRes := newErrorResponse("No CAR file found for the given Key")
			respond(w, http.StatusNotFound, errRes)
			return
		}
		log.Errorw("failed to remove CAR", "err", err, "key", req.Key)
		errRes := newErrorResponse("failed to remove CAR. %v", err)
		respond(w, http.StatusInternalServerError, errRes)
		return
	}

	log.Infow("removed CAR successfully", "contextID", req.Key)

	// Respond with successful remove result.
	resp := &RemoveCarRes{AdvId: advID}
	respond(w, http.StatusOK, resp)
}
