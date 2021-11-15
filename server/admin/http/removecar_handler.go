package adminserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/filecoin-project/index-provider/supplier"
)

type removeCarHandler struct {
	cs *supplier.CarSupplier
}

func (h *removeCarHandler) handle(w http.ResponseWriter, r *http.Request) {
	log.Info("received remove CAR request")

	// Decode request.
	var req RemoveCarReq
	if _, err := req.ReadFrom(r.Body); err != nil {
		msg := fmt.Sprintf("failed to unmarshal request. %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	if len(req.Key) == 0 {
		msg := "key must be specified"
		log.Debug(msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	// Remove CAR.
	ctx := context.Background()
	log.Info("Removing CAR by key", "key", req.Key)
	advID, err := h.cs.Remove(ctx, req.Key)

	// Respond with cause of failure.
	if err != nil {
		if err == supplier.ErrNotFound {
			msg := "no CAR file found for the given Key"
			log.Errorw(msg, "contextID", req.Key)
			http.Error(w, msg, http.StatusNotFound)
			return
		}
		msg := fmt.Sprintf("failed to remove CAR: %v", err)
		log.Errorw(msg, "err", err, "key", req.Key)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	log.Infow("removed CAR successfully", "contextID", req.Key)

	// Respond with successful remove result.
	resp := &RemoveCarRes{AdvId: advID}
	respond(w, http.StatusOK, resp)
}
