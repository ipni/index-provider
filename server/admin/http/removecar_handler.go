package adminserver

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/filecoin-project/index-provider/supplier"
)

type removeCarHandler struct {
	cs *supplier.CarSupplier
}

func (h *removeCarHandler) handle(w http.ResponseWriter, r *http.Request) {
	log.Info("Received remove CAR request")

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
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	b64Key := base64.StdEncoding.EncodeToString(req.Key)
	// Remove CAR.
	log.Infow("Removing CAR by key", "key", b64Key)
	advID, err := h.cs.Remove(context.Background(), req.Key)

	// Respond with cause of failure.
	if err != nil {
		if err == supplier.ErrNotFound {
			err = fmt.Errorf("provider has no car file for key %s", b64Key)
			log.Error(err)
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		log.Errorw("Failed to remove CAR", "err", err, "key", b64Key, "advertisement", advID)
		err = fmt.Errorf("error removing car: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infow("Removed CAR successfully", "contextID", b64Key)

	// Respond with successful remove result.
	resp := &RemoveCarRes{AdvId: advID}
	respond(w, http.StatusOK, resp)
}
