package adminserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/supplier"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/ipfs/go-cid"
)

type importCarHandler struct {
	cs *supplier.CarSupplier
}

func (h *importCarHandler) handle(w http.ResponseWriter, r *http.Request) {
	log.Info("received import CAR request")

	// Decode request.
	var req ImportCarReq
	if _, err := req.ReadFrom(r.Body); err != nil {
		msg := fmt.Sprintf("failed to unmarshal request: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	// Supply CAR.
	var advID cid.Cid
	var err error
	ctx := context.Background()

	log.Info("importing CAR")
	var parsedMetadata stiapi.ParsedMetadata
	if err = parsedMetadata.UnmarshalBinary(req.Metadata); err != nil {
		msg := fmt.Sprintf("failed to unmarshal binary: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	advID, err = h.cs.Put(ctx, req.Key, req.Path, parsedMetadata)

	// Respond with cause of failure.
	if err != nil {
		if err == provider.ErrAlreadyAdvertised {
			msg := "CAR already advertised"
			log.Infow(msg, "path", req.Path)
			http.Error(w, msg, http.StatusConflict)
			return
		}
		msg := fmt.Sprintf("failed to import CAR: %v", err)
		log.Errorw(msg, "err", err, "path", req.Path)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	log.Infow("imported CAR successfully", "path", req.Path, "contextID", req.Key)

	// Respond with successful import results.
	resp := &ImportCarRes{req.Key, advID}
	respond(w, http.StatusOK, resp)
}
