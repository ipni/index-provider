package adminserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/supplier"
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

	var md metadata.Metadata
	if err := md.UnmarshalBinary(req.Metadata); err != nil {
		msg := fmt.Sprintf("failed to unmarshal metadata: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	log.Info("importing CAR")
	advID, err = h.cs.Put(ctx, req.Key, req.Path, md)

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
