package adminserver

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/filecoin-project/indexer-reference-provider/core"
	"github.com/filecoin-project/indexer-reference-provider/internal/suppliers"
	"github.com/ipfs/go-cid"
)

type importCarHandler struct {
	cs *suppliers.CarSupplier
}

func (h *importCarHandler) handle(w http.ResponseWriter, r *http.Request) {
	var req ImportCarReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Errorw("cannot unmarshal request", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var key core.LookupKey
	var advId cid.Cid
	var err error
	ctx := context.Background()
	if req.hasId() {
		key = req.Key
		advId, err = h.cs.PutWithID(ctx, req.Key, req.Path, req.Metadata)
	} else {
		key, advId, err = h.cs.Put(ctx, req.Path, req.Metadata)
	}

	if err != nil {
		log.Errorw("failed to put CAR", "err", err, "path", req.Path)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp := &ImportCarRes{key, advId}
	respBytes, err := json.Marshal(resp)
	if err != nil {
		log.Errorw("failed to serialized response for imported CAR", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	if _, err = w.Write(respBytes); err != nil {
		log.Errorw("failed to write response body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (req *ImportCarReq) hasId() bool {
	return len(req.Key) != 0
}
