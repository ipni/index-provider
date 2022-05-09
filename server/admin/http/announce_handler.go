package adminserver

import (
	"fmt"
	provider "github.com/filecoin-project/index-provider"
	"net/http"
)

func (s *Server) announceHandler(w http.ResponseWriter, r *http.Request) {
	err := s.e.PublishLatest(r.Context())
	if err != nil {
		log.Errorw("Could not republish latest advertisement", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) publishAdvHandler(w http.ResponseWriter, r *http.Request) {
	var (
		req    AnnounceAdvReq
		mhIter provider.MultihashIterator
		err    error
	)
	if _, err = req.ReadFrom(r.Body); err != nil {
		msg := fmt.Sprintf("failed to unmarshal request: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	if !req.IsDel {
		mhIter = provider.NewSliceMhIterator(req.Indices)
	}

	advCid, err := s.e.PublishAdvForIndex(r.Context(), req.ContextID, mhIter, req.ProviderID,
		req.RetrievalAddrs, req.Md, req.IsDel)
	if err != nil {
		log.Errorf("Could not republish advertisement, contextID:0x%x, err:%v", req.ContextID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond with successful import results.
	respond(w, http.StatusOK, &AnnounceAdvRes{AdvId: advCid})
}
