package adminserver

import (
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
