package adminserver

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
)

func (s *Server) announceHandler(w http.ResponseWriter, r *http.Request) {
	if !methodOK(w, r, http.MethodPost) {
		return
	}

	adCid, err := s.e.PublishLatest(r.Context())
	if err != nil {
		log.Errorw("Could not republish latest advertisement", "err", err)
		if adCid.Defined() {
			http.Error(w, err.Error(), http.StatusBadGateway)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Respond with successful announce result.
	resp := &AnnounceRes{adCid}
	respond(w, http.StatusOK, resp)
}

func (s *Server) announceHttpHandler(w http.ResponseWriter, r *http.Request) {
	if !methodOK(w, r, http.MethodPost) {
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading import cidlist request", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	var params map[string][]byte
	err = json.Unmarshal(body, &params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	indexer, ok := params["indexer"]
	if !ok {
		http.Error(w, "missing indexer url in request", http.StatusBadRequest)
		return
	}

	indexerURL, err := url.Parse(string(indexer))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	adCid, err := s.e.PublishLatestHTTP(r.Context(), indexerURL)
	if err != nil {
		log.Errorw("Could not publish latest advertisement via http", "err", err)
		if adCid.Defined() {
			http.Error(w, err.Error(), http.StatusBadGateway)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Respond with successful announce result.
	resp := &AnnounceRes{adCid}
	respond(w, http.StatusOK, resp)
}
