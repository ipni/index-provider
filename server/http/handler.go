package adminserver

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/libp2p/go-libp2p-core/peer"
)

func (s *Server) connectHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body for connect request", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req := &ConnectReq{}
	err = json.Unmarshal(body, req)
	if err != nil {
		log.Errorw("failed unmarshalling connect request", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	addrInfo, err := peer.AddrInfoFromP2pAddr(req.Addr)
	if err != nil {
		log.Errorw("failed to create addrInfo from multiaddr", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = s.h.Connect(r.Context(), *addrInfo)
	if err != nil {
		log.Errorw("couldn't connect to server", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	log.Info("Connected successfully to peer")
}
