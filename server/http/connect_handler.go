package adminserver

import (
	"net/http"

	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func (s *Server) connectHandler(w http.ResponseWriter, r *http.Request) {
	// Decode request
	var req ConnectReq
	if _, err := req.ReadFrom(r.Body); err != nil {
		log.Errorw("cannot unmarshal request", "err", err)
		errRes := newErrorResponse("failed to unmarshal request. %v", err)
		respond(w, http.StatusBadRequest, errRes)
		return
	}

	maddr, err := ma.NewMultiaddr(req.Maddr)
	if err != nil {
		log.Errorw("failed parsing multiaddr", "err", err)
		errRes := newErrorResponse("failed to parse multiaddr. %v", err)
		respond(w, http.StatusBadRequest, errRes)
		return
	}
	addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		log.Errorw("failed to create addrInfo from multiaddr", "err", err)
		errRes := newErrorResponse("failed to create addrInfo from multiaddr. %v", err)
		respond(w, http.StatusBadRequest, errRes)
		return
	}

	// Attempt connect.
	err = s.h.Connect(r.Context(), *addrInfo)
	if err != nil {
		log.Errorw("could not connect to server", "err", err)
		errRes := newErrorResponse("could not connect to server. %v", err)
		respond(w, http.StatusInternalServerError, errRes)
		return
	}

	// Respond success case.
	log.Info("Connected successfully to peer")
	var resp ConnectRes
	respond(w, http.StatusOK, &resp)
}
