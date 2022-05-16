package adminserver

import (
	"fmt"
	"github.com/filecoin-project/index-provider/server/utils"
	"net/http"

	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func (s *Server) connectHandler(w http.ResponseWriter, r *http.Request) {
	// Decode request
	var req ConnectReq
	if _, err := req.ReadFrom(r.Body); err != nil {
		msg := fmt.Sprintf("failed to unmarshal request: %v", err)
		log.Errorw(msg, err, err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	maddr, err := ma.NewMultiaddr(req.Maddr)
	if err != nil {
		msg := fmt.Sprintf("failed to parse multiaddr: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		msg := fmt.Sprintf("failed to create addrInfo from multiaddr: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	// Attempt connect.
	err = s.h.Connect(r.Context(), *addrInfo)
	if err != nil {
		msg := fmt.Sprintf("failed to connect to peer: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	// Respond success case.
	log.Infow("Connected to peer successfully", "addrInfo", addrInfo)
	var resp ConnectRes
	if err := utils.Respond(w, http.StatusOK, &resp); err != nil {
		log.Errorw("failed to write response ", "err", err)
		return
	}
}
