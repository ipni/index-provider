package adminserver

import (
	"context"
	"net"
	"net/http"

	"github.com/filecoin-project/indexer-reference-provider/config"

	"github.com/filecoin-project/indexer-reference-provider/engine"
	"github.com/filecoin-project/indexer-reference-provider/internal/suppliers"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
)

var log = logging.Logger("adminserver")

type Server struct {
	server *http.Server
	l      net.Listener
	h      host.Host
	e      *engine.Engine
}

func New(cfg config.AdminServer, h host.Host, e *engine.Engine, cs *suppliers.CarSupplier) (*Server, error) {
	listen, err := cfg.ListenNetAddr()
	if err != nil {
		return nil, err
	}
	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter().StrictSlash(true)
	server := &http.Server{
		Handler:      r,
		WriteTimeout: cfg.WriteTimeout,
		ReadTimeout:  cfg.ReadTimeout,
	}
	s := &Server{server, l, h, e}

	// Set protocol handlers
	r.HandleFunc("/admin/connect", s.connectHandler).
		Methods(http.MethodPost).
		Headers("Content-Type", "application/json")

	icHandler := &importCarHandler{cs}
	r.HandleFunc("/admin/import/car", icHandler.handle).
		Methods(http.MethodPost).
		Headers("Content-Type", "application/json")

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("admin http server listening", "addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Info("admin http server shutdown")
	return s.server.Shutdown(ctx)
}
