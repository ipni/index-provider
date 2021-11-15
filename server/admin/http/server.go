package adminserver

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/filecoin-project/index-provider/config"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/supplier"
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

func New(cfg config.AdminServer, h host.Host, e *engine.Engine, cs *supplier.CarSupplier) (*Server, error) {
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
		WriteTimeout: time.Duration(cfg.WriteTimeout),
		ReadTimeout:  time.Duration(cfg.ReadTimeout),
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

	rcHandler := &removeCarHandler{cs}
	r.HandleFunc("/admin/remove/car", rcHandler.handle).
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
