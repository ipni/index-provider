package adminserver

import (
	"context"
	"net"
	"net/http"

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

func New(h host.Host, e *engine.Engine, cs *supplier.CarSupplier, o ...Option) (*Server, error) {

	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", opts.listenAddr)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter().StrictSlash(true)
	server := &http.Server{
		Handler:      r,
		ReadTimeout:  opts.readTimeout,
		WriteTimeout: opts.writeTimeout,
	}
	s := &Server{server, l, h, e}

	// Set protocol handlers
	r.HandleFunc("/admin/connect", s.connectHandler).
		Methods(http.MethodPost).
		Headers("Content-Type", "application/json")

	cHandler := &carHandler{cs}
	r.HandleFunc("/admin/import/car", cHandler.handleImport).
		Methods(http.MethodPost).
		Headers("Content-Type", "application/json")

	r.HandleFunc("/admin/remove/car", cHandler.handleRemove).
		Methods(http.MethodPost).
		Headers("Content-Type", "application/json")

	r.HandleFunc("/admin/list/car", cHandler.handleList).
		Methods(http.MethodGet)

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
