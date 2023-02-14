package adminserver

import (
	"context"
	"mime"
	"net"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/index-provider/engine"
	"github.com/ipni/index-provider/supplier"
	"github.com/libp2p/go-libp2p/core/host"
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

	mux := http.NewServeMux()
	server := &http.Server{
		Handler:      mux,
		ReadTimeout:  opts.readTimeout,
		WriteTimeout: opts.writeTimeout,
	}
	s := &Server{server, l, h, e}

	// Set protocol handlers
	mux.HandleFunc("/admin/announce", s.announceHandler)
	mux.HandleFunc("/admin/announcehttp", s.announceHttpHandler)

	mux.HandleFunc("/admin/connect", s.connectHandler)

	cHandler := &carHandler{cs}
	mux.HandleFunc("/admin/import/car", cHandler.handleImport)
	mux.HandleFunc("/admin/remove/car", cHandler.handleRemove)
	mux.HandleFunc("/admin/list/car", cHandler.handleList)

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

func methodOK(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		w.Header().Set("Allow", method)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

func matchContentTypeJson(w http.ResponseWriter, r *http.Request) bool {
	return matchContentType(w, r, "application/json")
}

func matchContentType(w http.ResponseWriter, r *http.Request, matchType string) bool {
	ctHdr := r.Header.Get("Content-Type")
	// If request does not have content type, assume it is correct.
	if ctHdr != "" {
		contentType, _, err := mime.ParseMediaType(ctHdr)
		if err != nil || contentType != matchType {
			http.Error(w, "", http.StatusUnsupportedMediaType)
			return false
		}
	}
	return true
}
