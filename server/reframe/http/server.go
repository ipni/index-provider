package reframeserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/ipfs/go-datastore"
	drserver "github.com/ipfs/go-delegated-routing/server"
	logging "github.com/ipfs/go-log/v2"
	provider "github.com/ipni/index-provider"
	reframelistener "github.com/ipni/index-provider/reframe"
)

var log = logging.Logger("adminserver")

type Server struct {
	server      *http.Server
	netListener net.Listener
	rListener   *reframelistener.ReframeListener
}

func New(cidTtl time.Duration,
	chunkSize int,
	snapshotSize int,
	providerID string,
	addrs []string,
	e provider.Interface,
	ds datastore.Batching,
	o ...Option) (*Server, error) {

	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	netListener, err := net.Listen("tcp", opts.listenAddr)
	if err != nil {
		return nil, fmt.Errorf("reframe initialisation failed: %s", err)
	}

	rListener, err := reframelistener.New(context.Background(), e, cidTtl, chunkSize, snapshotSize, providerID, addrs, ds, nil)
	if err != nil {
		return nil, fmt.Errorf("reframe initialisation failed: %s", err)
	}

	handler := drserver.DelegatedRoutingAsyncHandler(rListener)

	s := &http.Server{
		Handler:      handler,
		ReadTimeout:  opts.readTimeout,
		WriteTimeout: opts.writeTimeout,
	}

	return &Server{
		server:      s,
		netListener: netListener,
		rListener:   rListener,
	}, nil
}

func (s *Server) Start() error {
	log.Infow("reframe http server listening", "addr", s.netListener.Addr())
	return s.server.Serve(s.netListener)
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Info("reframe http server shutdown")
	s.rListener.Shutdown()
	return s.server.Shutdown(ctx)
}
