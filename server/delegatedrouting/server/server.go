package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/ipfs/boxo/routing/http/server"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	provider "github.com/ipni/index-provider"
	drouting "github.com/ipni/index-provider/delegatedrouting"
)

var log = logging.Logger("adminserver")

type Server struct {
	server      *http.Server
	netListener net.Listener
	rListener   *drouting.Listener
}

func New(cidTtl time.Duration,
	chunkSize int,
	snapshotSize int,
	pageSize int,
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
		return nil, fmt.Errorf("delegated routing initialisation failed: %s", err)
	}

	rListener, err := drouting.New(context.Background(),
		e,
		cidTtl,
		chunkSize,
		snapshotSize,
		providerID,
		addrs,
		ds,
		nil,
		drouting.WithPageSize(pageSize),
		drouting.WithAdFlushFrequency(opts.adFlushFrequency))
	if err != nil {
		return nil, fmt.Errorf("delegated routing initialisation failed: %s", err)
	}

	handler := server.Handler(rListener)

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
	log.Infow("Delegated Routing http server listening", "addr", s.netListener.Addr())
	return s.server.Serve(s.netListener)
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Info("Delegated Routing http server shutdown")
	s.rListener.Shutdown()
	return s.server.Shutdown(ctx)
}
