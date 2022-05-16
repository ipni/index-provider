package http_adv

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/filecoin-project/index-provider/server/utils"

	provider "github.com/filecoin-project/index-provider"
	mhiter "github.com/filecoin-project/index-provider/server/http_mh_iter"
	"github.com/filecoin-project/index-provider/supplier"
	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

const announceAdvPath = "/advertisement"
const HttpAdvToken = "HttpAdvToken"

var log = logging.Logger("adv_http_server")

type Server struct {
	*options

	e provider.Interface

	s *http.Server
	l net.Listener
	r *mux.Router

	mhs    map[string]provider.MultihashIterator
	mhsMux sync.Mutex
}

func NewServer(e provider.Interface, o ...Option) (*Server, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	s := &Server{e: e, options: opts, mhs: make(map[string]provider.MultihashIterator)}

	s.s = &http.Server{
		Handler:      s,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
	}

	if s.l, err = net.Listen("tcp", opts.ListenAddr); err != nil {
		return nil, err
	}

	s.r = mux.NewRouter().StrictSlash(true)
	s.r.HandleFunc(announceAdvPath, s.handleAnnounceAdvertisement)

	return s, nil
}

func (s *Server) handleAnnounceAdvertisement(w http.ResponseWriter, r *http.Request) {
	var (
		req AnnounceAdvReq
		err error
	)
	if _, err = req.ReadFrom(r.Body); err != nil {
		msg := fmt.Sprintf("failed to unmarshal request: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	var advCid cid.Cid
	var b64CtxID = base64.StdEncoding.EncodeToString(req.ContextID)

	if !req.IsDel {
		s.AddMultiHashIter(req.ContextID, mhiter.NewHttpMhIterator(&mhiter.HttpMhIterOption{
			ReqID:    req.ReqID,
			Url:      req.Url,
			Token:    req.Token,
			PageSize: s.options.httpMultiHashPageSize,
		}, req.ContextID, req.Total))

		if advCid, err = s.e.NotifyPut(r.Context(), req.ContextID, req.MetaData); err != nil {
			log.Errorf("publish advertisement failed, contextID:%s, err:%v", b64CtxID, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		if advCid, err = s.e.NotifyPut(r.Context(), req.ContextID, req.MetaData); err != nil {
			log.Errorf("publish advertisement failed, contextID:%s, err:%v", b64CtxID, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err := utils.Respond(w, http.StatusOK, &AnnounceAdvRes{AdvId: advCid}); err != nil {
		log.Errorw("failed to write response ", "err", err)
		return
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.VerifyToken != nil {
		if err := s.VerifyToken(r.Header.Get("Authorization")); err != nil {
			msg := fmt.Sprintf("token veriry failed:%v", err)
			log.Errorf(msg)
			http.Error(w, msg, http.StatusUnauthorized)
		}
	}
	s.r.ServeHTTP(w, r)
}

func (s *Server) AddMultiHashIter(contextID []byte, mhi provider.MultihashIterator) {
	s.mhsMux.Lock()
	defer s.mhsMux.Unlock()
	s.mhs[string(contextID)] = mhi
}

func (s *Server) DelMultihashIter(contextID []byte) (isDel bool) {
	s.mhsMux.Lock()
	defer s.mhsMux.Unlock()
	if _, isDel = s.mhs[string(contextID)]; isDel {
		delete(s.mhs, string(contextID))
	}
	return isDel
}

func (s *Server) ListMultihashes(_ context.Context, contextID []byte) (provider.MultihashIterator, error) {
	s.mhsMux.Lock()
	mhi, find := s.mhs[string(contextID)]
	s.mhsMux.Unlock()

	if !find {
		return nil, supplier.ErrNotFound
	}
	return mhi, nil
}

func (s *Server) Start() error {
	log.Infow("http advertisement server listening", "addr", s.l.Addr())
	return s.s.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Info("http advertisement server shutdown")
	return s.s.Shutdown(ctx)
}
