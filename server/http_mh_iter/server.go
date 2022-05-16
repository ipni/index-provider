package http_mh_iter

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/filecoin-project/index-provider/server/utils"

	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
)

const nextPagePath = "/next_page"
const defMulhashIterPageSize = 1000

var log = logging.Logger("http_mh_iter")

type Server struct {
	*options

	s *http.Server
	l net.Listener
	r *mux.Router

	mhs   map[string][]multihash.Multihash
	mhMux sync.Mutex
}

func NewServer(o ...Option) (*Server, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	s := &Server{mhs: make(map[string][]multihash.Multihash), options: opts}

	s.s = &http.Server{
		Handler:      s,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
	}

	if s.l, err = net.Listen("tcp", opts.ListenAddr); err != nil {
		return nil, err
	}

	s.r = mux.NewRouter().StrictSlash(true)

	s.r.HandleFunc(nextPagePath, s.handleNextPage)

	return s, nil
}

func (s *Server) handleNextPage(w http.ResponseWriter, r *http.Request) {
	var req = &nextPageRequest{}

	if _, err := req.ReadFrom(r.Body); err != nil {
		msg := fmt.Sprintf("failed to unmarshal request: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	b64CtxID := base64.StdEncoding.EncodeToString(req.ContextID)

	mhs, find := s.mhs[string(req.ContextID)]
	if !find {
		msg := fmt.Sprintf("context id : %s not found", b64CtxID)
		log.Errorf(msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	count := uint64(len(mhs))
	if req.Start >= count {
		msg := fmt.Sprintf("request context id(%s) mutihash out of range, start:%d > total:%d",
			b64CtxID, req.Start, len(mhs))
		log.Errorf(msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	var end = req.Start + req.Count
	if end > count {
		end = count
	}

	if err := utils.Respond(w, http.StatusOK, &nextPageResponse{Mhs: mhs[req.Start:end]}); err != nil {
		log.Errorw("failed to write response ", "err", err)
		return
	}

	log.Infof("multihash iter server load contextID:%s, from:%d, to:%d, success", b64CtxID, req.Start, req.Count)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.options.verifyToken != nil {
		if err := s.verifyToken(r.Header.Get("ReqID"), r.Header.Get("Authorization")); err != nil {
			msg := fmt.Sprintf("token veriry failed:%v", err)
			log.Errorf(msg)
			http.Error(w, msg, http.StatusUnauthorized)
			return
		}
	}
	s.r.ServeHTTP(w, r)
}

func (s *Server) AddMultiHashes(contextID []byte, mhs []multihash.Multihash) {
	s.mhMux.Lock()
	defer s.mhMux.Unlock()
	s.mhs[string(contextID)] = mhs
}

func (s *Server) DelMultiHashes(contextID []byte) (find bool) {
	s.mhMux.Lock()
	defer s.mhMux.Unlock()
	if _, find = s.mhs[string(contextID)]; find {
		delete(s.mhs, string(contextID))
	}
	return find
}

func (s *Server) Start() error {
	log.Infow("multihash interator http server listening", "addr", s.l.Addr())
	return s.s.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Info("multihash iterator http server shutdown")
	return s.s.Shutdown(ctx)
}
