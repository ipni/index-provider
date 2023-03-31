package adminserver

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/supplier"
)

type carHandler struct {
	cs *supplier.CarSupplier
}

func (h *carHandler) handleImport(w http.ResponseWriter, r *http.Request) {
	if !methodOK(w, r, http.MethodPost) {
		return
	}
	if !matchContentTypeJson(w, r) {
		return
	}
	log.Info("received import CAR request")

	// Decode request.
	var req ImportCarReq
	if _, err := req.ReadFrom(r.Body); err != nil {
		msg := fmt.Sprintf("failed to unmarshal request: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	// Supply CAR.
	var advID cid.Cid
	var err error
	ctx := context.Background()

	md := metadata.Default.New()
	if err := md.UnmarshalBinary(req.Metadata); err != nil {
		msg := fmt.Sprintf("failed to unmarshal metadata: %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	log.Info("importing CAR")
	advID, err = h.cs.Put(ctx, req.Key, req.Path, md)

	// Respond with cause of failure.
	if err != nil {
		if errors.Is(err, provider.ErrAlreadyAdvertised) {
			msg := "CAR already advertised"
			log.Infow(msg, "path", req.Path)
			http.Error(w, msg, http.StatusConflict)
			return
		}
		msg := fmt.Sprintf("failed to import CAR: %v", err)
		log.Errorw(msg, "err", err, "path", req.Path)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	log.Infow("imported CAR successfully", "path", req.Path, "contextID", req.Key)

	// Respond with successful import results.
	resp := &ImportCarRes{req.Key, advID}
	respond(w, http.StatusOK, resp)
}

func (h *carHandler) handleRemove(w http.ResponseWriter, r *http.Request) {
	if !methodOK(w, r, http.MethodPost) {
		return
	}
	if !matchContentTypeJson(w, r) {
		return
	}
	log.Info("Received remove CAR request")

	// Decode request.
	var req RemoveCarReq
	if _, err := req.ReadFrom(r.Body); err != nil {
		msg := fmt.Sprintf("failed to unmarshal request. %v", err)
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	if len(req.Key) == 0 {
		msg := "key must be specified"
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	b64Key := base64.StdEncoding.EncodeToString(req.Key)
	// Remove CAR.
	log.Infow("Removing CAR by key", "key", b64Key)
	advID, err := h.cs.Remove(context.Background(), req.Key)

	// Respond with cause of failure.
	if err != nil {
		if errors.Is(err, supplier.ErrNotFound) {
			err = fmt.Errorf("provider has no car file for key %s", b64Key)
			log.Error(err)
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		log.Errorw("Failed to remove CAR", "err", err, "key", b64Key, "advertisement", advID)
		err = fmt.Errorf("error removing car: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infow("Removed CAR successfully", "contextID", b64Key)

	// Respond with successful remove result.
	resp := &RemoveCarRes{AdvId: advID}
	respond(w, http.StatusOK, resp)
}

func (h *carHandler) handleList(w http.ResponseWriter, r *http.Request) {
	if !methodOK(w, r, http.MethodGet) {
		return
	}

	paths, err := h.cs.List(context.Background())
	if err != nil {
		err = fmt.Errorf("failed to list CARs %w", err)
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := &ListCarRes{
		Paths: paths,
	}
	respond(w, http.StatusOK, resp)
}
