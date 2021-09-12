package p2pserver

import (
	"context"
	"fmt"

	v0 "github.com/filecoin-project/indexer-reference-provider/api/v0"
	"github.com/filecoin-project/indexer-reference-provider/api/v0/provider/models"
	pb "github.com/filecoin-project/indexer-reference-provider/api/v0/provider/pb"
	"github.com/filecoin-project/indexer-reference-provider/core/engine"
	"github.com/gogo/protobuf/proto"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var log = logging.Logger("provider/p2pserver")

// handler handles requests for the finder resource
type handler struct {
	e *engine.Engine
}

// handlerFunc is the function signature required by handlers in this package
type handlerFunc func(context.Context, peer.ID, *pb.ProviderMessage) ([]byte, error)

func newHandler(e *engine.Engine) *handler {
	return &handler{
		e: e,
	}
}

func (h *handler) ProtocolID() protocol.ID {
	return v0.ProviderProtocolID
}

func (h *handler) HandleMessage(ctx context.Context, msgPeer peer.ID, msgbytes []byte) (proto.Message, error) {
	var req pb.ProviderMessage
	err := req.Unmarshal(msgbytes)
	if err != nil {
		return nil, err
	}

	var rspType pb.ProviderMessage_MessageType
	var handle handlerFunc
	switch req.GetType() {
	case pb.ProviderMessage_GET_LATEST:
		log.Debug("Handle new GET_LATEST message")
		handle = h.getLatest
		rspType = pb.ProviderMessage_AD_RESPONSE
	case pb.ProviderMessage_GET_AD:
		log.Debug("Handle new GET_AD message")
		handle = h.getAd
		rspType = pb.ProviderMessage_AD_RESPONSE
	default:
		msg := "ussupported message type"
		log.Errorw(msg, "type", req.GetType())
		return nil, fmt.Errorf("%s %d", msg, req.GetType())
	}

	data, err := handle(ctx, msgPeer, &req)
	if err != nil {
		rspType = pb.ProviderMessage_ERROR_RESPONSE
		data = v0.EncodeError(err)
	}

	return &pb.ProviderMessage{
		Type: rspType,
		Data: data,
	}, nil
}

func (h *handler) getLatest(ctx context.Context, p peer.ID, msg *pb.ProviderMessage) ([]byte, error) {
	// Get latests advertisement from engine.
	id, ad, err := h.e.GetLatestAdv(ctx)
	if err != nil {
		return nil, err
	}
	r := &models.AdResponse{ID: id, Ad: ad}
	return models.MarshalResp(r)
}

func (h *handler) getAd(ctx context.Context, p peer.ID, msg *pb.ProviderMessage) ([]byte, error) {
	req, err := models.UnmarshalReq(msg.GetData())
	if err != nil {
		return nil, err
	}

	// Get advertisement by ID from engine
	ad, err := h.e.GetAdv(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	r := &models.AdResponse{ID: req.ID, Ad: ad}
	return models.MarshalResp(r)
}
